"""Water Management Watering Station - Monitor

The monitor is the bridge between the station engine and CENTRAL:
    1. Logs the station into CENTRAL through the login server (WS_LOGIN / WS_REGISTER)
    2. Waits for its engine to connect (TCP, length-prefixed frames)
    3. Announces the station on Kafka (WS_I_AM on ws-state)
    4. Forwards CENTRAL / operator commands from ws-control to the engine
    5. Forwards every engine event (answers, states, water info) to ws-state

    CENTRAL <--login socket-- MONITOR <--TCP frames--> ENGINE
    CENTRAL <-----Kafka-----> MONITOR

Frames between monitor and engine: bytes[length, instruction (length bytes)]
"""

import socket
import sys
import threading
import time
from types import SimpleNamespace

from confluent_kafka import Consumer, Producer


USAGE_MSG = """
Usage:
    WM_WS_M <monitor_port> <central_host>:<central_port> <kafka_host>:<kafka_port> <ws_id> [<name> <x> <y>]
Arguments:
    <monitor_port>  - Port in where the engine connects
    <central_host>  - CENTRAL login server host name
    <central_port>  - CENTRAL login server port
    <kafka_host>    - Kafka server host name
    <kafka_port>    - Kafka server port
    <ws_id>         - Station id (1-255)
    <name> <x> <y>  - Register (or update) the station with this data, otherwise just log in
Example:
    $ WM_WS_M 6001 localhost:5000 localhost:9092 1 "River Park" 10 20
"""


# System message coloring
OK = "\033[1;32m"
WARNING = "\033[1;33m"
ERROR = "\033[1;31m"


# Water station states
DISCONNECTED = 0
AVAILABLE = 1
WATERING = 2
LEAK = 3
OUT_OF_SERVICE = 4
WS_STATES = (DISCONNECTED, AVAILABLE, WATERING, LEAK, OUT_OF_SERVICE)
WS_STATE_NAMES = ["DISCONNECTED", "AVAILABLE", "WATERING", "LEAK", "OUT_OF_SERVICE"]


# General instructions
EXIT = 0x00
STX = 0x01
ETX = 0x02
NACK = 0x03
ACK = 0x04


# Login server instructions
WS_REGISTER = 0x05  # bytes[0x05, id, x, y, STX, name (n bytes), ETX, state]
WS_LOGIN = 0x06  # bytes[0x06, id]
SERVER_ACK_WS_LOGIN = 0x07  # bytes[0x07, id, x, y, STX, name (n bytes), ETX, state]
SERVER_ACK_WS_REGISTER = 0x08  # bytes[0x08, id]


# Kafka instructions used by the watering station
OP_REQ_WATERING = 0x09  # bytes[0x09, op_id, ws_id]
WS_ANSWER_WATERING = 0x0A  # bytes[0x0A, ACK/NACK, op_id, ws_id]
C_REQ_WATERING = 0x0B  # bytes[0x0B, ws_id]
WS_C_ANSWER_WATERING = 0x0C  # bytes[0x0C, ACK/NACK]
C_WS_SHUTDOWN = 0x0D  # bytes[0x0D, ws_id]
WS_GOODBYE = 0x0F  # bytes[0x0F, ws_id]
C_PING_WS = 0x11  # bytes[0x11, ws_id]
WS_PUBLISH_STATE = 0x12  # bytes[0x12, ws_id, state]
C_WATERING = 0x14  # bytes[0x14, ws_id]
WS_I_AM = 0x19  # bytes[0x19, ws_id, x, y, STX, name (n bytes), ETX, state]
WS_WATERINFO = 0x1B  # bytes[0x1B, ws_id, liters_per_minute, accum]


# Kafka topics
T_WS_CONTROL = "ws-control"  # C -> WS
T_WS_STATE = "ws-state"  # WS -> C,OP


def stamp() -> str:
    return time.strftime("%H:%M:%S")


def tell(mtype: str, msg: str):
    print(
        f"[{stamp()}] WS_M: {mtype}{msg}",
        end="\033[1;0m\n",
        file=sys.stdout if mtype != ERROR else sys.stderr,
    )


def is_hostport(text: str) -> bool:
    l = text.find(":")
    return l not in (-1, 0, len(text) - 1) and text[l + 1 :].isnumeric()


def get_params(args: tuple[str, ...]) -> SimpleNamespace:
    if (
        len(args) not in (4, 7)
        or not args[0].isnumeric()
        or not is_hostport(args[1])
        or not is_hostport(args[2])
        or not args[3].isnumeric()
        or not 0 < int(args[3]) < 256
    ):
        print(USAGE_MSG)
        sys.exit(1)
    params = SimpleNamespace(
        port=int(args[0]),
        central=(args[1].split(":")[0], int(args[1].split(":")[1])),
        kafka=args[2],
        ws_id=int(args[3]),
        register=None,
    )
    if len(args) == 7:
        if not (args[5].isnumeric() and args[6].isnumeric()) or max(
            int(args[5]), int(args[6])
        ) > 255:
            print(USAGE_MSG)
            sys.exit(1)
        params.register = SimpleNamespace(name=args[4], x=int(args[5]), y=int(args[6]))
    return params


def send_frame(conn: socket.socket, ins: bytes) -> None:
    conn.sendall(bytes([len(ins)]) + ins)


def recv_exactly(conn: socket.socket, n: int) -> bytes | None:
    data = b""
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def recv_frame(conn: socket.socket) -> bytes | None:
    """Returns None when the other side closes the connection"""
    header = recv_exactly(conn, 1)
    if header is None:
        return None
    return recv_exactly(conn, header[0])


def login(params: SimpleNamespace) -> SimpleNamespace | None:
    """Register or log the station into CENTRAL, returns the station data"""
    ws_id = params.ws_id
    if params.register is not None:
        reg = params.register
        ins = (
            bytes([WS_REGISTER, ws_id, reg.x, reg.y, STX])
            + reg.name.encode()
            + bytes([ETX, AVAILABLE])
        )
    else:
        ins = bytes([WS_LOGIN, ws_id])

    with socket.create_connection(params.central, timeout=5) as conn:
        conn.sendall(ins)
        answer = conn.recv(512)

    if answer == bytes([SERVER_ACK_WS_REGISTER, ws_id]):
        reg = params.register
        return SimpleNamespace(id=ws_id, x=reg.x, y=reg.y, name=reg.name, state=AVAILABLE)

    if answer and answer[0] == SERVER_ACK_WS_LOGIN and len(answer) >= 7:
        # [SERVER_ACK_WS_LOGIN, id, x, y, STX, name..., ETX, state]
        name_end = answer.find(ETX, 5)
        if answer[4] == STX and name_end != -1:
            return SimpleNamespace(
                id=answer[1],
                x=answer[2],
                y=answer[3],
                name=answer[5:name_end].decode(errors="replace"),
                state=AVAILABLE,
            )

    return None


def main(*args):
    params = get_params(args)
    ws_id = params.ws_id

    # 1. Log into CENTRAL
    try:
        station = login(params)
    except OSError as err:
        tell(ERROR, f"no se puede conectar con CENTRAL {params.central}: {err}")
        sys.exit(1)
    if station is None:
        tell(
            ERROR,
            f"CENTRAL ha rechazado la estación {ws_id}"
            + ("" if params.register else ", prueba a registrarla con <name> <x> <y>"),
        )
        sys.exit(1)
    tell(OK, f"estación {ws_id} ({station.name}) en ({station.x},{station.y}) autenticada")

    # 2. Wait for the engine
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", params.port))
    srv.listen(1)
    tell(OK, f"esperando al engine en el puerto {params.port}...")
    engine, addr = srv.accept()
    srv.close()
    tell(OK, f"engine conectado desde {addr}")

    i_am = (
        bytes([WS_I_AM, ws_id, station.x, station.y, STX])
        + station.name.encode()
        + bytes([ETX, station.state])
    )
    engine_lock = threading.Lock()  # one writer at a time on the engine socket
    send_frame(engine, i_am)

    # 3. Announce the station on Kafka
    producer = Producer({"bootstrap.servers": params.kafka, "client.id": f"wm-ws-{ws_id}"})
    consumer = Consumer(
        {
            "bootstrap.servers": params.kafka,
            "group.id": f"wm-ws-{ws_id}",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([T_WS_CONTROL])
    producer.produce(T_WS_STATE, i_am, key=bytes([ws_id]))
    producer.poll(0)

    shutdown_event = threading.Event()

    # 4. Kafka -> engine
    def kafka_to_engine() -> None:
        while not shutdown_event.is_set():
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                tell(ERROR, f"(kafka) {msg.error()}")
                continue
            raw = msg.value()
            if not raw:
                continue
            opcode = raw[0]

            if opcode == OP_REQ_WATERING and len(raw) == 3 and raw[2] == ws_id:
                # [OP_REQ_WATERING, op_id, ws_id]
                tell(OK, f"el operador {raw[1]} pide regar")
            elif (
                opcode in (C_REQ_WATERING, C_WS_SHUTDOWN, C_PING_WS, C_WATERING)
                and len(raw) == 2
                and raw[1] == ws_id
            ):
                # [C_*, ws_id]
                tell(OK, f"comando de CENTRAL: {raw!r}")
            else:
                continue  # not for this station

            try:
                with engine_lock:
                    send_frame(engine, raw)
            except OSError:
                shutdown_event.set()

    threading.Thread(target=kafka_to_engine, name="ws-kafka", daemon=True).start()

    # 5. Engine -> Kafka
    try:
        while not shutdown_event.is_set():
            ins = recv_frame(engine)
            if ins is None:
                tell(WARNING, "el engine se ha desconectado")
                break
            if not ins:
                continue
            opcode = ins[0]

            if opcode == WS_PUBLISH_STATE and len(ins) == 3 and ins[2] in WS_STATES:
                # [WS_PUBLISH_STATE, ws_id, state]
                station.state = ins[2]
                tell(OK, f"nuevo estado: {WS_STATE_NAMES[ins[2]]}")
            elif opcode == WS_ANSWER_WATERING and len(ins) == 4:
                # [WS_ANSWER_WATERING, ACK/NACK, op_id, ws_id]
                tell(OK, f"respuesta al operador {ins[2]}: {'ACK' if ins[1] == ACK else 'NACK'}")
            elif opcode == WS_C_ANSWER_WATERING and len(ins) == 2:
                # [WS_C_ANSWER_WATERING, ACK/NACK]
                pass
            elif opcode == WS_WATERINFO and len(ins) == 4:
                # [WS_WATERINFO, ws_id, liters_per_minute, accum]
                pass
            elif opcode == WS_GOODBYE and len(ins) == 2:
                # [WS_GOODBYE, ws_id]: the engine is shutting down
                tell(WARNING, "el engine se despide")
                break
            else:
                tell(ERROR, f"instrucción del engine inválida: {ins!r}")
                continue

            producer.produce(T_WS_STATE, ins, key=bytes([ws_id]))
            producer.poll(0)
    except (KeyboardInterrupt, OSError):
        pass
    finally:
        tell(OK, "Saliendo...")
        shutdown_event.set()
        producer.produce(T_WS_STATE, bytes([WS_GOODBYE, ws_id]), key=bytes([ws_id]))
        producer.flush(3)
        consumer.close()
        engine.close()


if __name__ == "__main__":
    main(*sys.argv[1:])

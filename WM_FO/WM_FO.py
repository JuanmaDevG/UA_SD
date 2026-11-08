"""Water Management Field Operator

Talks only through Kafka (never through the CENTRAL login server):
    - Announces itself to CENTRAL (OP_I_AM) on op-state
    - Requests watering a station (OP_REQ_WATERING) on watering-requests
    - Receives CENTRAL / station answers on op-control
    - Learns where the stations are by reading ws-state
When a watering request is accepted, the operator teleports to the station location
and announces its new position.
"""

from random import randint
import sys
import threading
import time
from types import SimpleNamespace

from confluent_kafka import Consumer, Producer


USAGE_MSG = """
Usage:
    WM_FO <kafka_host>:<kafka_port> <operator_id> [requests_file]
    WM_FO here <operator_id> [requests_file]
Arguments:
    <kafka_host>    - Kafka server host name
    <kafka_port>    - Kafka server port
    <operator_id>   - Operator id (1-255)
    [requests_file] - File with lines "ws_id[, seconds]" to request automatically
    here            - $ WM_FO localhost:8000 <operator_id>
"""

FIELD_OPERATORS = ["Jeffrey", "Donald", "Charlie", "Kanye"]


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


# Kafka instructions used by the field operator
OP_REQ_WATERING = 0x09  # bytes[0x09, op_id, ws_id]
WS_ANSWER_WATERING = 0x0A  # bytes[0x0A, ACK/NACK, op_id, ws_id]
C_OP_SHUTDOWN = 0x0E  # bytes[0x0E, op_id]
OP_GOODBYE = 0x10  # bytes[0x10, op_id]
WS_PUBLISH_STATE = 0x12  # bytes[0x12, ws_id, state]
OP_WATERING = 0x13  # bytes[0x13, op_id, ws_id]
WS_I_AM = 0x19  # bytes[0x19, ws_id, x, y, STX, name (n bytes), ETX, state]
OP_I_AM = 0x1A  # bytes[0x1A, op_id, x, y, STX, name (n bytes), ETX, cur_station]


# Kafka topics
T_WATERING_REQUESTS = "watering-requests"  # OP -> C
T_OP_CONTROL = "op-control"  # C -> OP
T_WS_STATE = "ws-state"  # WS -> C,OP
T_OP_STATE = "op-state"  # OP -> C

NO_STATION = 0  # cur_station value when the operator is not at any station
ANSWER_TIMEOUT = 10  # seconds waiting for a watering answer
DEFAULT_WATERING_TIME = 10  # seconds, when the requests file does not say it


def stamp() -> str:
    return time.strftime("%H:%M:%S")


def tell(mtype: str, msg: str):
    print(
        f"[{stamp()}] FO: {mtype}{msg}",
        end="\033[1;0m\n",
        file=sys.stdout if mtype != ERROR else sys.stderr,
    )


def get_params(args: tuple[str, ...]) -> tuple[str, int, str | None]:
    if len(args) not in (2, 3) or not args[1].isnumeric() or not 0 < int(args[1]) < 256:
        print(USAGE_MSG)
        sys.exit(1)

    if args[0] == "here":
        kafka = "localhost:8000"
    elif (l := args[0].find(":")) not in (-1, 0, len(args[0]) - 1) and args[0][
        l + 1 :
    ].isnumeric():
        kafka = args[0]
    else:
        print(USAGE_MSG)
        sys.exit(1)

    return kafka, int(args[1]), args[2] if len(args) == 3 else None


def read_requests(path: str) -> list[tuple[int, int]]:
    """Lines "ws_id[, seconds]", '#' starts a comment"""
    requests = []
    with open(path) as f:
        for line in f:
            line = line.split("#")[0].strip()
            if not line:
                continue
            fields = [v.strip() for v in line.split(",")]
            ws_id = int(fields[0])
            seconds = int(fields[1]) if len(fields) > 1 else DEFAULT_WATERING_TIME
            requests.append((ws_id, seconds))
    return requests


def main(*args):
    kafka, op_id, requests_file = get_params(args)
    me = SimpleNamespace(
        id=op_id,
        x=randint(0, 255),
        y=randint(0, 255),
        name=FIELD_OPERATORS[op_id - 1]
        if op_id <= len(FIELD_OPERATORS)
        else f"FO-{op_id}",
        cur_station=NO_STATION,
    )
    stations: dict[int, SimpleNamespace] = {}  # ws_id -> SimpleNamespace(x, y, name, state)
    lock = threading.Lock()  # protects 'me' and 'stations'
    answer_received = threading.Event()
    answer = SimpleNamespace(ws_id=None, ack=None)
    shutdown_event = threading.Event()

    producer = Producer({"bootstrap.servers": kafka, "client.id": f"wm-fo-{op_id}"})
    consumer = Consumer(
        {
            "bootstrap.servers": kafka,
            # Own group: every operator must read every message
            "group.id": f"wm-fo-{op_id}",
            # Read ws-state history to learn every station location
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([T_OP_CONTROL, T_WS_STATE])

    def announce() -> None:
        with lock:
            ins = (
                bytes([OP_I_AM, me.id, me.x, me.y, STX])
                + me.name.encode()
                + bytes([ETX, me.cur_station])
            )
        producer.produce(T_OP_STATE, ins, key=bytes([op_id]))
        producer.poll(0)

    def listen() -> None:
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
            topic = msg.topic()
            opcode = raw[0]

            if topic == T_WS_STATE:
                if opcode == WS_I_AM and len(raw) >= 7 and raw[4] == STX:
                    # [WS_I_AM, ws_id, x, y, STX, name..., ETX, state]
                    name_end = raw.find(ETX, 5)
                    if name_end == -1 or raw[-1] not in WS_STATES:
                        continue
                    with lock:
                        stations[raw[1]] = SimpleNamespace(
                            x=raw[2],
                            y=raw[3],
                            name=raw[5:name_end].decode(errors="replace"),
                            state=raw[-1],
                        )
                elif opcode == WS_PUBLISH_STATE and len(raw) == 3 and raw[2] in WS_STATES:
                    # [WS_PUBLISH_STATE, ws_id, state]
                    with lock:
                        if raw[1] in stations:
                            stations[raw[1]].state = raw[2]

            elif topic == T_OP_CONTROL:
                if opcode == WS_ANSWER_WATERING and len(raw) == 4 and raw[2] == op_id:
                    # [WS_ANSWER_WATERING, ACK/NACK, op_id, ws_id]
                    if raw[3] != answer.ws_id or answer_received.is_set():
                        continue  # old answer, not the one being waited for
                    answer.ack = raw[1]
                    answer_received.set()
                elif opcode == C_OP_SHUTDOWN and len(raw) == 2 and raw[1] == op_id:
                    # [C_OP_SHUTDOWN, op_id]
                    tell(WARNING, "CENTRAL ha ordenado apagar el operador")
                    shutdown_event.set()

    def request_watering(ws_id: int) -> bool:
        answer.ws_id = ws_id
        answer.ack = None
        answer_received.clear()
        producer.produce(
            T_WATERING_REQUESTS, bytes([OP_REQ_WATERING, op_id, ws_id]), key=bytes([op_id])
        )
        producer.poll(0)
        tell(OK, f"pidiendo riego en la estación {ws_id}...")

        if not answer_received.wait(ANSWER_TIMEOUT):
            tell(ERROR, f"la estación {ws_id} no ha contestado a tiempo")
            return False
        if answer.ack != ACK:
            tell(WARNING, f"riego en la estación {ws_id} denegado")
            return False

        # Accepted: teleport to the station
        with lock:
            station = stations.get(ws_id)
            if station is not None:
                me.x, me.y = station.x, station.y
            else:
                tell(WARNING, f"ubicación de la estación {ws_id} desconocida")
            me.cur_station = ws_id
            tell(OK, f"riego aceptado, teletransportado a ({me.x},{me.y})")
        announce()
        producer.produce(
            T_WATERING_REQUESTS, bytes([OP_WATERING, op_id, ws_id]), key=bytes([op_id])
        )
        producer.poll(0)
        return True

    listener = threading.Thread(target=listen, name="fo-listener", daemon=True)
    listener.start()
    announce()
    tell(OK, f"operador {me.id} ({me.name}) en ({me.x},{me.y}), kafka={kafka}")

    try:
        if requests_file is not None:
            for ws_id, seconds in read_requests(requests_file):
                if shutdown_event.is_set():
                    break
                if request_watering(ws_id):
                    shutdown_event.wait(seconds)
        else:
            print("Comandos: r <ws_id> (pedir riego) | l (listar estaciones) | q (salir)")
            while not shutdown_event.is_set():
                cmd = input("> ").split()
                if not cmd:
                    continue
                if cmd[0] == "q":
                    break
                elif cmd[0] == "l":
                    with lock:
                        for ws_id, s in sorted(stations.items()):
                            print(
                                f"  {ws_id}: {s.name} ({s.x},{s.y}) {WS_STATE_NAMES[s.state]}"
                            )
                        print(f"  yo: ({me.x},{me.y}) estación={me.cur_station}")
                elif cmd[0] == "r" and len(cmd) == 2 and cmd[1].isnumeric():
                    request_watering(int(cmd[1]))
                else:
                    print("Comandos: r <ws_id> | l | q")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        tell(OK, "Saliendo...")
        shutdown_event.set()
        producer.produce(T_OP_STATE, bytes([OP_GOODBYE, op_id]), key=bytes([op_id]))
        producer.flush(3)
        listener.join(timeout=2)
        consumer.close()


if __name__ == "__main__":
    main(*sys.argv[1:])

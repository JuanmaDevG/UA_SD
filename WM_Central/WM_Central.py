"""Water Management Central

(Later a proper description)
"""

import os
from pathlib import Path
from queue import Queue
from random import randint
import socket
import sqlite3
import sys
import threading
import time
from types import SimpleNamespace

from confluent_kafka import Consumer, Producer


USAGE_MSG = """
Usage:
    WM_Central <login_port> <kafka_host>:<kafka_port>
    WM_central here
Arguments:
    <login_port>    - Port in where to put login
    <kafka_host>    - Kafka server host name
    <kafka_port>    - Kafka server port
    here            - $ WM_Central 5000 localhost:8000
"""

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS stations (
    id INTEGER PRIMARY KEY,
    x INTEGER, y INTEGER,
    name TEXT UNIQUE,
    state INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS operators (
    id INTEGER PRIMARY KEY,
    x INTEGER, y INTEGER,
    name TEXT UNIQUE,
    cur_station INTEGER DEFAULT NULL
);
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

# WS state metadata
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


# Kafka instructions
OP_REQ_WATERING = 0x09  # bytes[0x09, op_id, ws_id]
WS_ANSWER_WATERING = 0x0A  # bytes[0x0A, ACK/NACK, op_id, ws_id]
C_REQ_WATERING = 0x0B  # bytes[0x0B, ws_id]
WS_C_ANSWER_WATERING = 0x0C  # bytes[0x0C, ACK/NACK]
C_WS_SHUTDOWN = 0x0D  # bytes[0x0D, ws_id]
C_OP_SHUTDOWN = 0x0E  # bytes[0x0D, ws_id]
WS_GOODBYE = 0x0F  # bytes[0x0F, ws_id]
OP_GOODBYE = 0x10  # bytes[0x10, op_id]
C_PING_WS = 0x11  # bytes[0x11, ws_id]
WS_PUBLISH_STATE = 0x12  # bytes[0x12, ws_id, state]
OP_WATERING = 0x13  # bytes[0x13, op_id, ws_id]
C_WATERING = 0x14  # bytes[0x14, ws_id]
LS_STATIONS = 0x15  # bytes[0x15]
LS_OPERATORS = 0x16  # bytes[0x16]
C_GET_STATION = 0x17  # bytes[0x18, ws_id]
C_GET_OPERATOR = 0x18  # bytes[0x18, op_id]
WS_I_AM = 0x19  # bytes[0x19, op_id, x, y, STX, name (n bytes), ETX, state]
OP_I_AM = 0x1A  # bytes[0x1A, op_id, x, y, STX, name (n bytes), ETX, cur_station]
WS_WATERINFO = 0x1B  # bytes[0x1B, ws_id, liters_per_minute, accum]
FLUSH_DB = 0x1C  # bytes[0x1C]


# Kafka topics
T_WATERING_REQUESTS = "watering-requests"  # OP -> C
T_OP_CONTROL = "op-control"  # C -> OP
T_WS_CONTROL = "ws-control"  # C -> WS
T_WS_STATE = "ws-state"  # WS -> C
T_OP_STATE = "op-state"  # OP -> C


# TODO: make it print instruction formats automatically
class InsFmtException(Exception):
    def __init__(self, i: bytes, msg: str | Exception = "") -> None:
        self.instruction = i
        self.msg = str(msg)
        super().__init__(f"(opcode: {i[0]}) {msg} (tiene {len(i)} bytes)")


def decompress_instruction(i: bytes) -> SimpleNamespace:
    if not len(i):
        raise InsFmtException(i, "instrucción vacía")

    def entity_fmt(i: bytes) -> SimpleNamespace:
        name_end: int
        state: int
        if len(i) < 7:
            raise InsFmtException(
                i, "debe tener 7+ bytes (id, x, y, STX, name (n bytes), ETX, infobyte)"
            )
        if i[4] != STX:
            raise InsFmtException(i, "falta STX en el quinto byte")
        if (name_end := i.find(ETX, 5)) == -1:
            raise InsFmtException(i, "falta ETX como delimitador del campo 'name'")
        try:
            res = SimpleNamespace(id=i[1], x=i[2], y=i[3], name=i[5:name_end].decode())
        except UnicodeError as err:
            raise InsFmtException(i, err)

        if i[0] == OP_I_AM:
            res.cur_station = i[len(i) - 1]
        elif (state := i[len(i) - 1]) not in WS_STATES:
            raise InsFmtException(
                i, f"el estado de la estación con id={i[1]} no existe"
            )
        else:
            res.state = state

        return res

    def id_fmt(i: bytes) -> SimpleNamespace:
        if len(i) != 2:
            raise InsFmtException(i, "Debe tener 2 bytes (opcode, id)")
        return SimpleNamespace(id=i[1])

    def op_ws_fmt(i: bytes) -> SimpleNamespace:
        if len(i) != 3:
            raise InsFmtException(i, "debe tener 3 bytes (opcode, op_id, ws_id)")
        return SimpleNamespace(op_id=i[1], ws_id=i[2])

    def ack_nack_fmt(i: bytes) -> SimpleNamespace:
        res: SimpleNamespace
        if i[0] == WS_C_ANSWER_WATERING:
            if len(i) != 2:
                raise InsFmtException(i, "debe tener 2 bytes (opcode, ACK/NACK)")
            res = SimpleNamespace(ack=i[2])
        elif len(i) != 4:
            raise InsFmtException(
                i, "debe tener 4 bytes (opcode, ACK/NACK, op_id, station_id)"
            )
        res = SimpleNamespace(ack=i[1], op_id=i[2], ws_id=i[3])
        return res

    def state_publish_fmt(i: bytes) -> SimpleNamespace:
        if len(i) != 3:
            raise InsFmtException(i, "debe tener 3 bytes (opcode, id, estado)")
        if i[2] >= len(WS_STATE_NAMES):
            raise InsFmtException(
                i, f"id={i[1]} tiene un estado inexistente con código {i[2]}"
            )
        return SimpleNamespace(id=i[1], state=i[2])

    inst_v = {
        WS_REGISTER: entity_fmt,
        SERVER_ACK_WS_LOGIN: entity_fmt,
        WS_LOGIN: id_fmt,
        SERVER_ACK_WS_REGISTER: id_fmt,
        OP_REQ_WATERING: op_ws_fmt,
        WS_ANSWER_WATERING: ack_nack_fmt,
        C_REQ_WATERING: id_fmt,
        WS_C_ANSWER_WATERING: ack_nack_fmt,
        C_WS_SHUTDOWN: id_fmt,
        C_OP_SHUTDOWN: id_fmt,
        WS_GOODBYE: id_fmt,
        OP_GOODBYE: id_fmt,
        C_PING_WS: id_fmt,
        WS_PUBLISH_STATE: state_publish_fmt,
        OP_WATERING: op_ws_fmt,
        C_WATERING: id_fmt,
        C_GET_STATION: id_fmt,
        C_GET_OPERATOR: id_fmt,
        WS_I_AM: entity_fmt,
        OP_I_AM: entity_fmt,
    }
    if i[0] not in inst_v:
        if len(i) == 1:
            return SimpleNamespace(opcode=i[0])
        raise InsFmtException(i, "instrucción inexistente")
    return inst_v[i[0]](i)


def compress_namespace(opcode: int, ns: SimpleNamespace) -> bytes:
    def ws_fmt() -> bytes:
        return (
            bytes([opcode, ns.id, ns.x, ns.y, STX])
            + ns.name.encode()
            + bytes([ETX, ns.state])
        )

    def id_fmt() -> bytes:
        return bytes([opcode, ns.id])

    inst_c = {
        WS_REGISTER: ws_fmt,
        SERVER_ACK_WS_LOGIN: ws_fmt,
        WS_LOGIN: id_fmt,
        SERVER_ACK_WS_REGISTER: id_fmt,
    }
    if opcode not in inst_c:
        raise InsFmtException(bytes([opcode]), "Instrucción no existente")
    try:
        return inst_c[opcode]()
    except (AttributeError, ValueError) as e:
        raise InsFmtException(bytes([opcode]), str(e)) from e


def stamp() -> str:
    return time.strftime("%H:%M:%S")


def tell(mtype: str, msg: str):
    msgfmt = "[{}] CENTRAL: {}{}"
    print(
        msgfmt.format(stamp(), mtype, msg),
        end="\033[1;0m\n",
        file=sys.stdout if mtype != ERROR else sys.stderr,
    )


def get_netparams(args: tuple[str, ...]) -> tuple[int, str]:
    if len(args) == 0:
        print(USAGE_MSG)
        sys.exit(1)
    login_port: int
    kafka: str
    if len(args) == 1 and args[0] == "here":
        login_port = 5000
        kafka = "localhost:8000"
    elif (
        len(args) == 2
        and args[0].isnumeric()
        and (l := args[1].find(":")) not in (-1, 0, len(args[1]) - 1)
        and args[1][l + 1 :].isnumeric()
    ):
        login_port = int(args[0])
        kafka = args[1]
    else:
        print(USAGE_MSG)
        sys.exit(1)

    return login_port, kafka


def getdb() -> sqlite3.Connection:
    db_loc: Path
    if "DOCKER_VOLUME" in os.environ:
        db_loc = Path(os.environ["DOCKER_VOLUME"])
    elif "LOG_DIR" in os.environ:
        db_loc = Path(os.environ["LOG_DIR"])
    else:
        db_loc = Path(__file__).parent.parent / ".log"

    db_loc.mkdir(0o700, parents=True, exist_ok=True)
    db_loc /= "wm.db"
    db = sqlite3.Connection(db_loc)
    db.executescript(
        DB_SCHEMA
        + "INSERT OR IGNORE INTO operators (id, x, y, name) VALUES "
        + ",".join(
            [
                str((i, randint(0, 255), randint(0, 255), name))
                for i, name in enumerate(FIELD_OPERATORS, start=1)
            ]
        )
        + ";"
    )
    return db


def load_entities(
    db: sqlite3.Connection,
) -> tuple[dict[int, SimpleNamespace], ...]:
    db.row_factory = sqlite3.Row

    def extract(tbname: str) -> dict[int, SimpleNamespace]:
        return {
            r["id"]: SimpleNamespace(**r) for r in db.execute(f"SELECT * FROM {tbname}")
        }

    return extract("stations"), extract("operators")


def ws_login(
    ins: SimpleNamespace, stations: dict[int, SimpleNamespace], ws_lock: threading.Lock
) -> bytes:
    tell(OK, f"(login server) peticion de LOGIN con id={ins.id}")

    with ws_lock:
        station = stations.get(ins.id)

    if station is None:
        tell(
            ERROR,
            f"(login server) el id={ins.id} no existe, debe registrarse...",
        )
        return bytes([NACK])

    if not (station.x or station.y or station.name):
        tell(ERROR, f"(login server) la estación {ins.id} debe actualizar sus datos...")
        return bytes([NACK])

    return compress_namespace(SERVER_ACK_WS_LOGIN, station)


def ws_register(
    ins: SimpleNamespace, stations: dict[int, SimpleNamespace], ws_lock: threading.Lock
) -> bytes:
    tell(OK, f"(login server) comando REGISTER con id={ins.id}")

    with ws_lock:
        if ins.id in stations:
            tell(
                WARNING,
                f"(login server) el id={ins.id} ya estaba registrado, "
                "actualizando datos...",
            )
        stations[ins.id] = ins

    return compress_namespace(SERVER_ACK_WS_REGISTER, ins)


def login_server(
    login_port: int,
    stations: dict[int, SimpleNamespace],
    ws_lock: threading.Lock,
    watchdog_queue: Queue[bytes],
    shutdown_event: threading.Event,
) -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", login_port))
    srv.listen(10)
    srv.settimeout(0.3)
    instructions = {WS_LOGIN: ws_login, WS_REGISTER: ws_register}
    response = bytes([NACK])
    try:
        while not shutdown_event.is_set():
            try:
                conn, addr = srv.accept()
            except TimeoutError:
                continue
            tell(OK, f"(login server) {addr} se ha conectado")
            with conn:
                conn.settimeout(3)
                data = conn.recv(512)
                if not data:
                    tell(
                        WARNING,
                        f"(login server) el cliente {addr} se ha desconectado",
                    )
                    conn.sendall(bytes([NACK]))
                    continue
                if data[0] not in instructions:
                    tell(
                        ERROR,
                        f"(login server) {addr} ha enviado una instruccion desconocida",
                    )
                    conn.sendall(bytes([NACK]))
                    continue
                try:
                    ins = decompress_instruction(data)
                    response = instructions[data[0]](ins, stations, ws_lock)
                except (InsFmtException, UnicodeDecodeError) as err:
                    tell(
                        ERROR,
                        f"(login server) {addr} {err}",
                    )
                    response = bytes([NACK])
                conn.sendall(response)
                if response[0] != NACK:
                    watchdog_queue.put(data)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        tell(OK, "(login server) Saliendo...")
        srv.close()


def watchdog(q: Queue[bytes], shutdown_event: threading.Event) -> None:
    db = getdb()
    db.execute("PRAGMA journal_mode=EXCLUSIVE")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=5000")
    db.execute("BEGIN")
    try:
        start = time.monotonic()
        while not shutdown_event.is_set():
            ins = q.get()
            if ins[0] == EXIT:
                break

            ns: SimpleNamespace
            try:
                ns = decompress_instruction(ins)
            except InsFmtException as err:
                tell(ERROR, str(err))
                continue

            if ins[0] == WS_REGISTER or ins[0] == WS_I_AM:
                r = db.execute(
                    "SELECT id FROM stations WHERE id = ?", (ns.id,)
                ).fetchone()
                if r is None:
                    db.execute(
                        "INSERT INTO stations VALUES (?,?,?,?,?)",
                        (ns.id, ns.x, ns.y, ns.name, ns.state),
                    )
                    tell(
                        OK,
                        f"(watchdog) added watering station {ns.id}: {ns.name}({ns.x},{ns.y}) {WS_STATE_NAMES[ns.state]}",
                    )
                else:
                    db.execute(
                        "UPDATE stations SET x = ?, y = ?, name = ?,"
                        "state = ? WHERE id = ?",
                        (ns.x, ns.y, ns.name, ns.state, ns.id),
                    )
                    tell(
                        WARNING,
                        f"(watchdog) updated watering station {ns.id}: {ns.name}({ns.x},{ns.y}) {WS_STATE_NAMES[ns.state]}",
                    )
            elif ins[0] == WS_LOGIN:
                r = db.execute(
                    "SELECT * FROM stations WHERE id = ?", (ns.id,)
                ).fetchone()
                if r is None:
                    tell(
                        WARNING,
                        f"(watchdog) an unkown station ({ns.id})was trying to log into the database, ignoring...",
                    )
            elif ins[0] == WS_PUBLISH_STATE:
                tell(
                    OK,
                    f"(watchdog) updating station {ns.id} to {WS_STATE_NAMES[ns.state]}...",
                )
                cursor = db.execute(
                    "UPDATE stations SET state = ? WHERE id = ?", (ns.state, ns.id)
                )
                if cursor.rowcount == 0:
                    tell(
                        ERROR,
                        f"The station {ns.id} does not exist in the database",
                    )
            elif ins[0] == OP_I_AM:
                tell(OK, f"(watchdog) updating operator {ns.x}")
                cur = db.execute(
                    "UPDATE operators SET x = ?, y = ?, name = ?, cur_station = ? WHERE id = ?",
                    (ns.x, ns.y, ns.name, ns.cur_station, ns.id),
                )
                if cur.rowcount == 0:
                    db.execute(
                        "INSERT INTO operators VALUES (?,?,?,?,?)",
                        (ns.id, ns.x, ns.y, ns.name, ns.cur_station),
                    )
                    tell(
                        WARNING,
                        f"(watchdog) new operator {ns.id}: {ns.name} ({ns.x},{ns.y})",
                    )

            if time.monotonic() - start >= 1.0 or ins[0] == FLUSH_DB:
                tell(OK, "(watchdog) updating database...")
                db.commit()
                tell(OK, "(watchdog) database updated")
                db.execute("BEGIN")
                start = time.monotonic()

    except (KeyboardInterrupt, SystemExit):
        tell(OK, "(watchdog) main thread signaling system exit, quitting...")
    finally:
        db.commit()
        db.close()


def run_kafka(
    kafka: str,
    stations: dict[int, SimpleNamespace],
    operators: dict[int, SimpleNamespace],
    ws_lock: threading.Lock,
    watchdog_queue: Queue[bytes],
    ui_out: Queue[bytes],
    ui_input: Queue[bytes],
    shutdown_event: threading.Event,
) -> None:
    topics = [T_WATERING_REQUESTS, T_WS_STATE, T_OP_STATE]
    producer = Producer({"bootstrap.servers": kafka, "client.id": "wm-central"})
    consumer = Consumer(
        {
            "bootstrap.servers": kafka,
            "group.id": "wm-central",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(topics)
    tell(OK, f"(kafka) escuchando en {topics}@{kafka}")

    try:
        while not shutdown_event.is_set():
            # 1. Commands from the UI: C_* instructions, all of them [opcode, id]
            while not ui_input.empty():
                cmd = ui_input.get_nowait()
                if len(cmd) != 2:
                    tell(ERROR, f"(kafka) comando de la UI malformado: {cmd!r}")
                elif cmd[0] == C_OP_SHUTDOWN:
                    producer.produce(T_OP_CONTROL, cmd, key=bytes([cmd[1]]))
                elif cmd[0] in (C_REQ_WATERING, C_WS_SHUTDOWN, C_PING_WS, C_WATERING):
                    producer.produce(T_WS_CONTROL, cmd, key=bytes([cmd[1]]))
                else:
                    tell(ERROR, f"(kafka) comando de la UI desconocido: {cmd!r}")
            producer.poll(0)

            # 2. Messages from stations and operators
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

            if topic == T_WATERING_REQUESTS:
                if opcode == OP_REQ_WATERING and len(raw) == 3:
                    op_id, ws_id = raw[1], raw[2]
                    with ws_lock:
                        station = stations.get(ws_id)
                        available = station is not None and station.state == AVAILABLE
                    if op_id in operators and available:
                        tell(OK, f"(kafka) operador {op_id} pide regar en {ws_id}")
                        producer.produce(T_WS_CONTROL, raw, key=bytes([ws_id]))
                    else:
                        tell(
                            WARNING, f"(kafka) riego denegado: op {op_id} -> ws {ws_id}"
                        )
                        producer.produce(
                            T_OP_CONTROL,
                            bytes([WS_ANSWER_WATERING, NACK, op_id, ws_id]),
                            key=bytes([op_id]),
                        )
                elif opcode == OP_WATERING and len(raw) == 3:
                    ui_out.put(raw)
                else:
                    tell(ERROR, f"(kafka) {topic}: instrucción inválida {raw!r}")

            elif topic == T_WS_STATE:
                if opcode == WS_ANSWER_WATERING and len(raw) == 4:
                    # [WS_ANSWER_WATERING, ACK/NACK, op_id, ws_id]
                    answer, op_id, ws_id = raw[1], raw[2], raw[3]
                    producer.produce(T_OP_CONTROL, raw, key=bytes([op_id]))
                    if answer == ACK:
                        with ws_lock:
                            if ws_id in stations:
                                stations[ws_id].state = WATERING
                        state_ins = bytes([WS_PUBLISH_STATE, ws_id, WATERING])
                        watchdog_queue.put(state_ins)
                        ui_out.put(state_ins)
                elif (
                    opcode == WS_PUBLISH_STATE and len(raw) == 3 and raw[2] in WS_STATES
                ):
                    # [WS_PUBLISH_STATE, ws_id, state]
                    ws_id, state = raw[1], raw[2]
                    with ws_lock:
                        if ws_id in stations:
                            stations[ws_id].state = state
                    tell(OK, f"(kafka) estación {ws_id} -> {WS_STATE_NAMES[state]}")
                    watchdog_queue.put(raw)
                    ui_out.put(raw)
                elif opcode == WS_I_AM and len(raw) >= 7 and raw[4] == STX:
                    # [WS_I_AM, ws_id, x, y, STX, name..., ETX, state]
                    name_end = raw.find(ETX, 5)
                    state = raw[-1]
                    if name_end == -1 or state not in WS_STATES:
                        tell(ERROR, f"(kafka) WS_I_AM malformado: {raw!r}")
                        continue
                    ws_id = raw[1]
                    name = raw[5:name_end].decode(errors="replace")
                    with ws_lock:
                        stations[ws_id] = SimpleNamespace(
                            id=ws_id, x=raw[2], y=raw[3], name=name, state=state
                        )
                    watchdog_queue.put(raw)
                    ui_out.put(raw)
                elif opcode == WS_GOODBYE and len(raw) == 2:
                    # [WS_GOODBYE, ws_id]
                    ws_id = raw[1]
                    with ws_lock:
                        if ws_id in stations:
                            stations[ws_id].state = DISCONNECTED
                    tell(OK, f"(kafka) la estación {ws_id} se ha desconectado")
                    state_ins = bytes([WS_PUBLISH_STATE, ws_id, DISCONNECTED])
                    watchdog_queue.put(state_ins)
                    ui_out.put(state_ins)
                elif opcode == WS_WATERINFO and len(raw) == 4:
                    # [WS_WATERINFO, ws_id, liters_per_minute, accum]
                    ui_out.put(raw)
                else:
                    tell(ERROR, f"(kafka) {topic}: instrucción inválida {raw!r}")

            elif topic == T_OP_STATE:
                if opcode == OP_I_AM and len(raw) >= 7 and raw[4] == STX:
                    # [OP_I_AM, op_id, x, y, STX, name..., ETX, cur_station]
                    name_end = raw.find(ETX, 5)
                    if name_end == -1:
                        tell(ERROR, f"(kafka) OP_I_AM malformado: {raw!r}")
                        continue
                    op_id = raw[1]
                    operators[op_id] = SimpleNamespace(
                        id=op_id,
                        x=raw[2],
                        y=raw[3],
                        name=raw[5:name_end].decode(errors="replace"),
                        cur_station=raw[-1],
                    )
                    watchdog_queue.put(raw)
                    ui_out.put(raw)
                elif opcode == OP_GOODBYE and len(raw) == 2:
                    # [OP_GOODBYE, op_id]
                    tell(OK, f"(kafka) el operador {raw[1]} se ha desconectado")
                    ui_out.put(raw)
                else:
                    tell(ERROR, f"(kafka) {topic}: instrucción inválida {raw!r}")

    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        tell(OK, "(kafka) Saliendo...")
        shutdown_event.set()
        watchdog_queue.put(bytes([EXIT]))
        consumer.close()
        producer.flush(3)


def ui_server(ui_out: Queue[bytes]):
    pass


def main(*args):
    login_port, kafka = get_netparams(args)
    tell(
        OK, f"Argumentos de conexion correctos: login_port={login_port}, kafka={kafka}"
    )

    db: sqlite3.Connection = getdb()
    tell(OK, "Conectado exitosamente con la base de datos")

    stations, operators = load_entities(db)
    db.close()
    del db
    tell(
        OK,
        f"Se han precargado {len(stations)} estaciones y {len(operators)} operadores",
    )

    shutdown_event = threading.Event()
    ws_lock = threading.Lock()
    watchdog_queue = Queue()
    threading.Thread(
        target=login_server,
        name="wm-login",
        args=(login_port, stations, ws_lock, watchdog_queue, shutdown_event),
        daemon=True,
    ).start()

    threading.Thread(
        target=watchdog,
        name="wm-dbwatchdog",
        args=(watchdog_queue, shutdown_event),
        daemon=True,
    ).start()

    ui_in, ui_out = Queue(), Queue()
    run_kafka(
        kafka,
        stations,
        operators,
        ws_lock,
        watchdog_queue,
        ui_out,
        ui_in,
        shutdown_event,
    )


if __name__ == "__main__":
    main(*sys.argv[1:])

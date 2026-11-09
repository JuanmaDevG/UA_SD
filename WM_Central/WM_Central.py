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
from typing import Callable, override
from __future__ import annotations

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


# Kafka topics
T_WATERING_REQUESTS = "watering-requests"  # OP -> C
T_OP_CONTROL = "op-control"  # C -> OP
T_WS_CONTROL = "ws-control"  # C -> WS
T_WS_CHANNEL = "stations-channel"  # WS -> C
T_OP_CHANNEL = "operators-channel"  # OP -> C


# General instructions
EXIT = 0x00
STX = 0x01
ETX = 0x02
NACK = 0x03
ACK = 0x04

# Login server
WS_REGISTER = 0x05  # [id, x, y, STX, name (n bytes), ETX, state]
WS_LOGIN = 0x06  # [id]
SERVER_ACK_WS_LOGIN = 0x07  # [id, x, y, STX, name (n bytes), ETX, state]
SERVER_ACK_WS_REGISTER = 0x08  # [id]

# central -> operator instructions
OP_PING = 0x09  # [op_id]
OP_LS = 0x0A
OP_LOGOUT_PLEASE = 0x0B  # [op_id]
ANS_WATERING = 0x0C  # [op_id, ws_id, REASON]
PING_OP = 0x0D  # [op_id]

# operator -> central instructions
REQ_WATERING = 0x0E  # [op_id, ws_id]
CUR_STATION = 0x0F  # [op_id]
OP_I_AM = 0x10  # [op_id, x, y, STX, name (n bytes), ETX, cur_station]
OP_GOODBYE = 0x11  # [op_id]

# central -> station instructions
WS_PING = 0x12  # [ws_id]
WS_LS = 0x13
WS_LOGOUT_PLEASE = 0x14  # [ws_id]
WATERING_ASSIGN = 0x15  # [ws_id, op_id]
ASK_BUSY = 0x16  # [ws_id]

# station -> central instructions
PUBLISH_STATE = 0x17  # [ws_id, state]
WS_I_AM = 0x18  # [ws_id, x, y, STX, name (n bytes), ETX, state]
WS_GOODBYE = 0x19  # [ws_id]
WS_WATERINFO = 0x1A  # [ws_id, lpm, accum]

# miscelaneous
FLUSH_DB = 0x1B  # bytes[0x1C]


# REASON for ANS_WATERING
WATERING_OK = 0x00
NON_EXISTENT_STATION = 0x01
NON_EXISTENT_OPERATOR = 0x02
OPERATOR_IS_BUSY = 0x03
STATION_UNAVAILABLE = 0x04


class InsFmtError(Exception):
    # TODO: add instruction format printing
    def __init__(
        self, i: bytes, msg: str | Exception = "", expected_bytes: int | None = None
    ) -> None:
        self.instruction: bytes = i
        self.msg: str = str(msg)
        self.expected_bytes: int | None = expected_bytes
        xb_msg: str = "tiene un formato de instrucción erróneo"
        extra_info: str = ""
        if expected_bytes:
            xb_msg = f"esperaba {expected_bytes} bytes y tiene {len(i)} bytes"
        if self.msg:
            extra_info = ", " + self.msg
        super().__init__(f"(opcode: {i[0]}) {xb_msg} {extra_info}")


def decompress_instruction(i: bytes) -> SimpleNamespace:
    if not len(i):
        raise InsFmtError(i, "instrucción vacía")

    def entity_fmt(i: bytes) -> SimpleNamespace:
        name_end: int
        state: int
        if len(i) < 7:
            raise InsFmtError(
                i, "debe tener 7+ bytes (id, x, y, STX, name (n bytes), ETX, infobyte)"
            )
        if i[4] != STX:
            raise InsFmtError(i, "falta STX en el quinto byte")
        if (name_end := i.find(ETX, 5)) == -1:
            raise InsFmtError(i, "falta ETX como delimitador del campo 'name'")
        try:
            res = SimpleNamespace(id=i[1], x=i[2], y=i[3], name=i[5:name_end].decode())
        except UnicodeError as err:
            raise InsFmtError(i, err)

        if i[0] == OP_I_AM:
            res.cur_station = i[len(i) - 1]
        elif (state := i[len(i) - 1]) not in WS_STATES:
            raise InsFmtError(i, f"el estado de la estación con id={i[1]} no existe")
        else:
            res.state = state

        return res

    def id_fmt(i: bytes) -> SimpleNamespace:
        if len(i) != 2:
            raise InsFmtError(i, "Debe tener 2 bytes (opcode, id)")
        return SimpleNamespace(id=i[1])

    def op_ws_fmt(i: bytes) -> SimpleNamespace:
        if len(i) != 3:
            raise InsFmtError(i, "debe tener 3 bytes (opcode, op_id, ws_id)")
        return SimpleNamespace(op_id=i[1], ws_id=i[2])

    def ack_nack_fmt(i: bytes) -> SimpleNamespace:
        res: SimpleNamespace
        if len(i) != 4:
            raise InsFmtError(
                i, "debe tener 4 bytes (opcode, ACK/NACK, op_id, station_id)"
            )
        res = SimpleNamespace(ack=i[1], op_id=i[2], ws_id=i[3])
        return res

    def state_publish_fmt(i: bytes) -> SimpleNamespace:
        if len(i) != 3:
            raise InsFmtError(i, "debe tener 3 bytes (opcode, id, estado)")
        if i[2] >= len(WS_STATE_NAMES):
            raise InsFmtError(
                i, f"id={i[1]} tiene un estado inexistente con código {i[2]}"
            )
        return SimpleNamespace(id=i[1], state=i[2])

    inst_v = {
        WS_REGISTER: entity_fmt,
        SERVER_ACK_WS_LOGIN: entity_fmt,
        WS_LOGIN: id_fmt,
        SERVER_ACK_WS_REGISTER: id_fmt,
        REQ_WATERING: op_ws_fmt,
        ANS_WATERING: ack_nack_fmt,
        WS_GOODBYE: id_fmt,
        OP_GOODBYE: id_fmt,
        WS_I_AM: entity_fmt,
        OP_I_AM: entity_fmt,
    }
    if i[0] not in inst_v:
        if len(i) == 1:
            return SimpleNamespace(opcode=i[0])
        raise InsFmtError(i, "instrucción inexistente")
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
        raise InsFmtError(bytes([opcode]), "Instrucción no existente")
    try:
        return inst_c[opcode]()
    except (AttributeError, ValueError) as e:
        raise InsFmtError(bytes([opcode]), str(e)) from e


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
    _ = db.executescript(
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
                + "actualizando datos...",
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
                except (InsFmtError, UnicodeDecodeError) as err:
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
    _ = db.execute("PRAGMA journal_mode=EXCLUSIVE")
    _ = db.execute("PRAGMA synchronous=NORMAL")
    _ = db.execute("PRAGMA busy_timeout=5000")
    _ = db.execute("BEGIN")
    try:
        start = time.monotonic()
        while not shutdown_event.is_set():
            ins = q.get()
            if ins[0] == EXIT:
                break

            ns: SimpleNamespace
            try:
                ns = decompress_instruction(ins)
            except InsFmtError as err:
                tell(ERROR, str(err))
                continue

            if ins[0] == WS_REGISTER or ins[0] == WS_I_AM:
                r = db.execute(
                    "SELECT id FROM stations WHERE id = ?", (ns.id,)
                ).fetchone()
                if r is None:
                    _ = db.execute(
                        "INSERT INTO stations VALUES (?,?,?,?,?)",
                        (ns.id, ns.x, ns.y, ns.name, ns.state),
                    )
                    tell(
                        OK,
                        f"(watchdog) added watering station {ns.id}: {ns.name}({ns.x},{ns.y}) {WS_STATE_NAMES[ns.state]}",
                    )
                else:
                    _ = db.execute(
                        "UPDATE stations SET x = ?, y = ?, name = ?,"
                        + "state = ? WHERE id = ?",
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
            elif ins[0] == PUBLISH_STATE:
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
                    _ = db.execute(
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
                _ = db.execute("BEGIN")
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
    class Expectation:
        def __init__(self, opcode: int, author: int):
            self.opcode: int = opcode
            self.author: int = author

        @override
        def __eq__(self, value: object) -> bool:
            if isinstance(value, Expectation):
                return (
                    self.opcode == value.opcode
                    and self.author == value.author
                )
            if isinstance(value, bytes):
                if len(value) != 2:
                    return False
                return (
                    self.opcode == value[0]
                    and self.author == value[1]
                )
            return False

        @override
        def __hash__(self) -> int:
            return hash((self.opcode, self.author))

    class Action:
        def __init__(
            self,
            topic: str,
            opcode: int,
            params: bytes,
            maker: Callable[[Action, bytes], tuple[str, bytes, bytes]]
            # TODO: timeout_callback -> __del__ callback for when it was not valid to send something with a producer
        ):
            self.topic: str = topic
            self.opcode: int = opcode
            self.params: bytes = params
            # build(Action, raw_inst) -> tuple[topic, msg, key]
            self.maker: Callable[[Action, bytes], tuple[str, bytes, bytes]] = maker

        def build(self, raw_invoker: bytes):
            return self.maker(self, raw_invoker)

    class ActionChain:
        MAX_LIFETIME: float = 5.0
        def __init__(self, producer: Producer, *actions: Action):
            self.timestamp: float = time.monotonic()
            self.actions: tuple[Action, ...] = actions

        def are_valid(self) -> bool:
            return time.monotonic() - self.timestamp < Actions.MAX_LIFETIME

        def produce(self, raw_invoker: bytes) -> None:
            for a in self.actions:
                producer.produce(*a.build(raw_invoker))
            _ = producer.poll(0)

    topics = [T_WATERING_REQUESTS, T_WS_CHANNEL, T_OP_CHANNEL]
    producer = Producer({"bootstrap.servers": kafka, "client.id": "wm-central"})
    consumer = Consumer(
        {
            "bootstrap.servers": kafka,
            "group.id": "wm-central",
            "auto.offset.reset": "earliest",
        }
    )
    pending: dict[Expectation, ActionChain] = {}

    def handle_ui():
        while not ui_input.empty():
            cmd = ui_input.get_nowait()
            if len(cmd) != 2:
                tell(ERROR, f"(kafka) comando de la UI malformado: {cmd!r}")
            # TODO: handle ui commands
            else:
                tell(ERROR, f"(kafka) comando de la UI desconocido: {cmd!r}")
        _ = producer.poll(0)

    def fmt_op_log(op_id: int, ws_id: int | None, msg: str) -> str:
        ans: list[str] = ["(kafka) "]
        if op_id in operators:
            ans.append(f"(operador {op_id}, '{operators[op_id].name})")
        else:
            ans.append(f"(operador {op_id})")
        if ws_id is not None:
            ans.append(f" -> (estación {ws_id}")
            with ws_lock:
                if ws_id in stations:
                    ans.append(f", '{stations[ws_id].name}')")
                else:
                    ans.append(")")
        ans.append(f" :: {msg}")

        return "".join(ans)

    def handle_request_watering(raw: bytes):
        if len(raw) != 3:
            raise InsFmtError(raw, expected_bytes=3)
        op_id, ws_id = raw[1], raw[2]
        station: SimpleNamespace | None
        key = bytes([op_id])
        with ws_lock:
            station = stations.get(ws_id)
        if station is None:
            response = bytes([ANS_WATERING, op_id, ws_id, NON_EXISTENT_STATION])
            producer.produce(
                T_OP_CONTROL,
                response,
                key,
            )
            ui_out.put(response)
            tell(WARNING, fmt_op_log(op_id, ws_id, "estación inexistente"))
        elif op_id not in operators:
            response = bytes([ANS_WATERING, op_id, ws_id, NON_EXISTENT_OPERATOR])
            # Ask the ui to resolve this
            ui_out.put(response)
            tell(
                ERROR,
                fmt_op_log(
                    op_id,
                    ws_id,
                    "el operador no existe, ui resuelve...",
                ),
            )
        elif operators[op_id].cur_station is not None:
            response = bytes([ANS_WATERING, op_id, ws_id, OPERATOR_IS_BUSY])
            producer.produce(T_OP_CONTROL, response, key)
            ui_out.put(response)
            tell(WARNING, fmt_op_log(op_id, ws_id, "operador odupado"))
        elif station.state != AVAILABLE:
            response = bytes([ANS_WATERING, op_id, ws_id, STATION_UNAVAILABLE])
            producer.produce(T_OP_CONTROL, response, key)
            tell(WARNING, fmt_op_log(op_id, ws_id, "estación no disponible"))
        else:
            producer.produce(T_WS_CONTROL, bytes([WATERING_ASSIGN, ws_id, op_id]), key)
            pending[bytes([PUBLISH_STATE, ws_id])] = SimpleNamespace(
                timestamp=time.monotonic(),
                actions=[
                    SimpleNamespace(topic, opcode, params, callback)
                ]
                topic=T_OP_CONTROL,
                opcode=ANS_WATERING,
                params=bytes([op_id, ws_id]),
                callback=infer_reason
            )
            tell(OK, fmt_op_log(op_id, ws_id, "resolviendo solicitud de riego..."))

    def handle_publish_state(raw: bytes):
        pass

    consumer.subscribe(topics)
    tell(OK, f"(kafka) escuchando en {topics}@{kafka}")
    try:
        while not shutdown_event.is_set():
            handle_ui()

            msg = consumer.poll(0.2)
            if msg is None:
                continue
            elif msg.error():
                tell(ERROR, f"(kafka) {msg.error()}")
                continue
            raw = msg.value()
            if not raw:
                tell(WARNING, "(kafka) detectada una instrucción vacía")
                continue

            topic = msg.topic()
            opcode = raw[0]
            if len(raw) > 1 and raw[0:2] in pending:
                handle_pending(raw)
            if topic == T_WATERING_REQUESTS:
                if opcode == REQ_WATERING:
                    handle_request_watering(raw)
            elif topic == T_WS_CHANNEL:
                if opcode == PUBLISH_STATE and len(raw) == 3 and raw[2] in WS_STATES:
                    handle_publish_state(raw)
                    # [PUBLISH_STATE, ws_id, state]
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
                    state_ins = bytes([PUBLISH_STATE, ws_id, DISCONNECTED])
                    watchdog_queue.put(state_ins)
                    ui_out.put(state_ins)
                elif opcode == WS_WATERINFO and len(raw) == 4:
                    # [WS_WATERINFO, ws_id, liters_per_minute, accum]
                    ui_out.put(raw)
                else:
                    tell(ERROR, f"(kafka) {topic}: instrucción inválida {raw!r}")

            elif topic == T_OP_CHANNEL:
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
            _ = producer.poll(0)
            ui_out.put(raw)

    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        tell(OK, "(kafka) Saliendo...")
        shutdown_event.set()
        watchdog_queue.put(bytes([EXIT]))
        consumer.close()
        _ = producer.flush(3)


def ui_server(ui_out: Queue[bytes]):
    pass


def main(*args: str):
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
    watchdog_queue: Queue[bytes] = Queue()
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

    ui_input: Queue[bytes] = Queue()
    ui_out: Queue[bytes] = Queue()
    run_kafka(
        kafka,
        stations,
        operators,
        ws_lock,
        watchdog_queue,
        ui_out,
        ui_input,
        shutdown_event,
    )


if __name__ == "__main__":
    main(*sys.argv[1:])

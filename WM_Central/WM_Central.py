"""Water Management Central

(Later a proper description)
"""

import os
from pathlib import Path
from queue import Queue

import socket
import sqlite3
import sys
import threading
import time
from random import randint
from types import SimpleNamespace


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
    name TEXT,
    state INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS operators (
    id INTEGER PRIMARY KEY,
    x INTEGER, y INTEGER,
    name TEXT
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
# ...


INSTRUCTION_NAMES = {
    WS_REGISTER: "REGISTER_WATERING_STATION",
    WS_LOGIN: "LOGIN_WATERING_STATION",
}


class InsFmtException(Exception):
    def __init__(self, i: bytes, msg: str = "") -> None:
        self.instruction = i
        self.msg = msg
        super().__init__(f"(opcode: {i[0]}) {msg}")


def decompress_instruction(i: bytes) -> SimpleNamespace | None:
    if not len(i):
        raise InsFmtException(i, "Empty instruction")

    def ws_fmt(i: bytes) -> SimpleNamespace | None:
        name_end: int
        state: int
        if len(i) < 7:
            raise InsFmtException(
                i,
                f"Este opcode debe tener tener 7 bytes o mas, y tiene {len(i)}",
            )
        if i[4] != STX:
            raise InsFmtException(i, "Falta STX en el quinto byte")
        if (name_end := i.find(ETX, 5)) == -1:
            raise InsFmtException(i, "Falta ETX como delimitador del campo 'name'")
        if (state := i[len(i) - 1]) not in WS_STATES:
            raise InsFmtException(i, f"El estado de la WS con id={i[1]} no existe")
        return SimpleNamespace(
            id=i[1], x=i[2], y=i[3], name=i[5:name_end].decode(), state=state
        )

    def id_fmt(i: bytes) -> SimpleNamespace | None:
        if len(i) != 2:
            raise InsFmtException(
                i, f"Debe tener exactamente 2 bytes, y tiene {len(i)}"
            )
        return SimpleNamespace(id=i[1])

    inst_v = {
        WS_REGISTER: ws_fmt,
        SERVER_ACK_WS_LOGIN: ws_fmt,
        WS_LOGIN: id_fmt,
        SERVER_ACK_WS_REGISTER: id_fmt,
    }
    if i[0] not in inst_v:
        raise InsFmtException(i, "Instrucción no existente")
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
    """Load entities dictionaries
    Args:
        db: database connection
    Returns:
        stations: dict[id, entity_properties]
        operators: dict[id, entity_properties]
    """
    db.row_factory = sqlite3.Row

    def extract(tbname: str) -> dict[int, SimpleNamespace]:
        return {
            r["id"]: SimpleNamespace(**r) for r in db.execute(f"SELECT * FROM {tbname}")
        }

    return extract("stations"), extract("operators")


def ws_login(
    data: bytes, stations: dict[int, SimpleNamespace], ws_lock: threading.Lock
) -> bytes:
    if len(data) != 2:
        return bytes([NACK])

    station_id = data[1]
    tell(OK, f"(login server) peticion de LOGIN con id={station_id}")

    with ws_lock:
        station = stations.get(station_id)

    if station is None:
        tell(
            ERROR,
            f"(login server) el id={station_id} no existe, debe registrarse...",
        )
        return bytes([NACK])

    return (
        bytes(
            [
                SERVER_ACK_WS_LOGIN,
                station.id,
                station.x,
                station.y,
                STX,
            ]
        )
        + station.name.encode("utf-8")
        + bytes([ETX, station.state])
    )


def ws_register(
    data: bytes, stations: dict[int, SimpleNamespace], ws_lock: threading.Lock
) -> bytes:
    if len(data) < 7:
        return bytes([NACK])

    station_id = data[1]
    tell(OK, f"(login server) comando REGISTER con id={station_id}")

    name_end = data.find(ETX, 5)
    if data[4] != STX or name_end == -1 or name_end + 1 >= len(data):
        tell(
            ERROR,
            f"(login server) registro para id={station_id} formato incorrecto de "
            "argumentos, falta STX y/o ETX en el campo <name>",
        )
        return bytes([NACK])

    try:
        name = data[5:name_end].decode("utf-8")
    except UnicodeDecodeError:
        tell(
            ERROR,
            f"(login server) registro para id={station_id}: <name> no es UTF-8 valido",
        )
        return bytes([NACK])

    with ws_lock:
        if station_id in stations:
            tell(
                WARNING,
                f"(login server) el id={station_id} ya estaba registrado, "
                "actualizando datos...",
            )
        stations[station_id] = SimpleNamespace(
            id=station_id,
            name=name,
            state=data[name_end + 1],
            x=data[2],
            y=data[3],
        )

    return bytes([SERVER_ACK_WS_REGISTER, station_id])


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
                    response = instructions[data[0]](data, stations, ws_lock)
                except IndexError as ierr:
                    tell(
                        ERROR,
                        f"(login server) instruccion malformada "
                        f"{INSTRUCTION_NAMES[data[0]]} de {addr} "
                        f"{ierr}",
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
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=5000")
    changes = db.cursor()
    changes.execute("BEGIN")
    try:
        start = time.monotonic()
        while not shutdown_event.is_set():
            ins = q.get()
            if ins[0] == EXIT:
                break

            if ins[0] == WS_REGISTER:
                r = db.execute(
                    "SELECT id FROM stations WHERE id = ?", (ins[1],)
                ).fetchone()
                if r is None:
                    db.execute("INSERT INTO stations VALUES (?,?,?,?,?)")
                db.execute("")
            elif ins[0] == WS_LOGIN:
                db.execute("")
            # TODO: apply kafka instructions when available

            if time.monotonic() - start >= 5.0:
                tell(OK, "(watchdog) updating database...")
                db.commit()
                tell(OK, "(watchdog) database updated")
                start = time.monotonic()
                changes.execute("BEGIN")

    except (KeyboardInterrupt, SystemExit):
        tell(OK, "(watchdog) main thread system exit, quitting...")
        db.commit()
    finally:
        db.close()


def run_kafka(
    kafka: str,
    stations: dict[int, SimpleNamespace],
    operators: dict[int, SimpleNamespace],
    watchdog_queue: Queue[bytes],
) -> None:
    # Consumer + Producer
    # Answer to events and instructions
    pass


# ui_server helpers


def ui_server():
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

    run_kafka(kafka, stations, operators, watchdog_queue)


if __name__ == "__main__":
    main(*sys.argv[1:])

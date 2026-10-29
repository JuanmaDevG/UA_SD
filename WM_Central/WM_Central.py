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
from random import uniform
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
    x REAL, y REAL,
    name TEXT,
    blocked INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS operators (
    id TEXT PRIMARY KEY,
    x REAL, y REAL,
    name TEXT
);
"""

FIELD_OPERATORS = ["Jeffrey", "Donald", "Charlie", "Kanye"]


def stamp() -> str:
    return time.strftime("%H:%M:%S")


# System message coloring
OK = "\033[1;32m"
WARNING = "\033[1;33m"
ERROR = "\033[1;31m"


# General instructions
EXIT = 0x00
STX = 0x01
ETX = 0x02
NACK = 0x03
ACK = 0x04


# Login server instructions
ST_REGISTER = 0x05  # bytes[0x05, id, x, y, STX, name (n bytes), ETX, state]
ST_LOGIN = 0x06  # bytes[0x06, id (1 byte)]
ST_ACK_LOGGIN = 0x07  # bytes[0x07, id, x, y, STX, name (n bytes), ETX, state]


# Kafka instructions
# ...


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
        db_loc = Path(__file__).parent

    db_loc.mkdir(0o700, parents=True, exist_ok=True)
    db_loc /= "ws.db"
    db = sqlite3.Connection(db_loc)
    db.executescript(
        DB_SCHEMA
        + "INSERT OR IGNORE INTO operators (id, x, y, name) VALUES "
            + ",".join([str(
                (i, round(uniform(0,1), 2),round(uniform(0,1), 2), name))
                for i, name in enumerate(FIELD_OPERATORS, start=1)
            ])
        + ";"
    );

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


def st_login(
    data: bytes, stations: dict[int, SimpleNamespace], st_lock: threading.Lock
) -> int:
    res = ACK
    tell(
        OK,
        f"(login server) peticion de LOGIN con id={data[1]}",
    )
    st_lock.acquire()
    if int(data[1]) not in stations:
        res = NACK
    st_lock.release()
    if res == NACK:
        tell(
            ERROR,
            f"(login server) el id={data[1]} no existe, debe registrarse...",
        )
    return res


def st_register(
    data: bytes, stations: dict[int, SimpleNamespace], st_lock: threading.Lock
) -> int:
    res = ACK
    exists = False
    st_lock.acquire()
    if (id := data[1]) in stations:
        exists = True
    st_lock.release()

    tell(OK, f"(login server) comando REGISTER con id={id}")
    if exists:
        tell(
            WARNING,
            f"(login server) el id={id} ya estaba registrado, actualizando datos...",
        )

    if data[4] == STX and (name_end := data.find(ETX, 5)) != -1:
        name: str = data[3 : name_end + 1].decode("utf-8")
        state = int(data[name_end + 2])
        st_lock.acquire()
        stations[id] = SimpleNamespace(
            id=id,
            name=name,
            state=state,
            x=data[2] / 255,
            y=data[3] / 255,
        )
        st_lock.release()
    else:
        res = NACK
        tell(
            ERROR,
            f"(login server) registro para id={id} formato incorrecto de argumentos"
            ", falta STX y/o ETX en el campo <name>",
        )

    return res


def login_server(
    login_port: int,
    stations: dict[int, SimpleNamespace],
    st_lock: threading.Lock,
    watchdog_queue: Queue[bytes],
) -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", login_port))
    srv.listen(10)
    instructions = {ST_LOGIN: st_login, ST_REGISTER: st_register}
    response = bytearray([ACK])
    try:
        while True:
            conn, addr = srv.accept()
            tell(OK, f"(login server) {addr} se ha conectado")
            with conn:
                data = conn.recv(512)
                if int(data[0]) not in instructions:
                    tell(
                        ERROR,
                        f"(login server) {addr} ha enviado una instruccion desconocida",
                    )
                    srv.sendall(bytes([NACK]))
                    continue
                response[0] = instructions[data[0]](data, stations, st_lock)
                conn.sendall(response)
                if response[0] == ACK:
                    watchdog_queue.put(data)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        srv.close()


def watchdog(db: sqlite3.Connection, q: Queue[bytes]) -> None:
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=5000")
    changes = db.cursor()
    changes.execute("BEGIN")
    try:
        start = time.monotonic()
        while True:
            v = q.get()
            if v[0] == EXIT:
                break

            # TODO: apply instruction to cursor

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
    tell(
        OK,
        f"Se han precargado {len(stations)} estaciones y {len(operators)} operadores",
    )

    # TODO: change the st_lock with a threading.Lock
    st_lock = threading.Lock()
    watchdog_queue = Queue()
    threading.Thread(
        target=login_server,
        name="wm-login",
        args=(login_port, stations, st_lock, watchdog_queue),
        daemon=True,
    ).start()

    threading.Thread(
        target=watchdog,
        name="wm-dbwatchdog",
        args=(db, watchdog_queue),
        daemon=True,
    ).start()

    run_kafka(kafka, stations, operators, watchdog_queue)


if __name__ == "__main__":
    main(*sys.argv[1:])

"""Water Management Central

(Later a proper description)
"""

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

QUERY_ALL = "SELECT * FROM %s;"

import os
from pathlib import Path
import socket
import sqlite3
import sys
import threading
import time

from queue import Queue
from types import SimpleNamespace


def stamp() -> str:
    return time.strftime("%H:%M:%S")


# System message status
OK = "\033[1;32m"
WARNING = "\033[1;33m"
ERROR = "\033[1;31m"


# System instructions
EXIT = 0x00
# ...


def tell(mtype: str, msg: str):
    msgfmt = "[{}] CENTRAL: {}{}"
    print(
        msgfmt.format(stamp(), mtype, msg),
        end="\033[1;0m\n",
        file=sys.stdout if mtype != ERROR else sys.stderr,
    )


def get_netparams(args: tuple[str, str]) -> tuple[int, str]:
    login_port: int
    kafka: str
    if len(args) == 1 and args[0] == "here":
        login_port = 5000
        kafka = "localhost:8000"
    elif (
        len(args) == 2
        and args[0].isnumeric()
        and (l := args[1].find(":")) != -1
        and args[1][l + 1 :].isnumeric()
    ):
        login_port = int(args[0])
        kafka = args[1]
    else:
        print(USAGE_MSG)
        sys.exit()

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
    db.executescript(DB_SCHEMA)

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
            r["id"]: SimpleNamespace(**r) for r in db.execute(QUERY_ALL.format(tbname))
        }

    return extract("stations"), extract("operators")


# login_server helpers


def login_server(stations: dict[int, SimpleNamespace]) -> None:
    try:
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        pass  # TODO: write changes of the queue


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
            if v[0] == 0:
                break

            # TODO: apply instruction to cursor

            if time.monotonic() - start >= 5.0:
                tell(OK, "watchdog writing to database")
                db.commit()
                start = time.monotonic()
                changes.execute("BEGIN")

    except (KeyboardInterrupt, SystemExit):
        tell(OK, "(watchdog) main thread system exit, quitting...")
        db.commit()
    finally:
        db.close()


# kafka_server helpers


def kafka_server(
    stations: dict[int, SimpleNamespace],
    operators: dict[int, SimpleNamespace],
    watchdog_queue: Queue,
) -> None:
    # Consumer + Producer
    # Make instructions
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

    threading.Thread(
        target=login_server, name="wm-login", args=(stations), daemon=True
    ).run()

    watchdog_queue = Queue()
    # login thread
    # kafka thread


if __name__ == "__main__":
    main(*(sys.argv[1:]))

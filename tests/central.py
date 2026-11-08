import os
import shutil
import sqlite3
from types import SimpleNamespace
import unittest
import threading
from queue import Queue
import socket
import psutil
import time

from tests import LOG_DIR, DOCKER_VOLUME, ENV_LOG_DIR
from WM_Central import FLUSH_DB, STX, ETX, NACK, WATERING

# Import functions to test
from WM_Central import get_netparams
from WM_Central import FIELD_OPERATORS, getdb
from WM_Central import load_entities
from WM_Central import (
    DISCONNECTED,
    AVAILABLE,
    LEAK,
    OUT_OF_SERVICE,
    WS_REGISTER,
    WS_LOGIN,
    SERVER_ACK_WS_LOGIN,
    SERVER_ACK_WS_REGISTER,
    login_server,
)
from WM_Central import (
    EXIT,
    WS_I_AM,
    OP_I_AM,
    WS_PUBLISH_STATE,
    watchdog,
)


class TestParams(unittest.TestCase):
    def test_01_noparams(self):
        self.assertRaises(SystemExit, get_netparams, ())

    def test_02_wrongparams(self):
        self.assertRaises(SystemExit, get_netparams, ("not_here",))
        self.assertRaises(SystemExit, get_netparams, ("not_a_port", "localhost:8000"))
        self.assertRaises(SystemExit, get_netparams, ("5000", "not_a_broker"))
        self.assertRaises(SystemExit, get_netparams, ("5000", "broker.domain:"))
        self.assertRaises(SystemExit, get_netparams, ("5000", ":8000"))
        self.assertRaises(SystemExit, get_netparams, ("5000", ":"))
        self.assertRaises(
            SystemExit, get_netparams, ("5000", "broker.domain:not_a_port")
        )

    def test_03_lessparams(self):
        self.assertRaises(SystemExit, get_netparams, ("5000"))
        self.assertRaises(SystemExit, get_netparams, ("localhost:8000"))

    def test_04_toomanyparams(self):
        self.assertRaises(
            SystemExit, get_netparams, ("5000", "localhost:8000", "unrelated")
        )

    def test_05_correctparams(self):
        self.assertEqual(
            get_netparams(("2000", "localhost:3000")), (2000, "localhost:3000")
        )
        self.assertEqual(get_netparams(("here",)), (5000, "localhost:8000"))


class TestDBIntegrity(unittest.TestCase):
    def test_01_no_env(self):
        self.db = getdb()
        self.assertTrue(LOG_DIR.exists())
        self.check_data(self.db)
        self.db.close()

    def test_02_env_vars(self):
        os.environ["DOCKER_VOLUME"] = str(DOCKER_VOLUME)
        self.db = getdb()
        self.assertTrue(DOCKER_VOLUME.exists())
        del os.environ["DOCKER_VOLUME"]
        self.check_data(self.db)
        self.db.close()

        os.environ["LOG_DIR"] = str(ENV_LOG_DIR)
        self.db = getdb()
        self.assertTrue(ENV_LOG_DIR.exists())
        del os.environ["LOG_DIR"]
        self.check_data(self.db)
        self.db.close()

    def tearDown(self):
        self.db.close()

    def check_data(self, db: sqlite3.Connection):
        db.row_factory = sqlite3.Row
        rows: list[sqlite3.Row] = db.execute(
            "SELECT id, name FROM operators"
        ).fetchall()
        self.assertEqual(len(rows), 4)
        for i, fo in enumerate(FIELD_OPERATORS):
            self.assertEqual(rows[i]["id"], i + 1)
            self.assertEqual(rows[i]["name"], fo)
        self.assertIsNone(db.execute("SELECT id FROM stations LIMIT 1").fetchone())

    @classmethod
    def tearDownClass(cls) -> None:
        os.remove(str(LOG_DIR / "wm.db"))
        shutil.rmtree(DOCKER_VOLUME)
        shutil.rmtree(ENV_LOG_DIR)


class TestEntities(unittest.TestCase):
    def test_01_load_entities(self):
        stations, operators = load_entities(self.db)
        self.assertEqual(len(stations), 0)
        sample_op: SimpleNamespace = next(iter(operators.values()))
        self.assertTrue(hasattr(sample_op, "id"))
        self.assertTrue(hasattr(sample_op, "x"))
        self.assertTrue(hasattr(sample_op, "y"))
        self.assertTrue(hasattr(sample_op, "name"))
        for i, fo in enumerate(operators.values()):
            self.assertEqual(fo.id, i + 1)
            self.assertEqual(fo.name, FIELD_OPERATORS[i])

    def test_02_load_with_station(self):
        self.db.executescript(
            "INSERT INTO stations (id, x, y, name) VALUES (1, 2, 3, 'shawty')"
        )
        stations, _ = load_entities(self.db)
        sample_ws: SimpleNamespace = next(iter(stations.values()))
        self.assertTrue(hasattr(sample_ws, "id"))
        self.assertTrue(hasattr(sample_ws, "x"))
        self.assertTrue(hasattr(sample_ws, "y"))
        self.assertTrue(hasattr(sample_ws, "name"))
        self.assertTrue(hasattr(sample_ws, "state"))
        self.assertEqual(sample_ws.id, 1)
        self.assertEqual(sample_ws.x, 2)
        self.assertEqual(sample_ws.y, 3)
        self.assertEqual(sample_ws.name, "shawty")
        self.assertEqual(sample_ws.state, 0)

    def setUp(self) -> None:
        self.db = getdb()

    def tearDown(self) -> None:
        self.db.close()

    @classmethod
    def tearDownClass(cls) -> None:
        os.remove(str(LOG_DIR / "wm.db"))


class TestLoginServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.login_port = 5000
        cls.lock = threading.Lock()

    def setUp(self):
        self.shutdown = threading.Event()
        self.stations = {
            1: SimpleNamespace(
                id=1, x=127, y=127, name="City Center", state=DISCONNECTED
            ),
            2: SimpleNamespace(id=2, x=76, y=25, name="Sample Park", state=AVAILABLE),
        }
        self.q = Queue()
        self.server = threading.Thread(
            target=login_server,
            name="login-server",
            args=(self.login_port, self.stations, self.lock, self.q, self.shutdown),
        )
        self.client: socket.socket
        self.server.start()

    def wait_for_server(self):
        available = False
        while not available:
            for conn in psutil.net_connections(kind="inet"):
                if (
                    conn.laddr
                    and conn.laddr.port == self.login_port
                    and conn.status == psutil.CONN_LISTEN
                ):
                    available = True

    def client_interaction(self, msg: bytes) -> bytes:
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.wait_for_server()
        self.client.connect(("0.0.0.0", self.login_port))
        self.client.settimeout(2)
        self.client.sendall(msg)
        response = self.client.recv(512)
        if self.client.recv(512):  # Conn not closed
            self.fail()
        self.client.close()
        return response

    def server_is_alive(self) -> bool:
        for conn in psutil.net_connections(kind="inet"):
            if (
                conn.laddr
                and conn.laddr.port == self.login_port
                and conn.status == psutil.CONN_LISTEN
            ):
                return True
        return False

    def tearDown(self):
        self.shutdown.set()
        if hasattr(self, "client") and self.client and self.client.fileno() != -1:
            self.client.close()
        try:
            self.server.join(timeout=3)
            self.assertFalse(self.server.is_alive())
        except TimeoutError:
            self.fail()

    def test_01_start_and_shutdown(self):
        pass

    def test_02_register(self):
        response = self.client_interaction(
            bytes([WS_REGISTER, 3, 50, 50, STX, *list(b"Six Seven"), ETX, AVAILABLE])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 3]))
        with self.lock:
            self.assertTrue(3 in self.stations)

    def test_03_register_wsexist_update(self):
        response = self.client_interaction(
            bytes([WS_REGISTER, 2, 100, 100, STX, *b"New name", ETX, LEAK])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 2]))
        with self.lock:
            self.assertTrue(2 in self.stations)
            self.assertEqual(
                self.stations[2],
                SimpleNamespace(
                    id=2,
                    x=100,
                    y=100,
                    name="New name",
                    state=LEAK,
                ),
            )

    def test_04_login(self):
        response = self.client_interaction(bytes([WS_LOGIN, 2]))
        self.assertEqual(
            response,
            bytes(
                [SERVER_ACK_WS_LOGIN, 2, 76, 25, STX, *b"Sample Park", ETX, AVAILABLE]
            ),
        )

    def test_05_login_wsnoexist(self):
        response = self.client_interaction(bytes([WS_LOGIN, 3]))
        self.assertEqual(response, bytes([NACK]))

    def test_06_nonexistent_instruction(self):
        response = self.client_interaction(bytes([NACK, 0x01, 0x02]))
        self.assertEqual(response, bytes([NACK]))

    def test_07_malformed_instruction(self):
        response = self.client_interaction(bytes([WS_LOGIN, 1, *b"bad"]))
        self.assertEqual(response, bytes([NACK]))
        response = self.client_interaction(
            bytes([WS_REGISTER, 1, *b"longer string is worse"])
        )
        self.assertEqual(response, bytes([NACK]))

    def test_08_multiple_ops(self):
        response = self.client_interaction(bytes([WS_LOGIN, 1]))
        self.assertEqual(
            response,
            bytes(
                [
                    SERVER_ACK_WS_LOGIN,
                    1,
                    127,
                    127,
                    STX,
                    *b"City Center",
                    ETX,
                    DISCONNECTED,
                ]
            ),
        )
        response = self.client_interaction(
            bytes([WS_REGISTER, 2, 76, 25, STX, *b"Sample Park", ETX, LEAK])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 2]))
        with self.lock:
            self.assertEqual(
                self.stations[2],
                SimpleNamespace(id=2, x=76, y=25, name="Sample Park", state=LEAK),
            )
        response = self.client_interaction(
            bytes([WS_REGISTER, 3, 123, 40, STX, *b"Las mil viviendas", ETX, LEAK])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 3]))
        with self.lock:
            self.assertEqual(
                self.stations[3],
                SimpleNamespace(
                    id=3, x=123, y=40, name="Las mil viviendas", state=LEAK
                ),
            )
        response = self.client_interaction(
            bytes([WS_REGISTER, 4, 45, 12, STX, *b"Centro", ETX, OUT_OF_SERVICE])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 4]))
        with self.lock:
            self.assertEqual(
                self.stations[4],
                SimpleNamespace(id=4, x=45, y=12, name="Centro", state=OUT_OF_SERVICE),
            )

        response = self.client_interaction(
            bytes([WS_REGISTER, 5, 78, 23, STX, *b"La Estacion", ETX, LEAK])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 5]))
        with self.lock:
            self.assertEqual(
                self.stations[5],
                SimpleNamespace(id=5, x=78, y=23, name="La Estacion", state=LEAK),
            )

        response = self.client_interaction(
            bytes([WS_REGISTER, 6, 200, 55, STX, *b"El Parque", ETX, LEAK])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 6]))
        with self.lock:
            self.assertEqual(
                self.stations[6],
                SimpleNamespace(id=6, x=200, y=55, name="El Parque", state=LEAK),
            )

        response = self.client_interaction(
            bytes([WS_REGISTER, 7, 33, 90, STX, *b"San Jose", ETX, LEAK])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 7]))
        with self.lock:
            self.assertEqual(
                self.stations[7],
                SimpleNamespace(id=7, x=33, y=90, name="San Jose", state=LEAK),
            )

        response = self.client_interaction(
            bytes([WS_REGISTER, 8, 150, 67, STX, *b"Las Flores", ETX, DISCONNECTED])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 8]))
        with self.lock:
            self.assertEqual(
                self.stations[8],
                SimpleNamespace(
                    id=8, x=150, y=67, name="Las Flores", state=DISCONNECTED
                ),
            )

        response = self.client_interaction(
            bytes([WS_REGISTER, 9, 89, 34, STX, *b"El Mirador", ETX, LEAK])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 9]))
        with self.lock:
            self.assertEqual(
                self.stations[9],
                SimpleNamespace(id=9, x=89, y=34, name="El Mirador", state=LEAK),
            )

        response = self.client_interaction(
            bytes([WS_REGISTER, 10, 12, 78, STX, *b"La Colina", ETX, AVAILABLE])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 10]))
        with self.lock:
            self.assertEqual(
                self.stations[10],
                SimpleNamespace(id=10, x=12, y=78, name="La Colina", state=AVAILABLE),
            )

        response = self.client_interaction(
            bytes([WS_REGISTER, 11, 175, 44, STX, *b"Villa Nueva", ETX, LEAK])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 11]))
        with self.lock:
            self.assertEqual(
                self.stations[11],
                SimpleNamespace(id=11, x=175, y=44, name="Villa Nueva", state=LEAK),
            )

        response = self.client_interaction(
            bytes([WS_REGISTER, 12, 66, 111, STX, *b"Puerto Sol", ETX, AVAILABLE])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 12]))
        with self.lock:
            self.assertEqual(
                self.stations[12],
                SimpleNamespace(id=12, x=66, y=111, name="Puerto Sol", state=AVAILABLE),
            )

        response = self.client_interaction(
            bytes([WS_REGISTER, 13, 99, 5, STX, *b"Monte Alto", ETX, WATERING])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 13]))
        with self.lock:
            self.assertEqual(
                self.stations[13],
                SimpleNamespace(id=13, x=99, y=5, name="Monte Alto", state=WATERING),
            )


class TestWatchdog(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.db_path = LOG_DIR / "wm.db"

    def setUp(self) -> None:
        if self.db_path.exists():
            os.remove(str(self.db_path))
        os.environ.pop("DOCKER_VOLUME", None)
        os.environ.pop("LOG_DIR", None)
        self.q: Queue[bytes] = Queue()
        self.shutdown = threading.Event()
        self.thread = threading.Thread(
            target=watchdog,
            name="test-watchdog",
            args=(self.q, self.shutdown),
            daemon=True,
        )
        self.thread.start()

    def tearDown(self) -> None:
        self.shutdown.set()
        self.q.put(bytes([EXIT]))
        try:
            self.thread.join(timeout=3)
        except TimeoutError:
            print("Thread is still alive")
        if hasattr(self, "db") and self.db:
            try:
                self.db.close()
            except sqlite3.ProgrammingError:
                print("The database was not even open")

    def flush(self) -> None:
        self.q.put(bytes([FLUSH_DB]))
        time.sleep(0.3)

    def query(self, sql: str, params: tuple = ()) -> list[tuple]:
        self.db = sqlite3.connect(self.db_path)
        try:
            return self.db.execute(sql, params).fetchall()
        finally:
            self.db.close()

    def station(self, i: int) -> tuple | None:
        r = self.query("SELECT id, x, y, name, state FROM stations WHERE id = ?", (i,))
        return r[0] if r else None

    def operator(self, i: int) -> tuple | None:
        r = self.query(
            "SELECT id, x, y, name, cur_station FROM operators WHERE id = ?", (i,)
        )
        return r[0] if r else None

    @staticmethod
    def ws(opcode: int, id: int, x: int, y: int, name: str, state: int) -> bytes:
        return bytes([opcode, id, x, y, STX]) + name.encode() + bytes([ETX, state])

    @staticmethod
    def op(id: int, x: int, y: int, name: str, cur_station: int) -> bytes:
        return (
            bytes([OP_I_AM, id, x, y, STX]) + name.encode() + bytes([ETX, cur_station])
        )

    def test_01_exit_instruction_stops_thread(self):
        self.q.put(bytes([EXIT]))
        self.thread.join(timeout=3)
        self.assertFalse(self.thread.is_alive())

    def test_02_shutdown_event_stops_thread(self):
        self.shutdown.set()
        self.thread.join(timeout=3)
        self.assertFalse(self.thread.is_alive())

    def test_03_unknown_instruction_does_not_crash(self):
        self.q.put(bytes([0xFF, 0x01, 0x02]))
        time.sleep(0.3)
        self.assertTrue(self.thread.is_alive())
        self.q.put(bytes([EXIT]))
        self.thread.join(timeout=3)
        self.assertFalse(self.thread.is_alive())

    def test_04_periodic_commit(self):
        time.sleep(1)
        self.assertTrue(self.thread.is_alive())
        self.q.put(bytes([EXIT]))
        self.thread.join(timeout=3)
        self.assertFalse(self.thread.is_alive())

    def test_05_register_inserts_station(self):
        self.q.put(self.ws(WS_REGISTER, 7, 10, 20, "Norte", AVAILABLE))
        self.flush()
        self.assertTrue(self.thread.is_alive())
        self.assertEqual(self.station(7), (7, 10, 20, "Norte", AVAILABLE))

    def test_06_register_updates_station(self):
        self.q.put(self.ws(WS_REGISTER, 7, 10, 20, "Norte", AVAILABLE))
        self.q.put(self.ws(WS_REGISTER, 7, 11, 21, "Norte 2", LEAK))
        self.flush()
        self.assertEqual(self.station(7), (7, 11, 21, "Norte 2", LEAK))
        self.assertEqual(len(self.query("SELECT * FROM stations")), 1)

    def test_07_ws_i_am_inserts_and_updates(self):
        self.q.put(self.ws(WS_I_AM, 3, 1, 2, "Sur", WATERING))
        self.q.put(self.ws(WS_I_AM, 3, 1, 2, "Sur", OUT_OF_SERVICE))
        self.flush()
        self.assertEqual(self.station(3), (3, 1, 2, "Sur", OUT_OF_SERVICE))

    def test_08_login_unknown_station_does_not_crash(self):
        self.q.put(bytes([WS_LOGIN, 99]))
        time.sleep(0.3)
        self.assertTrue(self.thread.is_alive())
        self.assertIsNone(self.station(99))

    def test_09_login_known_station_keeps_data(self):
        self.q.put(self.ws(WS_REGISTER, 4, 5, 6, "Este", AVAILABLE))
        self.q.put(bytes([WS_LOGIN, 4]))
        self.flush()
        self.assertTrue(self.thread.is_alive())
        self.assertEqual(self.station(4), (4, 5, 6, "Este", AVAILABLE))

    def test_10_publish_state_updates_station(self):
        self.q.put(self.ws(WS_REGISTER, 2, 0, 0, "Oeste", AVAILABLE))
        self.q.put(bytes([WS_PUBLISH_STATE, 2, LEAK]))
        self.flush()
        self.assertTrue(self.thread.is_alive())
        self.assertEqual(self.station(2), (2, 0, 0, "Oeste", LEAK))

    def test_11_publish_state_unknown_station_does_not_crash(self):
        self.q.put(bytes([WS_PUBLISH_STATE, 50, LEAK]))
        self.flush()
        self.assertTrue(self.thread.is_alive())
        self.assertIsNone(self.station(50))

    def test_12_operator_i_am_updates_existing(self):
        self.q.put(self.op(1, 30, 40, FIELD_OPERATORS[0], 9))
        self.flush()
        self.assertTrue(self.thread.is_alive())
        self.assertEqual(self.operator(1), (1, 30, 40, FIELD_OPERATORS[0], 9))

    def test_13_operator_i_am_inserts_new(self):
        self.q.put(self.op(20, 1, 1, "Nuevo", 3))
        self.flush()
        self.assertTrue(self.thread.is_alive())
        self.assertEqual(self.operator(20), (20, 1, 1, "Nuevo", 3))

    def test_14_malformed_instructions_do_not_crash(self):
        for ins in (
            bytes([WS_REGISTER, 1, 2]),  # too short
            bytes([WS_REGISTER, 1, 2, 3, 0x00, 65, ETX, AVAILABLE]),  # no STX
            bytes([WS_REGISTER, 1, 2, 3, STX, 65, 66, AVAILABLE]),  # no ETX
            bytes([WS_REGISTER, 1, 2, 3, STX, 65, ETX, 0xEE]),  # bad state
            bytes([WS_REGISTER, 1, 2, 3, STX, 0xFF, ETX, AVAILABLE]),  # bad utf-8
            bytes([WS_LOGIN]),
            bytes([WS_PUBLISH_STATE, 1]),
            bytes([WS_PUBLISH_STATE, 1, 0xEE]),
        ):
            self.q.put(ins)
        time.sleep(0.3)
        self.assertTrue(self.thread.is_alive())
        self.flush()
        self.assertEqual(self.query("SELECT * FROM stations"), [])

    def test_15_exit_commits_pending_changes(self):
        self.q.put(self.ws(WS_REGISTER, 8, 1, 1, "Final", AVAILABLE))
        self.q.put(bytes([EXIT]))
        self.thread.join(timeout=3)
        self.assertFalse(self.thread.is_alive())
        self.assertEqual(self.station(8), (8, 1, 1, "Final", AVAILABLE))


class TestKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_01_tolerate_new_operator(self):
        pass

    def test_02_tolerate_operator_update(self):
        pass

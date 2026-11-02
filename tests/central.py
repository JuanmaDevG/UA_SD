import os
import shutil
import sqlite3
from types import SimpleNamespace
import unittest
import threading
from queue import Queue
import socket
import psutil

from tests import LOG_DIR, DOCKER_VOLUME, ENV_LOG_DIR
from WM_Central import STX, ETX

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


class TestParams(unittest.TestCase):
    def test_noparams(self):
        self.assertRaises(SystemExit, get_netparams, ())

    def test_wrongparams(self):
        self.assertRaises(SystemExit, get_netparams, ("not_here",))
        self.assertRaises(SystemExit, get_netparams, ("not_a_port", "localhost:8000"))
        self.assertRaises(SystemExit, get_netparams, ("5000", "not_a_broker"))
        self.assertRaises(SystemExit, get_netparams, ("5000", "broker.domain:"))
        self.assertRaises(SystemExit, get_netparams, ("5000", ":8000"))
        self.assertRaises(SystemExit, get_netparams, ("5000", ":"))
        self.assertRaises(
            SystemExit, get_netparams, ("5000", "broker.domain:not_a_port")
        )

    def test_lessparams(self):
        self.assertRaises(SystemExit, get_netparams, ("5000"))
        self.assertRaises(SystemExit, get_netparams, ("localhost:8000"))

    def test_toomanyparams(self):
        self.assertRaises(
            SystemExit, get_netparams, ("5000", "localhost:8000", "unrelated")
        )

    def test_correctparams(self):
        self.assertEqual(
            get_netparams(("2000", "localhost:3000")), (2000, "localhost:3000")
        )
        self.assertEqual(get_netparams(("here",)), (5000, "localhost:8000"))


class TestDBIntegrity(unittest.TestCase):
    def test_no_env(self):
        self.db = getdb()
        self.assertTrue(LOG_DIR.exists())
        self.check_data(self.db)
        self.db.close()

    def test_env_vars(self):
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
    def test_load_entities(self):
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

    def test_load_with_station(self):
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
            1: SimpleNamespace(id=1, x=0.5, y=0.5, name="City Center", state=0),
            2: SimpleNamespace(id=1, x=0.3, y=0.1, name="Sample Park"),
        }
        self.q = Queue()
        self.server = threading.Thread(
            target=login_server,
            name="login-server",
            args=(self.login_port, self.stations, self.lock, self.q, self.shutdown),
        )
        self.client: socket.socket

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

    def test_01_obey_shutdown_onwait(self):
        self.server.start()
        self.shutdown.set()
        try:
            self.server.join(timeout=3)
            self.assertFalse(self.server.is_alive())
        except TimeoutError:
            self.fail()

    def test_02_register(self):
        self.server.start()
        response = self.client_interaction(
            bytes([WS_REGISTER, 3, 50, 50, STX, *list(b"Six Seven"), ETX, AVAILABLE])
        )
        self.assertEqual(response, bytes([SERVER_ACK_WS_REGISTER, 3]))
        # TODO: check self.stations
        with self.lock:
            pass

    def test_03_register_duplicated(self):
        self.server.start()
        # TODO: maybe is better to receive a NACK
        response = self.client_interaction(
            bytes([WS_REGISTER, 2, 100, 100, STX, *b"New name"])
        )

    def test_04_login(self):
        pass

    def test_05_login_failed(self):
        pass

    def test_06_nonexistent_instruction(self):
        pass

    def test_07_malformed_instruction(self):
        pass

    def test_08_register_wsnoexist(self):
        pass


class TestWatchdog(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        pass

    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

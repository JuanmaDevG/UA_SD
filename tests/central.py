import os
import shutil
import sqlite3
from types import SimpleNamespace
import unittest

from tests import LOG_DIR, DOCKER_VOLUME, ENV_LOG_DIR
from WM_Central import STX, ETX

from WM_Central import WS_REGISTER, get_netparams
from WM_Central import FIELD_OPERATORS, getdb
from WM_Central import load_entities


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


class TestRegisterWaterStation(unittest.TestCase):
    def test_register(self):
        data = bytes([WS_REGISTER, 1, 1, 1, STX, *b"Saint Jones", ETX, 0])
        # TODO: make dictionary

    def test_register_noexist(self):
        pass

    def test_register_updates(self):
        pass

    def setUp(self):
        pass  # dict and stuff

    def tearDown(self):
        pass  # cleanup if necessary

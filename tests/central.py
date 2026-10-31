import os
import shutil
import sqlite3
import unittest

from WM_Central import get_netparams
from WM_Central import FIELD_OPERATORS, getdb
from tests import *


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
        db = getdb()
        self.assertTrue(LOG_DIR.exists())
        self.check_data(db)

    def test_env_vars(self):
        os.environ["DOCKER_VOLUME"] = str(DOCKER_VOLUME)
        db = getdb()
        self.assertTrue(DOCKER_VOLUME.exists())
        del os.environ["DOCKER_VOLUME"]
        self.check_data(db)

        os.environ["LOG_DIR"] = str(ENV_LOG_DIR)
        db = getdb()
        self.assertTrue(ENV_LOG_DIR.exists())
        del os.environ["LOG_DIR"]
        self.check_data(db)

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
        os.remove(LOG_DIR / "wm.db")
        shutil.rmtree(DOCKER_VOLUME)
        shutil.rmtree(ENV_LOG_DIR)

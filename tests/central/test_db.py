import os
from pathlib import Path
import shutil
import sqlite3
import unittest

from WM_Central import FIELD_OPERATORS, getdb
from tests import LOG_DIR


DOCKER_VOLUME = Path("/tmp/wm-docker")
ENV_LOG_DIR = Path("/tmp/wm-log")


def check_data(tc: unittest.TestCase, db: sqlite3.Connection):
    db.row_factory = sqlite3.Row
    rows: list[sqlite3.Row] = db.execute("SELECT id, name FROM operators").fetchall()
    tc.assertEqual(len(rows), 4)
    for i, fo in enumerate(FIELD_OPERATORS):
        tc.assertEqual(rows[i]["id"], i + 1)
        tc.assertEqual(rows[i]["name"], fo)
    tc.assertEqual(db.execute("SELECT 1 FROM stations").arraysize, 0)


class TestDBIntegrity(unittest.TestCase):
    def test_no_env(self):
        db = getdb()
        self.assertTrue(LOG_DIR.exists())
        check_data(self, db)

    def test_env_vars(self):
        os.environ["DOCKER_VOLUME"] = str(DOCKER_VOLUME)
        db = getdb()
        self.assertTrue(LOG_DIR.exists())
        del os.environ["DOCKER_VOLUME"]
        check_data(self, db)

        os.environ["LOG_DIR"] = str(ENV_LOG_DIR)
        db = getdb()
        self.assertTrue(ENV_LOG_DIR.exists())
        del os.environ["LOG_DIR"]
        check_data(self, db)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(LOG_DIR)
        shutil.rmtree(DOCKER_VOLUME)
        shutil.rmtree(ENV_LOG_DIR)


if __name__ == "__main__":
    unittest.main()

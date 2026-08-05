import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from yaal import Yaal

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_API = ROOT / "tests" / "fixtures" / "api"
SQLITE_SCHEMA = ROOT / "docker" / "sqlite" / "schema.sql"

DEFAULT_PG_URL = "postgresql://yaal:yaal@127.0.0.1:54329/yaal"
DEFAULT_MYSQL_URL = "mysql://yaal:yaal@127.0.0.1:33069/yaal"


def _wait_for(connect, attempts=60, delay=0.5):
    last_error = None
    for _ in range(attempts):
        try:
            connect()
            return True
        except Exception as exc:
            last_error = exc
            time.sleep(delay)
    raise unittest.SkipTest("database not ready: %s" % last_error)


def _build_yaal(db_url):
    y = Yaal(str(FIXTURE_API), debug=True)
    y.setup_data_provider("db", db_url)
    return y


def _fetch_user(y, user_id):
    return y.query("user/%s" % user_id, "get")


class SqlToJsonMixin:
    db_url = None

    def test_user_with_nested_roles(self):
        y = _build_yaal(self.db_url)
        result = _fetch_user(y, 1)
        self.assertEqual(
            result,
            {
                "id": 1,
                "name": "admin",
                "roles": [
                    {"id": 1, "name": "Administrator"},
                    {"id": 2, "name": "User"},
                ],
            },
        )

    def test_user_with_single_role(self):
        y = _build_yaal(self.db_url)
        result = _fetch_user(y, 2)
        self.assertEqual(
            result,
            {
                "id": 2,
                "name": "guest",
                "roles": [
                    {"id": 2, "name": "User"},
                ],
            },
        )


class TestSqliteIntegration(SqlToJsonMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        cls._db_path = path
        with sqlite3.connect(path) as con:
            con.executescript(SQLITE_SCHEMA.read_text())
        # Absolute sqlite paths need four slashes: sqlite3:////tmp/db.sqlite
        cls.db_url = "sqlite3:///%s" % path

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink(cls._db_path)
        except OSError:
            pass


@unittest.skipUnless(
    os.environ.get("YAAL_INTEGRATION") == "1",
    "set YAAL_INTEGRATION=1 with docker compose up",
)
class TestPostgresIntegration(SqlToJsonMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg2

        cls.db_url = os.environ.get("YAAL_PG_URL", DEFAULT_PG_URL)

        def connect():
            con = psycopg2.connect(cls.db_url)
            con.close()

        _wait_for(connect)


@unittest.skipUnless(
    os.environ.get("YAAL_INTEGRATION") == "1",
    "set YAAL_INTEGRATION=1 with docker compose up",
)
class TestMysqlIntegration(SqlToJsonMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import mysql.connector

        cls.db_url = os.environ.get("YAAL_MYSQL_URL", DEFAULT_MYSQL_URL)

        def connect():
            # mysql connector does not accept the full URI used by Yaal
            con = mysql.connector.connect(
                host="127.0.0.1",
                port=33069,
                user="yaal",
                password="yaal",
                database="yaal",
            )
            con.close()

        _wait_for(connect)


if __name__ == "__main__":
    unittest.main()

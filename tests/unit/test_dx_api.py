import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from yaal import Yaal
from yaal_errors import DescriptorNotFoundError, UnsupportedDatabaseUrlError, YaalError

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_API = ROOT / "tests" / "fixtures" / "api"
SQLITE_SCHEMA = ROOT / "docker" / "sqlite" / "schema.sql"


class TestDxApi(unittest.TestCase):

    def test_query_user_get(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            with sqlite3.connect(path) as con:
                con.executescript(SQLITE_SCHEMA.read_text())
            y = Yaal(str(FIXTURE_API), debug=True)
            y.setup_data_provider("db", "sqlite3:///%s" % path)
            result = y.query("user/get", args={"id": 1})
            self.assertEqual(result["id"], 1)
            self.assertEqual(result["name"], "admin")
            self.assertEqual(len(result["roles"]), 2)
        finally:
            os.unlink(path)

    def test_query_json_user_get(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            with sqlite3.connect(path) as con:
                con.executescript(SQLITE_SCHEMA.read_text())
            y = Yaal(str(FIXTURE_API), debug=True)
            y.setup_data_provider("db", "sqlite3:///%s" % path)
            raw = y.query_json("user/get", args={"id": 1})
            self.assertIn('"id": 1', raw)
            self.assertIn('"name": "admin"', raw)
        finally:
            os.unlink(path)

    def test_explain_sql_elides_null_args_id(self):
        y = Yaal(str(FIXTURE_API), debug=True)
        explained = y.explain_sql("user/get", args={})
        self.assertTrue(explained)
        sql = explained[0]["sql"]
        self.assertIn("u.active = 1", sql)
        self.assertNotIn("user_id = ?", sql)
        self.assertEqual(explained[0]["parameters"], [])

    def test_explain_sql_binds_args_id(self):
        y = Yaal(str(FIXTURE_API), debug=True)
        explained = y.explain_sql("user/get", args={"id": 1})
        self.assertTrue(explained)
        sql = explained[0]["sql"]
        self.assertIn("user_id = ?", sql)
        self.assertEqual(explained[0]["parameters"], [1])

    def test_unsupported_database_url(self):
        y = Yaal(str(FIXTURE_API), debug=True)
        with self.assertRaises(UnsupportedDatabaseUrlError):
            y.setup_data_provider("db", "redis://localhost/0")

    def test_bad_database_url(self):
        y = Yaal(str(FIXTURE_API), debug=True)
        with self.assertRaises(ValueError):
            y.setup_data_provider("db", "not-a-url")

    def test_missing_descriptor(self):
        y = Yaal(str(FIXTURE_API), debug=True)
        with self.assertRaises(DescriptorNotFoundError):
            y.create_descriptor("missing/get")

    def test_missing_data_provider(self):
        y = Yaal(str(FIXTURE_API), debug=True)
        with self.assertRaises(YaalError):
            y.get_data_provider("db")


if __name__ == "__main__":
    unittest.main()

# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from yaal import Yaal, FileContentReader
from yaal_errors import (
    DescriptorNotFoundError,
    PathEscapeError,
    UnsupportedDatabaseUrlError,
    YaalError,
)

ROOT = Path(__file__).resolve().parents[3]
FIXTURE_API = ROOT / "tests" / "fixtures" / "api"
SQLITE_SCHEMA = ROOT / "docker" / "sqlite" / "schema.sql"

try:
    import clickhouse_driver  # noqa: F401
    HAS_CLICKHOUSE = True
except ImportError:
    HAS_CLICKHOUSE = False


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

    @unittest.skipUnless(HAS_CLICKHOUSE, "clickhouse-driver not installed")
    def test_clickhouse_url_registers_and_uses_percent_s(self):
        y = Yaal(str(FIXTURE_API), debug=True)
        y.setup_data_provider("db", "clickhouse://yaal:yaal@127.0.0.1:9000/yaal")
        self.assertEqual(y._data_provider_schemes["db"], "clickhouse")
        self.assertEqual(y._default_placeholder(), "%s")
        self.assertIn("db", y._data_providers)

    @unittest.skipIf(HAS_CLICKHOUSE, "clickhouse-driver installed")
    def test_clickhouse_missing_driver_message(self):
        y = Yaal(str(FIXTURE_API), debug=True)
        with self.assertRaises(YaalError) as ctx:
            y.setup_data_provider("db", "clickhouse://yaal:yaal@127.0.0.1:9000/yaal")
        self.assertIn("yaal[clickhouse]", str(ctx.exception))

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

    def test_query_user_list(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            with sqlite3.connect(path) as con:
                con.executescript(SQLITE_SCHEMA.read_text())
            y = Yaal(str(FIXTURE_API), debug=True)
            y.setup_data_provider("db", "sqlite3:///%s" % path)
            result = y.query("user/list")
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0]["name"], "admin")
            active_only = y.query("user/list", args={"active": 1})
            self.assertEqual(len(active_only), 2)
        finally:
            os.unlink(path)

    def test_path_escape_rejected(self):
        reader = FileContentReader(str(FIXTURE_API))
        with self.assertRaises(PathEscapeError):
            reader.get_sql("$", "../secrets")


if __name__ == "__main__":
    unittest.main()

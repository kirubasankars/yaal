# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""SQLite e2e coverage for the example fixtures under tests/fixtures/api/."""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from yaal import Yaal

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_API = ROOT / "tests" / "fixtures" / "api"
SCHEMA = ROOT / "docker" / "sqlite" / "schema.sql"
FLAGS_SCHEMA = ROOT / "docker" / "sqlite" / "flags_schema.sql"


class TestFixtureExamples(unittest.TestCase):
    def setUp(self):
        fd, self._db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        fd2, self._flags_path = tempfile.mkstemp(suffix=".db")
        os.close(fd2)
        sqlite3.connect(self._db_path).executescript(SCHEMA.read_text())
        sqlite3.connect(self._flags_path).executescript(FLAGS_SCHEMA.read_text())
        self._yaal = Yaal(str(FIXTURE_API), debug=True)
        self._yaal.setup_data_provider("db", "sqlite3:///" + self._db_path)
        self._yaal.setup_data_provider("flags", "sqlite3:///" + self._flags_path)

    def tearDown(self):
        for path in (self._db_path, self._flags_path):
            try:
                os.unlink(path)
            except OSError:
                pass

    def test_user_get_nested_roles(self):
        result = self._yaal.query("user/get", args={"id": 1})
        self.assertEqual(result["id"], 1)
        self.assertEqual(result["name"], "admin")
        self.assertEqual(len(result["roles"]), 2)

    def test_user_nested_child_sql(self):
        result = self._yaal.query("user/nested", args={"id": 1})
        self.assertEqual(result["id"], 1)
        self.assertEqual(result["name"], "admin")
        self.assertEqual(
            result["roles"],
            [
                {"id": 1, "name": "Administrator"},
                {"id": 2, "name": "User"},
            ],
        )
        # Same shape as join + parent_rows.
        joined = self._yaal.query("user/get", args={"id": 1})
        self.assertEqual(result, joined)

    def test_user_page_branches(self):
        result = self._yaal.query(
            "user/page", args={"page": 1, "page_size": 10}
        )
        self.assertEqual(result["paging"]["page"], 1)
        self.assertEqual(result["paging"]["page_size"], 10)
        self.assertEqual(result["paging"]["total_count"], 2)
        self.assertEqual(len(result["data"]), 2)
        admin = result["data"][0]
        self.assertEqual(admin["name"], "admin")
        self.assertEqual(len(admin["roles"]), 2)

    def test_user_page_second_page_empty(self):
        result = self._yaal.query(
            "user/page", args={"page": 2, "page_size": 10}
        )
        self.assertEqual(result["paging"]["total_count"], 2)
        self.assertEqual(result["data"], [])

    def test_user_page_size_limits_users_not_join_rows(self):
        result = self._yaal.query(
            "user/page", args={"page": 1, "page_size": 1}
        )
        self.assertEqual(len(result["data"]), 1)
        admin = result["data"][0]
        self.assertEqual(admin["name"], "admin")
        self.assertEqual(len(admin["roles"]), 2)

    def test_user_create_multi_twig(self):
        result = self._yaal.query(
            "user/create", payload={"id": 3, "name": "newbie"}
        )
        self.assertEqual(result["id"], 3)
        self.assertEqual(result["name"], "newbie")
        self.assertEqual(result["roles"], [{"id": 2, "name": "User"}])

        listed = self._yaal.query("user/get", args={"id": 3})
        self.assertEqual(listed["name"], "newbie")

    def test_report_summary_with_aggregation(self):
        result = self._yaal.query("report/summary")
        self.assertEqual(result["user_count"], 2)
        self.assertEqual(result["active_count"], 2)
        self.assertEqual(result["assignment_count"], 3)

    def test_user_combine_multi_database(self):
        result = self._yaal.query("user/combine", args={"id": 1})
        self.assertEqual(result["app"], {"id": 1, "name": "admin"})
        self.assertEqual(result["flags"], {"user_id": 1, "vip": 1})


if __name__ == "__main__":
    unittest.main()

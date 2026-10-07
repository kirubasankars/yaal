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
from yaal_drivers import open as open_db

ROOT = Path(__file__).resolve().parents[3]
FIXTURE_API = ROOT / "tests" / "fixtures" / "api"
SCHEMA = ROOT / "docker" / "sqlite" / "schema.sql"


class TestFixtureExamples(unittest.TestCase):
    def setUp(self):
        fd, self._db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        sqlite3.connect(self._db_path).executescript(SCHEMA.read_text())
        self._yaal = Yaal(str(FIXTURE_API), debug=True)
        self._provider = open_db("sqlite3:///" + self._db_path)

    def tearDown(self):
        try:
            os.unlink(self._db_path)
        except OSError:
            pass

    def test_user_get_nested_roles(self):
        result = self._yaal.query(self._provider, "user/get", args={"id": 1})
        self.assertEqual(result["id"], 1)
        self.assertEqual(result["name"], "admin")
        self.assertEqual(len(result["roles"]), 2)

    def test_user_list_sort_dir(self):
        rows = self._yaal.query(self._provider, "user/list", args={"sort": "id", "dir": "desc"})
        self.assertEqual([r["id"] for r in rows], [2, 1])
        explained = self._yaal.explain_sql(self._provider, 
            "user/list", args={"sort": "name", "dir": "asc"}
        )
        self.assertIn("u.user_name", explained[0]["sql"])

    def test_user_nested_child_sql(self):
        result = self._yaal.query(self._provider, "user/nested", args={"id": 1})
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
        joined = self._yaal.query(self._provider, "user/get", args={"id": 1})
        self.assertEqual(result, joined)

    def test_user_page_branches(self):
        result = self._yaal.query(self._provider, 
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
        result = self._yaal.query(self._provider, 
            "user/page", args={"page": 2, "page_size": 10}
        )
        self.assertEqual(result["paging"]["total_count"], 2)
        self.assertEqual(result["data"], [])

    def test_user_page_size_limits_users_not_join_rows(self):
        result = self._yaal.query(self._provider, 
            "user/page", args={"page": 1, "page_size": 1}
        )
        self.assertEqual(len(result["data"]), 1)
        admin = result["data"][0]
        self.assertEqual(admin["name"], "admin")
        self.assertEqual(len(admin["roles"]), 2)

    def test_user_create_multi_twig(self):
        result = self._yaal.query(self._provider, 
            "user/create", payload={"id": 3, "name": "newbie"}
        )
        self.assertEqual(result["id"], 3)
        self.assertEqual(result["name"], "newbie")
        self.assertEqual(result["roles"], [{"id": 2, "name": "User"}])

        listed = self._yaal.query(self._provider, "user/get", args={"id": 3})
        self.assertEqual(listed["name"], "newbie")

    def test_report_summary_with_aggregation(self):
        result = self._yaal.query(self._provider, "report/summary")
        self.assertEqual(result["user_count"], 2)
        self.assertEqual(result["active_count"], 2)
        self.assertEqual(result["assignment_count"], 3)

if __name__ == "__main__":
    unittest.main()

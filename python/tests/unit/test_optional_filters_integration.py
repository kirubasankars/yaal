# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""SQLite integration tests for optional IN, multi-param optional, and optional_groups."""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from yaal import Yaal

ROOT = Path(__file__).resolve().parents[3]
FIXTURE_API = ROOT / "tests" / "fixtures" / "api"
SCHEMA = ROOT / "docker" / "sqlite" / "schema.sql"


class TestOptionalFiltersIntegration(unittest.TestCase):
    def setUp(self):
        fd, self._db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        sqlite3.connect(self._db_path).executescript(SCHEMA.read_text())
        self._yaal = Yaal(str(FIXTURE_API), debug=True)
        self._yaal.setup_data_provider("db", "sqlite3:///" + self._db_path)

    def tearDown(self):
        try:
            os.unlink(self._db_path)
        except OSError:
            pass

    def test_optional_in_elided_returns_all_users(self):
        rows = self._yaal.query("user/optional_in")
        self.assertEqual([r["id"] for r in rows], [1, 2])

    def test_optional_in_filters_by_list(self):
        rows = self._yaal.query("user/optional_in", args={"ids": [2]})
        self.assertEqual(rows, [{"id": 2, "name": "guest"}])

    def test_optional_in_expands_placeholders(self):
        explained = self._yaal.explain_sql(
            "user/optional_in", args={"ids": [1, 2]}
        )
        self.assertIn("in (?, ?)", explained[0]["sql"].lower())
        self.assertEqual(explained[0]["parameters"], [1, 2])

    def test_optional_multi_all_bound(self):
        rows = self._yaal.query(
            "user/optional_multi",
            args={"active": 1, "ids": [1], "name": "admin"},
        )
        self.assertEqual(rows, [{"id": 1, "name": "admin", "active": 1}])

    def test_optional_multi_all_elided(self):
        rows = self._yaal.query("user/optional_multi")
        self.assertEqual(len(rows), 2)

    def test_optional_multi_partial_args_errors(self):
        with self.assertRaises(ValueError) as ctx:
            self._yaal.query("user/optional_multi", args={"active": 1})
        self.assertIn("partial parameters", str(ctx.exception).lower())

    def test_optional_groups_in_per_row(self):
        rows = self._yaal.query(
            "user/groups_in",
            args={"pairs": [{"ids": [1]}, {"ids": [2]}]},
        )
        self.assertEqual([r["id"] for r in rows], [1, 2])

    def test_optional_groups_in_single_row_list(self):
        rows = self._yaal.query(
            "user/groups_in", args={"pairs": [{"ids": [1, 2]}]}
        )
        self.assertEqual([r["id"] for r in rows], [1, 2])

    def test_optional_groups_in_elided(self):
        rows = self._yaal.query("user/groups_in")
        self.assertEqual([r["id"] for r in rows], [1, 2])

    def test_optional_groups_in_multi_row_parenthesized_beside_and(self):
        explained = self._yaal.explain_sql(
            "user/groups_filter",
            args={"pairs": [{"ids": [1]}, {"ids": [2]}], "active": 1},
        )
        sql = explained[0]["sql"].lower()
        self.assertIn("((", sql)
        self.assertIn(") or (", sql)
        self.assertIn("and (", sql)

    def test_groups_filter_optional_scalar_and_groups_in(self):
        rows = self._yaal.query(
            "user/groups_filter",
            args={"pairs": [{"ids": [1]}], "active": 1},
        )
        self.assertEqual(rows, [{"id": 1, "name": "admin"}])

    def test_groups_filter_elided_optional_keeps_group(self):
        rows = self._yaal.query(
            "user/groups_filter", args={"pairs": [{"ids": [2]}]}
        )
        self.assertEqual(rows, [{"id": 2, "name": "guest"}])


if __name__ == "__main__":
    unittest.main()

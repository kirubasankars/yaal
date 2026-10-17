# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from yaal import CompileError, compile

SQL = """--(id, active)--
select * from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})
"""

PARTIAL = """--(id, active)--
select * from users u
where optional(u.id = {{id}} and u.active = {{active}})
"""


def norm(sql):
    return " ".join(sql.split())


class CompileTests(unittest.TestCase):
    def test_question_and_numbered(self):
        compiled = compile(SQL, {"id": 1})
        self.assertEqual(len(compiled["statements"]), 1)
        statement = compiled["statements"][0]
        self.assertEqual(
            norm(statement["sql"]),
            "select * from users u where (u.id = ?)",
        )
        self.assertEqual(
            statement["parameters"],
            [{"name": "id", "type": "integer", "value": 1}],
        )

        numbered = compile(SQL, {"id": 1}, placeholder="numbered")
        self.assertIn("$1", numbered["statements"][0]["sql"])
        self.assertNotIn("?", numbered["statements"][0]["sql"])

    def test_fixture_cases(self):
        path = Path(__file__).resolve().parents[2] / "tests/fixtures/sql_compile/cases.json"
        cases = json.loads(path.read_text())
        self.assertGreater(len(cases), 100)
        for case in cases:
            name = case["name"]
            with self.subTest(name=name):
                if "expect_error_contains" in case:
                    with self.assertRaises(CompileError) as caught:
                        compile(case["sql"], case["params"])
                    self.assertIn(
                        case["expect_error_contains"].lower(),
                        str(caught.exception).lower(),
                    )
                    continue
                compiled = compile(case["sql"], case["params"])
                self.assertEqual(len(compiled["statements"]), 1, name)
                statement = compiled["statements"][0]
                self.assertEqual(norm(statement["sql"]), norm(case["expect_sql"]), name)
                names = [param["name"] for param in statement["parameters"]]
                self.assertEqual(names, case.get("expect_param_names", []), name)

    def test_partial_optional_is_an_error(self):
        with self.assertRaises(CompileError) as caught:
            compile(PARTIAL, {"id": 1})
        self.assertIn("partial", str(caught.exception).lower())

    def test_nul_in_sql_is_an_error(self):
        with self.assertRaises(CompileError) as caught:
            compile("--(id)--\nselect {{id}}\0 or 1 = 1", {"id": 1})
        self.assertIn("nul", str(caught.exception).lower())

    def test_windows_wheel_uses_the_win_amd64_tag(self):
        from pack import platform_tag

        self.assertEqual(platform_tag("win32", "AMD64"), "win_amd64")
        self.assertEqual(platform_tag("linux", "x86_64"), "linux_x86_64")
        self.assertEqual(platform_tag("darwin", "arm64"), "macosx_11_0_arm64")


if __name__ == "__main__":
    unittest.main()

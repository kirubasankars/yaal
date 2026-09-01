# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import json
import re
import unittest
from pathlib import Path

from yaal_parser import lexer, parser, compile_sql

CASES_PATH = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "sql_compile" / "cases.json"


def _normalize_ws(sql):
    return re.sub(r"\s+", " ", sql).strip()


def _load_cases():
    with open(CASES_PATH, "r") as f:
        return json.load(f)


class TestNullableFilters(unittest.TestCase):

    def test_shared_sql_compile_goldens(self):
        cases = _load_cases()
        self.assertTrue(cases, "sql_compile goldens missing")
        for case in cases:
            with self.subTest(case["name"]):
                sql = case["sql"]
                expect_err = case.get("expect_error_contains")
                if expect_err:
                    with self.assertRaises((TypeError, ValueError)) as ctx:
                        parser(lexer(sql), "$")
                    self.assertIn(expect_err, str(ctx.exception))
                    continue

                ast = parser(lexer(sql), "$")
                twig = ast["sql_stmts"][0]
                if "expect_nullable_contains" in case:
                    for name in case["expect_nullable_contains"]:
                        self.assertIn(name, twig.get("nullable") or [])

                nulls = case.get("nulls") or []
                placeholder = case.get("placeholder") or "?"
                compiled = compile_sql(twig, nulls, placeholder)
                self.assertEqual(
                    _normalize_ws(compiled["content"]),
                    _normalize_ws(case["expect_sql"]),
                )
                self.assertEqual(
                    [p["name"] for p in compiled["parameters"]],
                    case.get("expect_param_names") or [],
                )


if __name__ == "__main__":
    unittest.main()

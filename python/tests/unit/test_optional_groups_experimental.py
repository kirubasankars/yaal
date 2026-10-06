# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Extended optional_groups coverage (experimental fixture; not part of core goldens)."""

import json
import re
import unittest
from pathlib import Path

from yaal_executor import DataProviderHelper
from yaal_parser import (
    compile_sql,
    group_shapes_for_compile,
    lexer,
    parser,
)

CASES_PATH = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "fixtures"
    / "sql_compile"
    / "optional_groups_experimental.json"
)


def _normalize_ws(sql):
    return re.sub(r"\s+", " ", sql).strip()


def _load_cases():
    with open(CASES_PATH, "r") as f:
        return json.load(f)


def _group_open_token(twig):
    for tok in twig.get("content") or []:
        if tok.get("type") == "brace" and tok.get("value") == "(" and tok.get("group_source"):
            return tok
    return None


class _Shape:
    def __init__(self, props):
        self._props = props

    def get_prop(self, name):
        return self._props.get(name)


class TestOptionalGroupsExperimental(unittest.TestCase):

    def test_experimental_sql_compile_goldens(self):
        cases = _load_cases()
        self.assertTrue(cases, "optional_groups experimental fixture missing")
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

                if "expect_group_fields" in case:
                    open_tok = _group_open_token(twig)
                    self.assertIsNotNone(open_tok)
                    self.assertEqual(open_tok.get("group_fields"), case["expect_group_fields"])

                nulls = case.get("nulls") or []
                placeholder = case.get("placeholder") or "?"
                sort_map = case.get("sort_map") or {}
                compile_kwargs = {
                    "array_lengths": case.get("array_lengths") or {},
                    "group_counts": case.get("group_counts") or {},
                    "group_field_lengths": case.get("group_field_lengths") or {},
                    "sort_map": sort_map,
                }
                expect_compile_err = case.get("expect_compile_error_contains")
                if expect_compile_err:
                    with self.assertRaises((TypeError, ValueError)) as ctx:
                        compile_sql(twig, nulls, placeholder, **compile_kwargs)
                    self.assertIn(expect_compile_err, str(ctx.exception))
                    continue

                compiled = compile_sql(twig, nulls, placeholder, **compile_kwargs)
                self.assertEqual(
                    _normalize_ws(compiled["content"]),
                    _normalize_ws(case["expect_sql"]),
                )
                self.assertEqual(
                    [p["name"] for p in compiled["parameters"]],
                    case.get("expect_param_names") or [],
                )

    def test_experimental_bind_scalar_vs_single_element_array_same_placeholder_count(self):
        sql = (
            "--(pairs blob)--\n"
            "select * from t where optional_groups({{pairs}}, col in ({{cv}}))\n"
        )
        twig = parser(lexer(sql), "$")["sql_stmts"][0]
        pairs = [{"cv": 5}, {"cv": [4]}]
        shape = _Shape({"pairs": pairs})
        nulls = []
        counts, lengths, is_array = group_shapes_for_compile(twig, shape.get_prop, nulls)
        self.assertEqual(is_array["pairs"][0]["cv"], False)
        self.assertEqual(is_array["pairs"][1]["cv"], True)
        compiled = compile_sql(
            twig,
            nulls,
            "?",
            group_counts=counts,
            group_field_lengths=lengths,
            group_field_is_array=is_array,
        )
        helper = DataProviderHelper()
        values = helper.build_parameters(compiled, shape, lambda _t, v: v)
        self.assertEqual(values, [5, 4])

    def test_experimental_bind_coercion_types(self):
        sql = (
            "--(pairs blob)--\n"
            "select * from t where optional_groups({{pairs}}, a = {{a}} and b = {{b}} "
            "and c = {{c}} and d = {{d}})\n"
        )
        twig = parser(lexer(sql), "$")["sql_stmts"][0]
        pairs = [{"a": 1, "b": 2.5, "c": True, "d": "x"}]
        shape = _Shape({"pairs": pairs})
        nulls = []
        counts, lengths, is_array = group_shapes_for_compile(twig, shape.get_prop, nulls)
        compiled = compile_sql(
            twig,
            nulls,
            "?",
            group_counts=counts,
            group_field_lengths=lengths,
            group_field_is_array=is_array,
        )
        values = DataProviderHelper().build_parameters(
            compiled, shape, lambda _t, v: v
        )
        self.assertEqual(values, [1, 2.5, True, "x"])

    def test_experimental_shape_walk_missing_row_field(self):
        sql = (
            "--(pairs blob)--\n"
            "select * from t where optional_groups({{pairs}}, col1 = {{cv1}})\n"
        )
        twig = parser(lexer(sql), "$")["sql_stmts"][0]
        shape = _Shape({"pairs": [{}]})
        with self.assertRaises(ValueError) as ctx:
            group_shapes_for_compile(twig, shape.get_prop, [])
        self.assertIn("missing optional_groups field", str(ctx.exception))

    def test_experimental_compile_cache_key_differs_by_field_lengths(self):
        sql = (
            "--(pairs blob)--\n"
            "select * from t where optional_groups({{pairs}}, col in ({{cv}}))\n"
        )
        twig = parser(lexer(sql), "$")["sql_stmts"][0]
        helper = DataProviderHelper()
        shape_a = _Shape({"pairs": [{"cv": [1, 2]}]})
        shape_b = _Shape({"pairs": [{"cv": [1]}]})
        sql_a = helper.get_executable_content("?", twig, shape_a)["content"]
        sql_b = helper.get_executable_content("?", twig, shape_b)["content"]
        self.assertNotEqual(_normalize_ws(sql_a), _normalize_ws(sql_b))


if __name__ == "__main__":
    unittest.main()

# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Production hardening for blob/array params: shapes a caller can actually send."""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from yaal import Yaal
from yaal_builder import _json_type_for_param
from yaal_executor import DataProviderHelper
from yaal_parser import (
    array_lengths_for_compile,
    coerce_optional_groups_blob,
    compile_sql,
    group_shapes_for_compile,
    lexer,
    nullable_value_is_absent,
    parser,
)

ROOT = Path(__file__).resolve().parents[3]
FIXTURE_API = ROOT / "tests" / "fixtures" / "api"
SCHEMA = ROOT / "docker" / "sqlite" / "schema.sql"

GROUPS_SQL = (
    "--(pairs blob)--\n"
    "select * from t where optional_groups({{pairs}}, id = {{id}})\n"
)
GROUPS_IN_SQL = (
    "--(pairs blob)--\n"
    "select * from t where optional_groups({{pairs}}, id in ({{ids}}))\n"
)
TWO_GROUPS_AND_SQL = (
    "--(pairs blob, pairs1 blob)--\n"
    "select * from t where optional_groups({{pairs}}, id = {{id}})"
    " and optional_groups({{pairs1}}, id = {{id}})\n"
)


class _Shape:
    def __init__(self, props):
        self._props = props

    def get_prop(self, name):
        return self._props.get(name)


def _compile_and_bind(sql, pairs):
    twig = parser(lexer(sql), "$")["sql_stmts"][0]
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
    values = DataProviderHelper().build_parameters(compiled, shape, lambda _t, v: v)
    return compiled["content"].strip(), values


class TestGroupPrecedence(unittest.TestCase):
    """A group is one boolean unit: AND must not bind tighter than its OR rows."""

    def _compile(self, sql, props):
        twig = parser(lexer(sql), "$")["sql_stmts"][0]
        shape = _Shape(props)
        helper = DataProviderHelper()
        compiled = helper.get_executable_content("?", twig, shape)
        values = helper.build_parameters(compiled, shape, lambda _t, v: v)
        return compiled["content"].strip(), values

    def test_multi_row_group_is_parenthesized_beside_and(self):
        sql, values = self._compile(
            TWO_GROUPS_AND_SQL,
            {"pairs": [{"id": 1}, {"id": 3}], "pairs1": [{"id": 2}]},
        )
        self.assertEqual(
            sql, "select * from t where ((id = ?) or (id = ?)) and (id = ?)"
        )
        self.assertEqual(values, [1, 3, 2])

    def test_single_row_group_is_not_double_wrapped(self):
        sql, values = self._compile(
            TWO_GROUPS_AND_SQL,
            {"pairs": [{"id": 1}], "pairs1": [{"id": 2}]},
        )
        self.assertEqual(sql, "select * from t where (id = ?) and (id = ?)")
        self.assertEqual(values, [1, 2])

    def test_elided_group_leaves_other_group_intact(self):
        sql, values = self._compile(
            TWO_GROUPS_AND_SQL,
            {"pairs": [{"id": 1}, {"id": 3}], "pairs1": None},
        )
        self.assertEqual(sql, "select * from t where ((id = ?) or (id = ?))")
        self.assertEqual(values, [1, 3])


class TestBlobSourceShapes(unittest.TestCase):
    """A blob can arrive as a list, a JSON string, or JSON bytes."""

    def test_list_of_rows(self):
        sql, values = _compile_and_bind(GROUPS_SQL, [{"id": 1}, {"id": 2}])
        self.assertEqual(sql, "select * from t where ((id = ?) or (id = ?))")
        self.assertEqual(values, [1, 2])

    def test_json_string_compiles_and_binds(self):
        sql, values = _compile_and_bind(GROUPS_SQL, '[{"id": 1}, {"id": 2}]')
        self.assertEqual(sql, "select * from t where ((id = ?) or (id = ?))")
        self.assertEqual(values, [1, 2])

    def test_json_bytes_compiles_and_binds(self):
        sql, values = _compile_and_bind(GROUPS_SQL, b'[{"id": 7}]')
        self.assertEqual(sql, "select * from t where (id = ?)")
        self.assertEqual(values, [7])

    def test_json_string_with_in_list_per_row(self):
        sql, values = _compile_and_bind(
            GROUPS_IN_SQL, '[{"ids": [1, 2]}, {"ids": [3]}]'
        )
        self.assertEqual(
            sql, "select * from t where ((id in (?, ?)) or (id in (?)))"
        )
        self.assertEqual(values, [1, 2, 3])

    def test_json_object_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            _compile_and_bind(GROUPS_SQL, '{"id": 1}')
        self.assertIn("must be a JSON array of objects", str(ctx.exception))

    def test_malformed_json_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            _compile_and_bind(GROUPS_SQL, "not json")
        self.assertIn("must be a JSON array of objects", str(ctx.exception))

    def test_undecodable_bytes_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            _compile_and_bind(GROUPS_SQL, b"\xff\xfe")
        self.assertIn("must be a JSON array of objects", str(ctx.exception))

    def test_scalar_source_rejected(self):
        for value in (5, 1.5, True, {"id": 1}):
            with self.assertRaises(ValueError):
                coerce_optional_groups_blob(value, "pairs")

    def test_none_stays_none(self):
        self.assertIsNone(coerce_optional_groups_blob(None, "pairs"))


class TestBlobRowValues(unittest.TestCase):
    """Row values must be bindable scalars (or arrays of scalars for IN)."""

    def test_nested_object_value_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            _compile_and_bind(GROUPS_SQL, [{"id": {"nested": 1}}])
        self.assertIn("must be a scalar value", str(ctx.exception))

    def test_nested_object_inside_in_list_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            _compile_and_bind(GROUPS_IN_SQL, [{"ids": [1, {"nested": 2}]}])
        self.assertIn("must be a scalar value", str(ctx.exception))

    def test_error_names_source_and_field(self):
        with self.assertRaises(ValueError) as ctx:
            _compile_and_bind(GROUPS_SQL, [{"id": {"nested": 1}}])
        message = str(ctx.exception)
        self.assertIn("{{id}}", message)
        self.assertIn("pairs", message)
        self.assertIn("dict", message)

    def test_non_object_row_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            _compile_and_bind(GROUPS_SQL, [1, 2])
        self.assertIn("rows must be objects", str(ctx.exception))

    def test_null_row_value_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            _compile_and_bind(GROUPS_SQL, [{"id": None}])
        self.assertIn("missing optional_groups field", str(ctx.exception))

    def test_empty_in_list_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            _compile_and_bind(GROUPS_IN_SQL, [{"ids": []}])
        self.assertIn("IN list cannot be empty", str(ctx.exception))

    def test_bytes_row_value_binds_as_single_placeholder(self):
        sql, values = _compile_and_bind(GROUPS_SQL, [{"id": b"\x01\x02"}])
        self.assertEqual(sql, "select * from t where (id = ?)")
        self.assertEqual(values, [b"\x01\x02"])


class TestBlobAbsence(unittest.TestCase):
    """Every "no rows" spelling elides the clause the same way."""

    def test_nullable_value_is_absent_matrix(self):
        for value in (None, [], "", "  ", "[]", b"[]"):
            with self.subTest(value=value):
                self.assertTrue(nullable_value_is_absent("blob", value))
        for value in ([{"id": 1}], '[{"id": 1}]'):
            with self.subTest(value=value):
                self.assertFalse(nullable_value_is_absent("blob", value))

    def test_malformed_blob_is_not_treated_as_absent(self):
        self.assertFalse(nullable_value_is_absent("blob", "not json"))

    def test_zero_rows_elides_whole_clause(self):
        for pairs in ([], "", "[]"):
            with self.subTest(pairs=pairs):
                sql, values = _compile_and_bind(GROUPS_SQL, pairs)
                self.assertEqual(sql, "select * from t")
                self.assertEqual(values, [])

    def test_null_blob_elides_clause_through_helper(self):
        twig = parser(lexer(GROUPS_SQL), "$")["sql_stmts"][0]
        shape = _Shape({"pairs": None})
        helper = DataProviderHelper()
        compiled = helper.get_executable_content("?", twig, shape)
        self.assertEqual(compiled["content"].strip(), "select * from t")
        self.assertEqual(
            helper.build_parameters(compiled, shape, lambda _t, v: v), []
        )

    def test_missing_blob_value_errors(self):
        twig = parser(lexer(GROUPS_SQL), "$")["sql_stmts"][0]
        with self.assertRaises(ValueError) as ctx:
            group_shapes_for_compile(twig, _Shape({}).get_prop, [])
        self.assertIn("missing blob value", str(ctx.exception))


class TestArrayParamShapes(unittest.TestCase):
    """A string is not a list: `integer[]` must not expand per character."""

    def setUp(self):
        sql = "--(ids integer[])--\nselect * from t where id in ({{ids}})\n"
        self._twig = parser(lexer(sql), "$")["sql_stmts"][0]

    def _lengths(self, value):
        return array_lengths_for_compile(
            self._twig, _Shape({"ids": value}).get_prop, []
        )

    def test_list_and_tuple_accepted(self):
        self.assertEqual(self._lengths([1, 2, 3]), {"ids": 3})
        self.assertEqual(self._lengths((1, 2)), {"ids": 2})

    def test_string_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            self._lengths("abc")
        self.assertIn("must be a sequence, got str", str(ctx.exception))

    def test_mapping_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            self._lengths({"id": [1, 2]})
        self.assertIn("must be a sequence, got dict", str(ctx.exception))

    def test_none_is_skipped(self):
        self.assertEqual(self._lengths(None), {})


class TestBlobArgSchema(unittest.TestCase):
    """blob args model as a JSON array of objects so lists survive Shape casting."""

    def test_json_type_for_blob(self):
        self.assertEqual(
            _json_type_for_param({"type": "blob"}),
            {"type": "array", "items": {"type": "object"}},
        )

    def test_blob_arg_is_not_stringified_by_shape(self):
        from yaal import create_context

        yaal = Yaal(str(FIXTURE_API))
        descriptor = yaal.create_descriptor("user/groups")
        self.assertEqual(
            descriptor["model"]["args"]["properties"]["pairs"],
            {"type": "array", "items": {"type": "object"}},
        )
        context = create_context(descriptor, args={"pairs": [{"id": 1}]})
        self.assertEqual(context.get_prop("$args.pairs"), [{"id": 1}])


class TestOptionalGroupsEndToEnd(unittest.TestCase):
    """The user/groups fixture over real SQLite."""

    def setUp(self):
        fd, self._db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        sqlite3.connect(self._db_path).executescript(SCHEMA.read_text())
        self._yaal = Yaal(str(FIXTURE_API))
        self._yaal.setup_data_provider("db", "sqlite3:///" + self._db_path)

    def tearDown(self):
        try:
            os.unlink(self._db_path)
        except OSError:
            pass

    def test_single_row_filters(self):
        self.assertEqual(
            self._yaal.query("user/groups", args={"pairs": [{"id": 1}]}),
            [{"id": 1}],
        )

    def test_two_rows_or_joined(self):
        self.assertEqual(
            self._yaal.query("user/groups", args={"pairs": [{"id": 1}, {"id": 2}]}),
            [{"id": 1}, {"id": 2}],
        )

    def test_omitted_blob_elides_filter(self):
        self.assertEqual(
            self._yaal.query("user/groups"),
            [{"id": 1}, {"id": 2}],
        )

    def test_empty_blob_elides_filter(self):
        self.assertEqual(
            self._yaal.query("user/groups", args={"pairs": []}),
            [{"id": 1}, {"id": 2}],
        )

    def test_explain_shows_one_branch_per_row(self):
        explained = self._yaal.explain_sql(
            "user/groups", args={"pairs": [{"id": 1}, {"id": 2}]}
        )
        self.assertIn("((id = ?) or (id = ?))", explained[0]["sql"])
        self.assertEqual(explained[0]["parameters"], [1, 2])

    def test_row_count_change_recompiles(self):
        one = self._yaal.explain_sql("user/groups", args={"pairs": [{"id": 1}]})
        two = self._yaal.explain_sql(
            "user/groups", args={"pairs": [{"id": 1}, {"id": 2}]}
        )
        self.assertNotEqual(one[0]["sql"], two[0]["sql"])


if __name__ == "__main__":
    unittest.main()

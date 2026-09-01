# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Parameter header default values (= literal)."""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from yaal import Yaal, create_context
from yaal_parser import lexer, parser

ROOT = Path(__file__).resolve().parents[3]
FIXTURE_API = ROOT / "tests" / "fixtures" / "api"
SCHEMA = ROOT / "docker" / "sqlite" / "schema.sql"


class TestParamDefaultParse(unittest.TestCase):
    def test_string_bare_and_quoted(self):
        ast = parser(
            lexer("--($args.sort string = id, note string = 'a,b')--\nselect 1\n"),
            "$",
        )
        self.assertEqual(ast["parameters"]["$args.sort"]["default"], "id")
        self.assertEqual(ast["parameters"]["note"]["default"], "a,b")

    def test_integer_float_bool(self):
        ast = parser(
            lexer(
                "--(n integer = 1, f float = 1.5, b bool = false)--\nselect 1\n"
            ),
            "$",
        )
        self.assertEqual(ast["parameters"]["n"]["default"], 1)
        self.assertEqual(ast["parameters"]["f"]["default"], 1.5)
        self.assertEqual(ast["parameters"]["b"]["default"], False)

    def test_required_with_default_rejected(self):
        with self.assertRaises(TypeError) as ctx:
            parser(lexer("--(n! integer = 1)--\nselect 1\n"), "$")
        self.assertIn("cannot have a default", str(ctx.exception))

    def test_blob_default_rejected(self):
        with self.assertRaises(TypeError) as ctx:
            parser(lexer("--(b blob = x)--\nselect 1\n"), "$")
        self.assertIn("not supported for blob", str(ctx.exception))

    def test_invalid_integer_default(self):
        with self.assertRaises(TypeError):
            parser(lexer("--(n integer = x)--\nselect 1\n"), "$")


class TestParamDefaultSchema(unittest.TestCase):
    def test_derived_schema_includes_default(self):
        y = Yaal(str(FIXTURE_API), debug=True)
        desc = y.create_descriptor("user/list")
        sort_prop = desc["model"]["args"]["properties"]["sort"]
        self.assertEqual(sort_prop["default"], "id")
        self.assertEqual(desc["model"]["args"]["properties"]["dir"]["default"], "asc")

    def test_context_get_prop_uses_default(self):
        y = Yaal(str(FIXTURE_API), debug=True)
        desc = y.create_descriptor("user/list")
        ctx = create_context(desc, args={})
        self.assertEqual(ctx.get_prop("$args.sort"), "id")
        self.assertEqual(ctx.get_prop("$args.dir"), "asc")
        self.assertIsNone(ctx.get_prop("$args.active"))

    def test_explicit_arg_overrides_default(self):
        y = Yaal(str(FIXTURE_API), debug=True)
        desc = y.create_descriptor("user/list")
        ctx = create_context(desc, args={"sort": "name"})
        self.assertEqual(ctx.get_prop("$args.sort"), "name")


class TestParamDefaultRuntime(unittest.TestCase):
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

    def test_list_default_sort_id_asc(self):
        rows = self._yaal.query("user/list")
        self.assertEqual([r["id"] for r in rows], [1, 2])
        explained = self._yaal.explain_sql("user/list")
        sql = explained[0]["sql"]
        self.assertIn("u.user_id", sql)
        self.assertIn("ASC", sql)

    def test_optional_with_defaulted_arg_keeps_bind(self):
        # Header default on a filter param should keep optional predicate.
        sql = (
            "--($args.active integer = 1)--\n"
            "select u.user_id from users u\n"
            "where 1 = 1 and optional(u.active = {{$args.active}})\n"
        )
        # Build a tiny temp fixture is heavy; use explain via in-memory descriptor path.
        # Instead compile through parser + DataProviderHelper with Shape from schema.
        from yaal_executor import DataProviderHelper
        from yaal_shape import Shape

        twig = parser(lexer(sql), "$")["sql_stmts"][0]
        shape = Shape(
            schema={"type": "object", "properties": {}},
            extras={
                "$args": Shape(
                    schema={
                        "type": "object",
                        "properties": {
                            "active": {"type": "integer", "default": 1},
                        },
                    },
                    data={},
                )
            },
        )
        compiled = DataProviderHelper().get_executable_content("?", twig, shape)
        self.assertIn("?", compiled["content"])
        self.assertEqual([p["name"] for p in compiled["parameters"]], ["$args.active"])

# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Edge-case matrix for sort()/dir() dynamic ORDER BY."""

import os
import re
import sqlite3
import tempfile
import unittest
from pathlib import Path

from yaal import Yaal
from yaal_errors import SortDirError
from yaal_executor import DataProviderHelper
from yaal_parser import compile_sql, lexer, parser, resolve_sort_dir_values
from yaal_shape import Shape

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_API = ROOT / "tests" / "fixtures" / "api"
SCHEMA = ROOT / "docker" / "sqlite" / "schema.sql"

LIST_SQL = """--($args.sort string, $args.dir string, $args.active integer)--
select u.user_id, u.user_name from users u
where 1 = 1
  and optional(u.active = {{$args.active}})
order by
  sort({{$args.sort}}, name = u.user_name, id = u.user_id)
  dir({{$args.dir}})
"""


def _norm(sql):
    return re.sub(r"\s+", " ", sql).strip()


def _twig(sql=LIST_SQL):
    return parser(lexer(sql), "$")["sql_stmts"][0]


def _shape(sort=None, dir_=None, active=None):
    data = {}
    if sort is not None:
        data["sort"] = sort
    if dir_ is not None:
        data["dir"] = dir_
    if active is not None:
        data["active"] = active
    return Shape(
        schema={"type": "object", "properties": {}},
        extras={
            "$args": Shape(
                schema={
                    "type": "object",
                    "properties": {
                        "sort": {"type": "string"},
                        "dir": {"type": "string"},
                        "active": {"type": "integer"},
                    },
                },
                data=data,
            )
        },
    )


class TestSortDirParse(unittest.TestCase):
    def _err(self, sql_fragment, needle):
        sql = "--(a string)--\nselect 1 order by " + sql_fragment + "\n"
        with self.assertRaises(TypeError) as ctx:
            parser(lexer(sql), "$")
        self.assertIn(needle, str(ctx.exception).lower())

    def test_sort_empty(self):
        self._err("sort()", "empty")

    def test_sort_missing_param(self):
        self._err("sort(name = x)", "{{param}}")

    def test_sort_two_params(self):
        self._err("sort({{a}}, {{b}}, name = x)", "exactly one")

    def test_sort_zero_pairs(self):
        self._err("sort({{a}})", "at least one")

    def test_duplicate_keys(self):
        self._err("sort({{a}}, name = x, name = y)", "duplicate")

    def test_dir_empty(self):
        self._err("dir()", "empty")

    def test_dir_with_pairs(self):
        self._err("dir({{a}}, name = x)", "does not accept")

    def test_unclosed_sort(self):
        with self.assertRaises(TypeError) as ctx:
            parser(lexer("--(a string)--\nselect 1 order by sort({{a}}, name = x\n"), "$")
        self.assertIn("unclosed", str(ctx.exception).lower())

    def test_nested_sort(self):
        self._err("sort(sort({{a}}, name = x), id = y)", "nested")

    def test_key_illegal_chars(self):
        self._err("sort({{a}}, bad-key = x)", "word characters")

    def test_mixed_static_order_by_rejected(self):
        sql = (
            "--(s string)--\n"
            "select 1 order by sort({{s}}, a = x), y\n"
        )
        with self.assertRaises(TypeError) as ctx:
            parser(lexer(sql), "$")
        self.assertIn("must not include other terms", str(ctx.exception))

    def test_sort_outside_order_by_allowed(self):
        ast = parser(
            lexer("--(s string)--\nselect sort({{s}}, a = x) from t\n"),
            "$",
        )
        types = [t["type"] for t in ast["sql_stmts"][0]["content"]]
        self.assertIn("sort", types)


class TestSortDirResolve(unittest.TestCase):
    def test_sort_name(self):
        twig = _twig()
        sm, dm = resolve_sort_dir_values(twig, _shape(sort="name"))
        c = compile_sql(twig, ["$args.active"], "?", sort_map=sm, dir_map=dm)
        self.assertIn("u.user_name", c["content"])
        self.assertNotIn("?", c["content"].split("order by", 1)[-1])
        self.assertIn("ASC", c["content"])

    def test_sort_id(self):
        twig = _twig()
        sm, dm = resolve_sort_dir_values(twig, _shape(sort="id"))
        c = compile_sql(twig, ["$args.active"], "?", sort_map=sm, dir_map=dm)
        self.assertIn("u.user_id", _norm(c["content"]))

    def test_sort_case_insensitive(self):
        twig = _twig()
        sm, _ = resolve_sort_dir_values(twig, _shape(sort="NAME"))
        self.assertEqual(sm["$args.sort"], "u.user_name")

    def test_dir_desc_case(self):
        twig = _twig()
        _, dm = resolve_sort_dir_values(twig, _shape(sort="id", dir_="DESC"))
        self.assertEqual(dm["$args.dir"], "DESC")

    def test_dir_default_asc(self):
        twig = _twig()
        _, dm = resolve_sort_dir_values(twig, _shape(sort="id"))
        self.assertEqual(dm["$args.dir"], "ASC")

    def test_sort_and_dir(self):
        helper = DataProviderHelper()
        c = helper.get_executable_content("?", _twig(), _shape(sort="name", dir_="desc"))
        n = _norm(c["content"])
        self.assertIn("u.user_name", n)
        self.assertIn("DESC", n)

    def test_elide_order_by_when_sort_null(self):
        helper = DataProviderHelper()
        c = helper.get_executable_content("?", _twig(), _shape())
        self.assertNotIn("order by", c["content"].lower())

    def test_elide_even_if_dir_set(self):
        helper = DataProviderHelper()
        c = helper.get_executable_content("?", _twig(), _shape(dir_="desc"))
        self.assertNotIn("order by", c["content"].lower())

    def test_sort_present_dir_null_defaults_asc(self):
        helper = DataProviderHelper()
        c = helper.get_executable_content("?", _twig(), _shape(sort="id"))
        self.assertIn("ASC", c["content"])

    def test_optional_and_sort_both_null(self):
        helper = DataProviderHelper()
        c = helper.get_executable_content("?", _twig(), _shape())
        n = _norm(c["content"])
        self.assertNotIn("where", n.lower())
        self.assertNotIn("order by", n.lower())

    def test_unknown_key_soft_error(self):
        with self.assertRaises(SortDirError) as ctx:
            resolve_sort_dir_values(_twig(), _shape(sort="nope"))
        self.assertIn("unknown sort key", ctx.exception.message)

    def test_injection_key_soft_error(self):
        with self.assertRaises(SortDirError):
            resolve_sort_dir_values(_twig(), _shape(sort="id; drop table"))
        helper = DataProviderHelper()
        with self.assertRaises(SortDirError):
            helper.get_executable_content("?", _twig(), _shape(sort="id; drop table"))

    def test_raw_column_as_key_soft_error(self):
        with self.assertRaises(SortDirError):
            resolve_sort_dir_values(_twig(), _shape(sort="u.user_name"))

    def test_empty_string_soft_error(self):
        with self.assertRaises(SortDirError):
            resolve_sort_dir_values(_twig(), _shape(sort=""))

    def test_whitespace_sort_soft_error(self):
        with self.assertRaises(SortDirError):
            resolve_sort_dir_values(_twig(), _shape(sort="   "))

    def test_bad_dir_soft_error(self):
        for bad in ("ascending", "1", "desc;"):
            with self.assertRaises(SortDirError):
                resolve_sort_dir_values(_twig(), _shape(sort="id", dir_=bad))

    def test_multi_token_expr(self):
        sql = (
            "--(s string, d string)--\n"
            "select 1 order by\n"
            "  sort({{s}}, name = lower(u.user_name))\n"
            "  dir({{d}})\n"
        )
        twig = _twig(sql)

        class Bag:
            def __init__(self, d):
                self.d = d

            def get_prop(self, n):
                return self.d.get(n)

        sm, dm = resolve_sort_dir_values(twig, Bag({"s": "name", "d": "asc"}))
        c = compile_sql(twig, [], "?", sort_map=sm, dir_map=dm)
        self.assertIn("lower(u.user_name)", c["content"])

    def test_security_quote_comment_key(self):
        for bad in ("x'--", 'x";--', "name\x00", "a" * 500, "nаme"):  # cyrillic a
            with self.assertRaises(SortDirError):
                resolve_sort_dir_values(_twig(), _shape(sort=bad))


class TestSortDirCache(unittest.TestCase):
    def test_different_sort_keys_separate_cache(self):
        helper = DataProviderHelper()
        twig = _twig()
        a = helper.get_executable_content("?", twig, _shape(sort="name"))
        b = helper.get_executable_content("?", twig, _shape(sort="id"))
        self.assertNotEqual(a["content"], b["content"])
        self.assertEqual(len(helper._compile_cache), 2)

    def test_same_sort_dir_cache_hit(self):
        helper = DataProviderHelper()
        twig = _twig()
        helper.get_executable_content("?", twig, _shape(sort="name", dir_="desc"))
        helper.get_executable_content("?", twig, _shape(sort="name", dir_="desc"))
        self.assertEqual(len(helper._compile_cache), 1)

    def test_optional_null_set_independent(self):
        helper = DataProviderHelper()
        twig = _twig()
        helper.get_executable_content("?", twig, _shape(sort="id"))
        helper.get_executable_content("?", twig, _shape(sort="id", active=1))
        self.assertEqual(len(helper._compile_cache), 2)


class TestSortDirIntegration(unittest.TestCase):
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

    def test_list_sort_name(self):
        rows = self._yaal.query("user/list", args={"sort": "name"})
        self.assertEqual([r["name"] for r in rows], ["admin", "guest"])

    def test_list_sort_id_desc(self):
        rows = self._yaal.query("user/list", args={"sort": "id", "dir": "desc"})
        self.assertEqual([r["id"] for r in rows], [2, 1])

    def test_unknown_sort_soft_errors(self):
        result = self._yaal.query("user/list", args={"sort": "nope"})
        self.assertIn("errors", result)
        self.assertTrue(any("unknown sort key" in e.get("message", "") for e in result["errors"]))

    def test_explain_shows_resolved_order_by(self):
        explained = self._yaal.explain_sql(
            "user/list", args={"sort": "name", "dir": "desc"}
        )
        sql = explained[0]["sql"]
        self.assertIn("u.user_name", sql)
        self.assertIn("DESC", sql)
        self.assertNotIn("sort(", sql.lower())

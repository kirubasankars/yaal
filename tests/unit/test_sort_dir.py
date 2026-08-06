# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Edge-case matrix for sort()/dir() dynamic ORDER BY (multi-column, NULLS
FIRST/LAST, and mixing with a static tiebreaker)."""

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

# Same as LIST_SQL, plus a static tiebreaker after the dynamic term.
LIST_SQL_TIEBREAKER = """--($args.sort string, $args.dir string, $args.active integer)--
select u.user_id, u.user_name from users u
where 1 = 1
  and optional(u.active = {{$args.active}})
order by
  sort({{$args.sort}}, name = u.user_name, id = u.user_id)
  dir({{$args.dir}}),
  u.user_id asc
"""

# Same choices, static tiebreaker leads instead of trailing.
LIST_SQL_LEADING_STATIC = """--($args.sort string, $args.dir string)--
select u.user_id, u.user_name from users u
order by
  u.is_pinned desc,
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

    def test_static_tiebreaker_after_allowed(self):
        sql = (
            "--(s string)--\n"
            "select 1 order by sort({{s}}, a = x), y\n"
        )
        ast = parser(lexer(sql), "$")
        self.assertIsNotNone(ast)

    def test_static_tiebreaker_before_allowed(self):
        sql = (
            "--(s string)--\n"
            "select 1 order by y, sort({{s}}, a = x)\n"
        )
        ast = parser(lexer(sql), "$")
        self.assertIsNotNone(ast)

    def test_two_dynamic_terms_rejected(self):
        sql = (
            "--(a string, b string)--\n"
            "select 1 order by sort({{a}}, x = y), sort({{b}}, p = q)\n"
        )
        with self.assertRaises(TypeError) as ctx:
            parser(lexer(sql), "$")
        self.assertIn("only one dynamic sort()/dir() term", str(ctx.exception))

    def test_dir_split_from_sort_by_comma_rejected(self):
        sql = (
            "--(a string, b string)--\n"
            "select 1 order by sort({{a}}, x = y), foo, dir({{b}})\n"
        )
        with self.assertRaises(TypeError) as ctx:
            parser(lexer(sql), "$")
        self.assertIn("only one dynamic sort()/dir() term", str(ctx.exception))

    def test_dir_not_immediately_after_sort_same_term_rejected(self):
        sql = (
            "--(a string, b string)--\n"
            "select 1 order by sort({{a}}, x = y) foo dir({{b}})\n"
        )
        with self.assertRaises(TypeError) as ctx:
            parser(lexer(sql), "$")
        self.assertIn("must not include other terms", str(ctx.exception))

    def test_dynamic_term_mixed_with_static_no_comma_rejected(self):
        sql = (
            "--(s string)--\n"
            "select 1 order by sort({{s}}, a = x) foo\n"
        )
        with self.assertRaises(TypeError) as ctx:
            parser(lexer(sql), "$")
        self.assertIn("must not include other terms", str(ctx.exception))

    def test_empty_order_by_term_rejected(self):
        sql = (
            "--(s string)--\n"
            "select 1 order by sort({{s}}, a = x), ,\n"
        )
        with self.assertRaises(TypeError) as ctx:
            parser(lexer(sql), "$")
        self.assertIn("empty order by term", str(ctx.exception).lower())

    def test_static_term_with_internal_paren_comma_allowed(self):
        sql = (
            "--(s string)--\n"
            "select 1 order by coalesce(a, b) desc, sort({{s}}, x = y)\n"
        )
        ast = parser(lexer(sql), "$")
        self.assertIsNotNone(ast)

    def test_sort_outside_order_by_allowed(self):
        ast = parser(
            lexer("--(s string)--\nselect sort({{s}}, a = x) from t\n"),
            "$",
        )
        types = [t["type"] for t in ast["sql_stmts"][0]["content"]]
        self.assertIn("sort", types)

    # -- multiple sort()/dir() pairs in one statement --------------------------

    def test_multiple_sort_dir_pairs_distinct_params_allowed(self):
        sql = (
            "--(s1 string, d1 string, s2 string, d2 string)--\n"
            "select * from (\n"
            "  select * from t1 order by sort({{s1}}, a = x, b = y) dir({{d1}})\n"
            ") sub\n"
            "order by sort({{s2}}, c = z, d = w) dir({{d2}})\n"
        )
        ast = parser(lexer(sql), "$")
        self.assertIsNotNone(ast)

    def test_reused_param_across_two_sort_calls_rejected(self):
        sql = (
            "--(s string, d string)--\n"
            "select * from (\n"
            "  select * from t1 order by sort({{s}}, a = x, b = y) dir({{d}})\n"
            ") sub\n"
            "order by sort({{s}}, a = p, b = q) dir({{d}})\n"
        )
        with self.assertRaises(TypeError) as ctx:
            parser(lexer(sql), "$")
        self.assertIn("more than one sort(...)", str(ctx.exception))


class TestSortDirResolve(unittest.TestCase):
    def test_sort_name(self):
        twig = _twig()
        sm = resolve_sort_dir_values(twig, _shape(sort="name"))
        c = compile_sql(twig, ["$args.active"], "?", sort_map=sm)
        self.assertIn("u.user_name", c["content"])
        self.assertNotIn("?", c["content"].split("order by", 1)[-1])
        self.assertIn("ASC", c["content"])

    def test_sort_id(self):
        twig = _twig()
        sm = resolve_sort_dir_values(twig, _shape(sort="id"))
        c = compile_sql(twig, ["$args.active"], "?", sort_map=sm)
        self.assertIn("u.user_id", _norm(c["content"]))

    def test_sort_case_insensitive(self):
        twig = _twig()
        sm = resolve_sort_dir_values(twig, _shape(sort="NAME"))
        self.assertEqual(sm["$args.sort"], "u.user_name ASC")

    def test_dir_desc_case(self):
        twig = _twig()
        sm = resolve_sort_dir_values(twig, _shape(sort="id", dir_="DESC"))
        self.assertEqual(sm["$args.sort"], "u.user_id DESC")

    def test_dir_default_asc(self):
        twig = _twig()
        sm = resolve_sort_dir_values(twig, _shape(sort="id"))
        self.assertEqual(sm["$args.sort"], "u.user_id ASC")

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

        sm = resolve_sort_dir_values(twig, Bag({"s": "name", "d": "asc"}))
        c = compile_sql(twig, [], "?", sort_map=sm)
        self.assertIn("lower(u.user_name)", c["content"])

    def test_security_quote_comment_key(self):
        for bad in ("x'--", 'x";--', "name\x00", "a" * 500, "nаme"):  # cyrillic a
            with self.assertRaises(SortDirError):
                resolve_sort_dir_values(_twig(), _shape(sort=bad))

    # -- multi-column sort --------------------------------------------------

    def test_multi_column_resolve(self):
        twig = _twig()
        sm = resolve_sort_dir_values(twig, _shape(sort="name,id", dir_="desc,asc"))
        self.assertEqual(sm["$args.sort"], "u.user_name DESC, u.user_id ASC")

    def test_multi_column_dir_shorter_pads_asc(self):
        twig = _twig()
        sm = resolve_sort_dir_values(twig, _shape(sort="name,id", dir_="desc"))
        self.assertEqual(sm["$args.sort"], "u.user_name DESC, u.user_id ASC")

    def test_multi_column_dir_missing_defaults_all_asc(self):
        twig = _twig()
        sm = resolve_sort_dir_values(twig, _shape(sort="name,id"))
        self.assertEqual(sm["$args.sort"], "u.user_name ASC, u.user_id ASC")

    def test_multi_column_dir_longer_soft_error(self):
        with self.assertRaises(SortDirError) as ctx:
            resolve_sort_dir_values(_twig(), _shape(sort="name", dir_="desc,asc"))
        self.assertIn("too many dir values", ctx.exception.message)

    def test_multi_column_duplicate_key_soft_error(self):
        with self.assertRaises(SortDirError) as ctx:
            resolve_sort_dir_values(_twig(), _shape(sort="name,name"))
        self.assertIn("duplicate sort key", ctx.exception.message)

    def test_multi_column_unknown_key_among_valid_soft_error(self):
        with self.assertRaises(SortDirError) as ctx:
            resolve_sort_dir_values(_twig(), _shape(sort="name,nope"))
        self.assertIn("unknown sort key", ctx.exception.message)

    def test_multi_column_whitespace_around_commas(self):
        twig = _twig()
        sm = resolve_sort_dir_values(twig, _shape(sort=" name , id ", dir_=" desc , asc "))
        self.assertEqual(sm["$args.sort"], "u.user_name DESC, u.user_id ASC")

    def test_multi_column_compiles_comma_joined(self):
        twig = _twig()
        sm = resolve_sort_dir_values(twig, _shape(sort="name,id", dir_="desc,asc"))
        c = compile_sql(twig, ["$args.active"], "?", sort_map=sm)
        self.assertIn("order by\n  u.user_name DESC, u.user_id ASC", c["content"])

    # -- NULLS FIRST/LAST -----------------------------------------------------

    def test_nulls_vocabulary_resolves(self):
        cases = {
            "asc": "ASC",
            "desc": "DESC",
            "asc_nulls_first": "ASC NULLS FIRST",
            "asc_nulls_last": "ASC NULLS LAST",
            "desc_nulls_first": "DESC NULLS FIRST",
            "desc_nulls_last": "DESC NULLS LAST",
        }
        for raw, sql_dir in cases.items():
            sm = resolve_sort_dir_values(_twig(), _shape(sort="id", dir_=raw))
            self.assertEqual(sm["$args.sort"], "u.user_id " + sql_dir)

    def test_nulls_vocabulary_case_insensitive(self):
        sm = resolve_sort_dir_values(_twig(), _shape(sort="id", dir_="DESC_NULLS_LAST"))
        self.assertEqual(sm["$args.sort"], "u.user_id DESC NULLS LAST")

    def test_nulls_vocabulary_unknown_value_soft_error(self):
        with self.assertRaises(SortDirError):
            resolve_sort_dir_values(_twig(), _shape(sort="id", dir_="desc_nulls_middle"))

    def test_nulls_mixed_in_multi_column_list(self):
        twig = _twig()
        sm = resolve_sort_dir_values(twig, _shape(sort="name,id", dir_="desc,asc_nulls_last"))
        self.assertEqual(sm["$args.sort"], "u.user_name DESC, u.user_id ASC NULLS LAST")

    # -- static tiebreaker mixing --------------------------------------------

    def test_trailing_static_tiebreaker_kept_when_sort_present(self):
        twig = _twig(LIST_SQL_TIEBREAKER)
        sm = resolve_sort_dir_values(twig, _shape(sort="name", dir_="desc"))
        c = compile_sql(twig, ["$args.active"], "?", sort_map=sm)
        n = _norm(c["content"])
        self.assertIn("order by u.user_name DESC, u.user_id asc", n)

    def test_trailing_static_tiebreaker_kept_when_sort_null(self):
        twig = _twig(LIST_SQL_TIEBREAKER)
        sm = resolve_sort_dir_values(twig, _shape())
        c = compile_sql(twig, ["$args.active"], "?", sort_map=sm)
        n = _norm(c["content"])
        self.assertIn("order by u.user_id asc", n)
        self.assertNotIn("ASC,", n)

    def test_leading_static_term_kept_with_sort(self):
        twig = _twig(LIST_SQL_LEADING_STATIC)
        sm = resolve_sort_dir_values(twig, _shape(sort="name", dir_="desc"))
        c = compile_sql(twig, [], "?", sort_map=sm)
        n = _norm(c["content"])
        self.assertIn("order by u.is_pinned desc, u.user_name DESC", n)

    def test_leading_static_term_kept_when_sort_null(self):
        twig = _twig(LIST_SQL_LEADING_STATIC)
        sm = resolve_sort_dir_values(twig, _shape())
        c = compile_sql(twig, [], "?", sort_map=sm)
        n = _norm(c["content"])
        self.assertIn("order by u.is_pinned desc", n)
        self.assertNotIn(",", n.split("order by", 1)[-1])

    def test_whole_clause_elides_when_only_dynamic_term_and_sort_null(self):
        # LIST_SQL (no tiebreaker) has only the dynamic term.
        helper = DataProviderHelper()
        c = helper.get_executable_content("?", _twig(LIST_SQL), _shape())
        self.assertNotIn("order by", c["content"].lower())

    # -- multiple sort()/dir() pairs in one statement --------------------------

    def test_multiple_sort_dir_pairs_resolve_independently(self):
        sql = (
            "--(s1 string, d1 string, s2 string, d2 string)--\n"
            "select * from (\n"
            "  select * from t1 order by sort({{s1}}, a = x, b = y) dir({{d1}})\n"
            ") sub\n"
            "order by sort({{s2}}, c = z, d = w) dir({{d2}})\n"
        )
        twig = _twig(sql)

        class Bag:
            def __init__(self, d):
                self.d = d

            def get_prop(self, n):
                return self.d.get(n)

        sm = resolve_sort_dir_values(
            twig, Bag({"s1": "a", "d1": "desc", "s2": "d", "d2": "asc"})
        )
        self.assertEqual(sm["s1"], "x DESC")
        self.assertEqual(sm["s2"], "w ASC")
        c = compile_sql(twig, [], "?", sort_map=sm)
        n = _norm(c["content"])
        self.assertIn("order by x DESC", n)
        self.assertIn("order by w ASC", n)

    def test_multiple_sort_dir_pairs_one_null_one_set(self):
        sql = (
            "--(s1 string, d1 string, s2 string, d2 string)--\n"
            "select * from (\n"
            "  select * from t1 order by sort({{s1}}, a = x, b = y) dir({{d1}})\n"
            ") sub\n"
            "order by sort({{s2}}, c = z, d = w) dir({{d2}})\n"
        )
        twig = _twig(sql)

        class Bag:
            def __init__(self, d):
                self.d = d

            def get_prop(self, n):
                return self.d.get(n)

        sm = resolve_sort_dir_values(twig, Bag({"s2": "c"}))
        self.assertIsNone(sm["s1"])
        self.assertEqual(sm["s2"], "z ASC")
        c = compile_sql(twig, [], "?", sort_map=sm)
        n = _norm(c["content"])
        # The subquery's ORDER BY (only the dynamic term) elides entirely.
        self.assertNotIn("order by x", n)
        self.assertNotIn("order by y", n)
        self.assertIn("order by z ASC", n)


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

    def test_different_multi_column_combos_separate_cache(self):
        helper = DataProviderHelper()
        twig = _twig()
        a = helper.get_executable_content("?", twig, _shape(sort="name,id", dir_="desc,asc"))
        b = helper.get_executable_content("?", twig, _shape(sort="name,id", dir_="asc,desc"))
        self.assertNotEqual(a["content"], b["content"])
        self.assertEqual(len(helper._compile_cache), 2)

    def test_only_dir_change_busts_cache(self):
        # Direction is folded into the sort_map string; changing only dir must
        # still produce a distinct cache key (regression guard for dropping dir_map).
        helper = DataProviderHelper()
        twig = _twig()
        helper.get_executable_content("?", twig, _shape(sort="id", dir_="asc"))
        helper.get_executable_content("?", twig, _shape(sort="id", dir_="desc"))
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

    def test_list_multi_column_sort(self):
        rows = self._yaal.query("user/list", args={"sort": "name,id", "dir": "desc,asc"})
        self.assertEqual([r["name"] for r in rows], ["guest", "admin"])

    def test_list_nulls_last_dir(self):
        # Only exercises that the *_nulls_last vocabulary round-trips through a
        # real SQLite query without erroring (SQLite supports NULLS LAST).
        rows = self._yaal.query("user/list", args={"sort": "name", "dir": "desc_nulls_last"})
        self.assertEqual(len(rows), 2)

    def test_unknown_sort_soft_errors(self):
        result = self._yaal.query("user/list", args={"sort": "nope"})
        self.assertIn("errors", result)
        self.assertTrue(any("unknown sort key" in e.get("message", "") for e in result["errors"]))

    def test_too_many_dir_values_soft_errors(self):
        result = self._yaal.query("user/list", args={"sort": "name", "dir": "desc,asc"})
        self.assertIn("errors", result)
        self.assertTrue(any("too many dir values" in e.get("message", "") for e in result["errors"]))

    def test_explain_shows_resolved_order_by(self):
        explained = self._yaal.explain_sql(
            "user/list", args={"sort": "name", "dir": "desc"}
        )
        sql = explained[0]["sql"]
        self.assertIn("u.user_name", sql)
        self.assertIn("DESC", sql)
        self.assertNotIn("sort(", sql.lower())

    def test_explain_keeps_static_tiebreaker_when_sort_omitted(self):
        explained = self._yaal.explain_sql("user/list")
        sql = explained[0]["sql"]
        self.assertIn("order by", sql.lower())
        self.assertIn("u.user_id", sql)

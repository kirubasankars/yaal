import re
import unittest

from yaal_parser import lexer, parser, compile_sql


def _compile(sql, nulls, placeholder="?"):
    ast = parser(lexer(sql), "$")
    twig = ast["sql_stmts"][0]
    return compile_sql(twig, nulls, placeholder)


def _normalize_ws(sql):
    return re.sub(r"\s+", " ", sql).strip()


class TestNullableFilters(unittest.TestCase):

    def test_null_strips_or_after_one_equals_one(self):
        sql = """--(param1 integer)--
select * from a where 1 = 1 or ({{param1}} is null or col1 = {{param1}})
"""
        compiled = _compile(sql, ["param1"])
        self.assertEqual(_normalize_ws(compiled["content"]), "select * from a where 1 = 1")
        self.assertEqual(compiled["parameters"], [])

    def test_null_strips_and_after_predicate(self):
        sql = """--(p integer)--
select * from a where a = 1 and ({{p}} is null or col = {{p}})
"""
        compiled = _compile(sql, ["p"])
        self.assertEqual(_normalize_ws(compiled["content"]), "select * from a where a = 1")
        self.assertEqual(compiled["parameters"], [])

    def test_sole_nullable_predicate_falls_back_to_one_equals_one(self):
        sql = """--(p integer)--
select * from a where ({{p}} is null or col = {{p}})
"""
        compiled = _compile(sql, ["p"])
        self.assertEqual(_normalize_ws(compiled["content"]), "select * from a where 1 = 1")
        self.assertEqual(compiled["parameters"], [])

    def test_non_null_keeps_binds_and_rewrites_is_null(self):
        sql = """--(param1 integer)--
select * from a where 1 = 1 or ({{param1}} is null or col1 = {{param1}})
"""
        compiled = _compile(sql, [])
        self.assertEqual(
            _normalize_ws(compiled["content"]),
            "select * from a where 1 = 1 or (1 = 2 or col1 = ?)",
        )
        self.assertEqual(len(compiled["parameters"]), 1)
        self.assertEqual(compiled["parameters"][0]["name"], "param1")

    def test_elided_params_omitted_from_bind_list(self):
        sql = """--(a integer, b integer)--
select * from t where col = {{a}} and ({{b}} is null or other = {{b}})
"""
        compiled = _compile(sql, ["b"])
        self.assertEqual(_normalize_ws(compiled["content"]), "select * from t where col = ?")
        self.assertEqual([p["name"] for p in compiled["parameters"]], ["a"])

    def test_nullable_name_match_is_case_insensitive(self):
        sql = """--(Param1 integer)--
select * from a where 1 = 1 OR ({{Param1}} is null or col1 = {{Param1}})
"""
        ast = parser(lexer(sql), "$")
        twig = ast["sql_stmts"][0]
        self.assertIn("param1", twig["nullable"])
        compiled = compile_sql(twig, ["PARAM1"], "?")
        self.assertEqual(_normalize_ws(compiled["content"]), "select * from a where 1 = 1")
        self.assertEqual(compiled["parameters"], [])

    def test_path_id_style_filter_like_fixture(self):
        sql = """--($path.id integer)--
select * from users u
where u.active = 1
  and r.active = 1
  and ({{$path.id}} is null or u.user_id = {{$path.id}})
order by u.user_id
"""
        compiled = _compile(sql, ["$path.id"])
        self.assertEqual(
            _normalize_ws(compiled["content"]),
            "select * from users u where u.active = 1 and r.active = 1 order by u.user_id",
        )
        self.assertEqual(compiled["parameters"], [])

    def test_optional_sugar_null_strips_and(self):
        sql = """--(p integer)--
select * from a where a = 1 and optional(col = {{p}})
"""
        compiled = _compile(sql, ["p"])
        self.assertEqual(_normalize_ws(compiled["content"]), "select * from a where a = 1")
        self.assertEqual(compiled["parameters"], [])

    def test_optional_sugar_non_null_keeps_binds(self):
        sql = """--(param1 integer)--
select * from a where 1 = 1 or optional(col1 = {{param1}})
"""
        compiled = _compile(sql, [])
        self.assertEqual(
            _normalize_ws(compiled["content"]),
            "select * from a where 1 = 1 or (1 = 2 or col1 = ?)",
        )
        self.assertEqual(len(compiled["parameters"]), 1)
        self.assertEqual(compiled["parameters"][0]["name"], "param1")

    def test_optional_sugar_sole_predicate_falls_back(self):
        sql = """--(p integer)--
select * from a where optional(col = {{p}})
"""
        compiled = _compile(sql, ["p"])
        self.assertEqual(_normalize_ws(compiled["content"]), "select * from a where 1 = 1")
        self.assertEqual(compiled["parameters"], [])

    def test_optional_sugar_path_id_fixture_style(self):
        sql = """--($path.id integer)--
select * from users u
where u.active = 1
  and r.active = 1
  and optional(u.user_id = {{$path.id}})
order by u.user_id
"""
        compiled = _compile(sql, ["$path.id"])
        self.assertEqual(
            _normalize_ws(compiled["content"]),
            "select * from users u where u.active = 1 and r.active = 1 order by u.user_id",
        )
        self.assertEqual(compiled["parameters"], [])

    def test_optional_sugar_requires_one_param(self):
        sql = """--(a integer)--
select * from a where optional(col = 1)
"""
        with self.assertRaises(TypeError) as ctx:
            parser(lexer(sql), "$")
        self.assertIn("exactly one", str(ctx.exception))

    def test_optional_sugar_rejects_multiple_params(self):
        sql = """--(a integer, b integer)--
select * from a where optional(col = {{a}} and other = {{b}})
"""
        with self.assertRaises(TypeError) as ctx:
            parser(lexer(sql), "$")
        self.assertIn("exactly one", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

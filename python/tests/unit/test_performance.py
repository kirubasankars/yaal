# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Performance-oriented parse/compile behavior (compact twigs, has_sort_dir)."""

import copy
import unittest

from yaal_executor import DataProviderHelper
from yaal_parser import compact_twig_tokens, compile_sql, lexer, parser, resolve_sort_dir_values
from yaal_shape import Shape


LIST_SQL = """--($args.sort string, $args.dir string, $args.active integer)--
select u.user_id, u.user_name from users u
where 1 = 1
  and optional(u.active = {{$args.active}})
order by
  sort({{$args.sort}}, name = u.user_name, id = u.user_id)
  dir({{$args.dir}})
"""


class TestCompactTwigs(unittest.TestCase):

    def test_compact_reduces_whitespace_tokens(self):
        raw = lexer(LIST_SQL)
        self.assertGreater(len(raw), 20)
        twig = parser(raw, "$")["sql_stmts"][0]
        compacted_len = len(twig["content"])
        self.assertLess(compacted_len, len(raw))

    def test_compact_compile_matches_uncompacted(self):
        ast = parser(lexer(LIST_SQL), "$")
        twig = ast["sql_stmts"][0]
        compacted = compact_twig_tokens(copy.deepcopy(twig["content"]))
        twig_compact = dict(twig)
        twig_compact["content"] = compacted

        class Bag:
            def __init__(self, **kw):
                self._d = kw
            def get_prop(self, k):
                return self._d.get(k)

        shape = Bag(sort="name", dir_="desc", active=1)
        sm = resolve_sort_dir_values(twig, shape)
        c1 = compile_sql(twig, ["$args.active"], "?", sort_map=sm)
        c2 = compile_sql(twig_compact, ["$args.active"], "?", sort_map=sm)
        self.assertEqual(c1["content"], c2["content"])
        self.assertEqual(
            [p["name"] for p in c1["parameters"]],
            [p["name"] for p in c2["parameters"]],
        )

    def test_has_sort_dir_false_skips_resolve(self):
        twig = parser(lexer("select 1 from users"), "$")["sql_stmts"][0]
        self.assertFalse(twig.get("has_sort_dir"))
        helper = DataProviderHelper()
        c = helper.get_executable_content("?", twig, None)
        self.assertIn("select", c["content"].lower())

    def test_has_sort_dir_true_on_sort_twig(self):
        twig = parser(lexer(LIST_SQL), "$")["sql_stmts"][0]
        self.assertTrue(twig.get("has_sort_dir"))


class TestShapeSchemaShare(unittest.TestCase):

    def test_array_items_share_item_schema(self):
        schema = {
            "type": "array",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
            },
        }
        original = copy.deepcopy(schema)
        data = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        parent = Shape(schema=schema, data=data)
        child0 = parent.get_prop("@0")
        child1 = parent.get_prop("@1")
        self.assertIs(child0._input_properties, child1._input_properties)
        self.assertEqual(schema, original)


if __name__ == "__main__":
    unittest.main()

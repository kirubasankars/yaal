# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import unittest

from yaal_executor import DataProviderHelper
from yaal_parser import lexer, parser
from yaal_provider import parse_pool_int
from yaal_shape import Shape


class TestCompileCache(unittest.TestCase):

    def _twig(self):
        sql = (
            "--($args.active integer)--\n"
            "select 1 from users where 1 = 1 "
            "and optional(u.active = {{$args.active}})"
        )
        ast = parser(lexer(sql), "$")
        return ast["sql_stmts"][0]

    def test_same_nulls_reuse_compiled_sql(self):
        helper = DataProviderHelper()
        twig = self._twig()
        shape = Shape(
            schema={"type": "object", "properties": {}},
            extras={
                "$args": Shape(
                    schema={
                        "type": "object",
                        "properties": {"active": {"type": "integer"}},
                    },
                    data={},
                )
            },
        )
        a = helper.get_executable_content("?", twig, shape)
        b = helper.get_executable_content("?", twig, shape)
        self.assertEqual(a["content"], b["content"])
        self.assertEqual(a["parameters"], b["parameters"])
        self.assertEqual(len(helper._compile_cache), 1)

    def test_different_nulls_different_sql(self):
        helper = DataProviderHelper()
        twig = self._twig()
        null_shape = Shape(
            schema={"type": "object", "properties": {}},
            extras={
                "$args": Shape(
                    schema={
                        "type": "object",
                        "properties": {"active": {"type": "integer"}},
                    },
                    data={},
                )
            },
        )
        present_shape = Shape(
            schema={"type": "object", "properties": {}},
            extras={
                "$args": Shape(
                    schema={
                        "type": "object",
                        "properties": {"active": {"type": "integer"}},
                    },
                    data={"active": 1},
                )
            },
        )
        elided = helper.get_executable_content("?", twig, null_shape)
        bound = helper.get_executable_content("?", twig, present_shape)
        self.assertNotEqual(elided["content"], bound["content"])
        self.assertEqual(len(helper._compile_cache), 2)

    def test_clear_cache_keeps_compile_cache(self):
        helper = DataProviderHelper()
        twig = self._twig()
        shape = Shape(
            schema={"type": "object", "properties": {}},
            extras={
                "$args": Shape(
                    schema={
                        "type": "object",
                        "properties": {"active": {"type": "integer"}},
                    },
                    data={},
                )
            },
        )
        helper.get_executable_content("?", twig, shape)
        helper.clear_cache()
        self.assertEqual(len(helper._compile_cache), 1)


class TestPoolUrlParsing(unittest.TestCase):

    def test_parse_pool_int_defaults_and_caps(self):
        self.assertEqual(parse_pool_int({}, "pool_size", 10), 10)
        self.assertEqual(parse_pool_int({"pool_size": "5"}, "pool_size", 10), 5)
        self.assertEqual(parse_pool_int({"pool_size": "0"}, "pool_size", 10), 1)
        self.assertEqual(parse_pool_int({"pool_size": "99"}, "pool_size", 10), 32)
        self.assertEqual(parse_pool_int({"pool_size": "x"}, "pool_size", 10), 10)


if __name__ == "__main__":
    unittest.main()

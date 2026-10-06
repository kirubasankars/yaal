# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import unittest

from yaal_executor import DataProviderHelper
from yaal_parser import lexer, parser


class _PropBag:
    def __init__(self, values):
        self._values = values

    def get_prop(self, name):
        return self._values.get(name)


class TestArrayParams(unittest.TestCase):

    def test_build_parameters_expands_array_in_order(self):
        sql = """--(a integer, cv integer[], b integer)--
select * from t
where col1 = {{a}} and col2 in ({{cv}}) and col3 = {{b}}
"""
        twig = parser(lexer(sql), "$")["sql_stmts"][0]
        helper = DataProviderHelper()
        input_shape = _PropBag({"a": 10, "cv": [1, 2, 3], "b": 99})
        exe = helper.get_executable_content("?", twig, input_shape)
        values = helper.build_parameters(
            exe,
            input_shape,
            lambda _t, v: v,
        )
        self.assertEqual(values, [10, 1, 2, 3, 99])

    def test_optional_array_in_via_args_shape(self):
        from yaal_shape import Shape

        sql = (
            "--($args.id integer[])--\n"
            "select * from t where optional(id in ({{$args.id}}))\n"
        )
        twig = parser(lexer(sql), "$")["sql_stmts"][0]
        empty_args = Shape(
            schema={"type": "object", "properties": {}},
            extras={
                "$args": Shape(
                    schema={
                        "type": "object",
                        "properties": {"id": {"type": "array"}},
                    },
                    data={},
                )
            },
        )
        helper = DataProviderHelper()
        elided = helper.get_executable_content("?", twig, empty_args)
        self.assertEqual(
            elided["content"].strip(),
            "select * from t",
        )
        bound = helper.get_executable_content(
            "?",
            twig,
            Shape(
                schema={"type": "object", "properties": {}},
                extras={
                    "$args": Shape(
                        schema={
                            "type": "object",
                            "properties": {"id": {"type": "array"}},
                        },
                        data={"id": [1, 2]},
                    )
                },
            ),
        )
        self.assertIn("in (?, ?)", bound["content"])
        shape_ids = Shape(
            schema={"type": "object", "properties": {}},
            extras={
                "$args": Shape(
                    schema={
                        "type": "object",
                        "properties": {"id": {"type": "array"}},
                    },
                    data={"id": [1, 2]},
                )
            },
        )
        self.assertEqual(
            helper.build_parameters(bound, shape_ids, lambda _t, v: v),
            [1, 2],
        )


if __name__ == "__main__":
    unittest.main()

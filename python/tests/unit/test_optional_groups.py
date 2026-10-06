# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import unittest

from yaal_executor import DataProviderHelper
from yaal_parser import compile_sql, group_shapes_for_compile, lexer, parser


class _Shape:
    def __init__(self, props):
        self._props = props

    def get_prop(self, name):
        return self._props.get(name)


class TestOptionalGroupsBind(unittest.TestCase):

    def test_bind_order_mixed_in_and_scalar(self):
        sql = (
            "--(pairs blob)--\n"
            "select * from t where optional_groups("
            "{{pairs}}, col2 in ({{cv}}) and col1 = {{cv1}})\n"
        )
        twig = parser(lexer(sql), "$")["sql_stmts"][0]
        pairs = [
            {"cv": [1, 2, 3], "cv1": 10},
            {"cv": [4], "cv1": 20},
        ]
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
        helper = DataProviderHelper()
        values = helper.build_parameters(compiled, shape, lambda _t, v: v)
        self.assertEqual(values, [1, 2, 3, 10, 4, 20])

    def test_bind_case_insensitive_row_keys(self):
        sql = (
            "--(pairs blob)--\n"
            "select * from t where optional_groups({{pairs}}, col1 = {{cv1}})\n"
        )
        twig = parser(lexer(sql), "$")["sql_stmts"][0]
        shape = _Shape({"pairs": [{"CV1": 7}]})
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
        helper = DataProviderHelper()
        values = helper.build_parameters(compiled, shape, lambda _t, v: v)
        self.assertEqual(values, [7])


if __name__ == "__main__":
    unittest.main()

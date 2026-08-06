# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import unittest

from yaal_parser import lexer, parser


class TestParseHarden(unittest.TestCase):

    def test_tab_and_cr_are_space_tokens(self):
        tokens = lexer("a\tb\rc")
        types = [t["type"] for t in tokens]
        self.assertEqual(types, ["word", "space", "word", "space", "word"])

    def test_sql_line_comment_discarded(self):
        tokens = lexer("select 1 -- ignore\nfrom t")
        values = [t["value"] for t in tokens if t["type"] != "space"]
        self.assertEqual(values, ["select", "1", "\n", "from", "t"])
        self.assertFalse(any(t["type"] == "dash" for t in tokens))

    def test_sql_directive_still_dash(self):
        tokens = lexer("select 1 --sql-- select 2")
        self.assertTrue(any(t["type"] == "dash" and t["value"] == "--sql--" for t in tokens))

    def test_unclosed_string(self):
        with self.assertRaises(TypeError) as ctx:
            lexer("select 'oops")
        self.assertIn("unclosed string", str(ctx.exception))

    def test_leading_ws_then_header(self):
        ast = parser(lexer("\n\n--(id integer)--\nselect {{id}}"), "$")
        self.assertIn("id", ast["parameters"])
        self.assertEqual(ast["parameters"]["id"]["type"], "integer")
        self.assertFalse(ast["parameters"]["id"]["required"])

    def test_required_bang_on_name(self):
        ast = parser(lexer("--(id! integer, name! string)--\nselect {{id}}"), "$")
        self.assertTrue(ast["parameters"]["id"]["required"])
        self.assertTrue(ast["parameters"]["name"]["required"])
        self.assertEqual(ast["parameters"]["id"]["name"], "id")

    def test_required_bang_on_args(self):
        ast = parser(lexer("--($args.id! integer)--\nselect {{$args.id}}"), "$")
        self.assertTrue(ast["parameters"]["$args.id"]["required"])
        self.assertEqual(ast["parameters"]["$args.id"]["name"], "$args.id")

    def test_invalid_bang_placement(self):
        with self.assertRaises(TypeError):
            parser(lexer("--(!id integer)--\nselect 1"), "$")


if __name__ == "__main__":
    unittest.main()

# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Unit coverage for result-column $mode control values."""

import unittest

from yaal import create_context
from yaal_executor import _execute_branch


def _branch(twig_count=1):
    return {
        "path": "op",
        "connections": ["db"],
        "input_type": "object",
        "method": "$",
        "twigs": [
            {"connection": "db", "content": [], "parameters": []}
            for _ in range(twig_count)
        ],
        "model": {"output": None},
        "output_type": "array",
    }


class ScriptedProvider:
    def __init__(self, responses):
        self._responses = list(responses)
        self.begun = self.ended = self.errored = False

    def begin(self):
        self.begun = True

    def end(self):
        self.ended = True

    def error(self):
        self.errored = True

    def execute(self, twig, ctx, helper):
        return self._responses.pop(0)


class TestModes(unittest.TestCase):
    def test_mode_params_copies_into_params_bag(self):
        provider = ScriptedProvider(
            [
                ([{"$mode": "params", "total_count": 42}], None),
                ([{"page": 1, "total_count": 42}], None),
            ]
        )
        ctx = create_context({"path": "op"})
        out, errors = _execute_branch(
            _branch(twig_count=2), True, {"db": provider}, ctx, []
        )
        self.assertIsNone(errors)
        self.assertEqual(out, [{"page": 1, "total_count": 42}])
        self.assertEqual(ctx.get_prop("$params").get_prop("total_count"), 42)
        self.assertTrue(provider.begun)
        self.assertTrue(provider.ended)
        self.assertFalse(provider.errored)

    def test_mode_error_returns_soft_errors(self):
        provider = ScriptedProvider(
            [([{"$mode": "error", "message": "nope", "code": 1}], None)]
        )
        ctx = create_context({"path": "op"})
        out, errors = _execute_branch(_branch(), True, {"db": provider}, ctx, [])
        self.assertIsNone(out)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["message"], "nope")
        self.assertTrue(provider.errored)
        self.assertFalse(provider.ended)

    def test_mode_break_returns_rows_without_mode_column(self):
        provider = ScriptedProvider(
            [
                (
                    [
                        {"$mode": "break", "id": 1, "name": "a"},
                        {"$mode": "break", "id": 2, "name": "b"},
                    ],
                    None,
                )
            ]
        )
        ctx = create_context({"path": "op"})
        out, errors = _execute_branch(_branch(), True, {"db": provider}, ctx, [])
        self.assertIsNone(errors)
        self.assertEqual(out, [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
        for row in out:
            self.assertNotIn("$mode", row)
        self.assertTrue(provider.ended)

    def test_mode_json_parses_string_column(self):
        provider = ScriptedProvider(
            [([{"$mode": "json", "json": '{"id": 7, "ok": true}'}], None)]
        )
        ctx = create_context({"path": "op"})
        out, errors = _execute_branch(_branch(), True, {"db": provider}, ctx, [])
        self.assertIsNone(errors)
        self.assertEqual(out, [{"id": 7, "ok": True}])
        self.assertTrue(provider.ended)

    def test_mode_json_passes_through_non_string(self):
        payload = {"id": 3, "name": "x"}
        provider = ScriptedProvider([([{"$mode": "json", "json": payload}], None)])
        ctx = create_context({"path": "op"})
        out, errors = _execute_branch(_branch(), True, {"db": provider}, ctx, [])
        self.assertIsNone(errors)
        self.assertEqual(out, [payload])

    def test_ordinary_rows_without_mode(self):
        provider = ScriptedProvider([([{"id": 1}, {"id": 2}], None)])
        ctx = create_context({"path": "op"})
        out, errors = _execute_branch(_branch(), True, {"db": provider}, ctx, [])
        self.assertIsNone(errors)
        self.assertEqual(out, [{"id": 1}, {"id": 2}])


if __name__ == "__main__":
    unittest.main()

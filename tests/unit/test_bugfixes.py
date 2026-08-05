import copy
import unittest

from yaal import create_context, _parse_rfc1738_args
from yaal_executor import DataProviderHelper, _execute_branch
from yaal_shape import Shape, _to_lower_keys_deep


class TestBugfixes(unittest.TestCase):

    def test_array_params_not_cached_across_items(self):
        helper = DataProviderHelper()
        sql = {"parameters": [{"name": "id", "type": "integer"}], "content": "x"}
        self.assertEqual(helper.build_parameters(sql, Shape(data={"id": 1}), lambda t, v: v), [1])
        self.assertEqual(helper.build_parameters(sql, Shape(data={"id": 2}), lambda t, v: v), [2])

    def test_zero_is_converted(self):
        helper = DataProviderHelper()
        sql = {"parameters": [{"name": "n", "type": "integer"}], "content": "x"}
        self.assertEqual(helper.build_parameters(sql, Shape(data={"n": 0}), lambda t, v: v), [0])

    def test_empty_array_stays_list(self):
        shape = Shape(schema={"type": "array", "properties": {}}, data=[])
        self.assertEqual(shape._data, [])
        self.assertEqual(shape.get_prop("$length"), 0)

    def test_schema_not_mutated_on_array_item_failure(self):
        schema = {"type": "array", "properties": {"a": {"type": "integer"}}}
        schema_before = copy.deepcopy(schema)
        with self.assertRaises(TypeError):
            Shape(schema=schema, data=[{"a": 1}, "bad"])
        self.assertEqual(schema, schema_before)

    def test_required_and_nested_properties_lowercased(self):
        raw = {
            "type": "object",
            "required": ["User"],
            "properties": {
                "User": {
                    "type": "object",
                    "properties": {"FirstName": {"type": "string"}},
                }
            },
        }
        normalized = _to_lower_keys_deep(raw)
        self.assertEqual(normalized["required"], ["user"])
        self.assertIn("user", normalized["properties"])
        self.assertIn("firstname", normalized["properties"]["user"]["properties"])

    def test_bool_false_string(self):
        shape = Shape(schema={"type": "object", "properties": {"b": {"type": "boolean"}}})
        shape.set_prop("b", "false")
        self.assertIs(shape.get_prop("b"), False)
        shape.set_prop("b", "true")
        self.assertIs(shape.get_prop("b"), True)

    def test_action_error_cleans_up_connection(self):
        class Leak:
            def __init__(self):
                self.begun = self.ended = self.errored = False

            def begin(self):
                self.begun = True

            def end(self):
                self.ended = True

            def error(self):
                self.errored = True

            def execute(self, twig, ctx, helper):
                return [{"$action": "error", "message": "boom"}], None

        descriptor = {
            "path": "p",
            "connections": ["db"],
            "input_type": "object",
            "method": "$",
            "twigs": [{"connection": "db", "content": [], "parameters": []}],
            "model": {"output": None},
            "output_type": "array",
        }
        ctx = create_context({"path": "p"})
        leak = Leak()
        out, errors = _execute_branch(descriptor, True, {"db": leak}, ctx, [])
        self.assertIsNone(out)
        self.assertTrue(errors)
        self.assertTrue(leak.begun)
        self.assertFalse(leak.ended)
        self.assertTrue(leak.errored)

    def test_use_parent_rows_skips_twigs(self):
        calls = {"n": 0}

        class DP:
            def execute(self, twig, ctx, helper):
                calls["n"] += 1
                return [{"role_id": 1}], None

        branch = {
            "input_type": "object",
            "use_parent_rows": True,
            "method": "$.roles",
            "name": "roles",
            "twigs": [{"connection": "db", "content": [], "parameters": []}],
            "output_type": "array",
        }
        ctx = create_context({"path": "p"})
        parent = [{"user_id": 1, "role_id": 1}]
        out, err = _execute_branch(branch, False, {"db": DP()}, ctx, parent)
        self.assertIsNone(err)
        self.assertEqual(calls["n"], 0)
        self.assertEqual(out[0]["user_id"], 1)

    def test_child_lists_are_copied_per_parent_row(self):
        class DP:
            def __init__(self):
                self.calls = 0

            def begin(self):
                pass

            def end(self):
                pass

            def error(self):
                pass

            def execute(self, twig, ctx, helper):
                self.calls += 1
                if self.calls == 1:
                    return [{"id": 1}, {"id": 2}], None
                return [{"c": 1}], None

        trunk = {
            "input_type": "object",
            "method": "$",
            "connections": ["db"],
            "twigs": [{"connection": "db", "content": [], "parameters": []}],
            "output_type": "array",
            "branches": [
                {
                    "name": "child",
                    "input_type": "object",
                    "method": "$.child",
                    "twigs": [{"connection": "db", "content": [], "parameters": []}],
                    "output_type": "array",
                }
            ],
        }
        ctx = create_context({"path": "p"})
        out, err = _execute_branch(trunk, True, {"db": DP()}, ctx, [])
        self.assertIsNone(err)
        self.assertIsNot(out[0]["child"], out[1]["child"])
        out[0]["child"].append({"c": 2})
        self.assertEqual(len(out[1]["child"]), 1)

    def test_sqlite_uri_dot_relative_and_absolute(self):
        name, opts = _parse_rfc1738_args("sqlite3://./serve/db/app.db")
        self.assertEqual(name, "sqlite3")
        self.assertEqual(opts["database"], "./serve/db/app.db")

        name, opts = _parse_rfc1738_args("sqlite3:////tmp/yaal.db")
        self.assertEqual(opts["database"], "/tmp/yaal.db")

    def test_username_is_unquoted(self):
        name, opts = _parse_rfc1738_args("postgresql://user%40name:p%40ss@localhost:5432/db")
        self.assertEqual(opts["username"], "user@name")
        self.assertEqual(opts["password"], "p@ss")


if __name__ == "__main__":
    unittest.main()

import unittest

from yaal_executor import _output_mapper
from yaal_output_schema import normalize_output_model


class TestFlatOutputSchema(unittest.TestCase):

    def test_flat_maps_fields(self):
        output_model = {
            "type": "array",
            "properties": {
                "id": {"mapped": "id"},
                "name": {"mapped": "name"},
            },
        }
        rows = [
            {"id": 1, "name": "a", "extra": 9},
            {"id": 2, "name": "b", "extra": 8},
        ]
        result = _output_mapper("array", output_model, None, rows)
        self.assertEqual(result, [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])

    def test_nested_item_wrapper_rejected(self):
        output_model = {
            "type": "array",
            "properties": {
                "type": "object",
                "properties": {
                    "id": {"mapped": "id"},
                },
            },
        }
        with self.assertRaises(TypeError) as ctx:
            normalize_output_model(output_model)
        self.assertIn("bare properties.type", str(ctx.exception))

    def test_sibling_type_meta_rejected(self):
        output_model = {
            "type": "array",
            "properties": {
                "type": "object",
                "id": {"mapped": "id"},
            },
        }
        with self.assertRaises(TypeError) as ctx:
            _output_mapper("array", output_model, None, [{"id": 1}])
        self.assertIn("bare properties.type", str(ctx.exception))

    def test_field_named_type_via_mapped_still_works(self):
        output_model = {
            "type": "array",
            "properties": {
                "type": {"mapped": "kind"},
                "id": {"mapped": "id"},
            },
        }
        rows = [{"id": 1, "kind": "user"}]
        result = _output_mapper("array", output_model, None, rows)
        self.assertEqual(result, [{"type": "user", "id": 1}])

    def test_parent_rows_object_child_shapes(self):
        from yaal_executor import _output_mapper

        # Mapper shapes each row; parent_rows nesting is executed at branch level.
        # Here we verify the nested branch model itself is flat and valid.
        child_model = {
            "type": "object",
            "parent_rows": True,
            "properties": {
                "name": {"mapped": "name"},
            },
        }
        normalized = normalize_output_model(child_model)
        self.assertEqual(
            normalized["properties"],
            {"name": {"mapped": "name"}},
        )
        result = _output_mapper(
            "object",
            child_model,
            None,
            [{"id": 1, "name": "a"}],
        )
        self.assertEqual(result, {"name": "a"})


class TestBuilderFlatSchema(unittest.TestCase):

    def test_builder_rejects_item_wrapper(self):
        from yaal_builder import _build_branch

        class Reader:
            def get_sql(self, method, path):
                return None

            def list_sql(self, path):
                return []

        branch = {"path": "p", "method": "$", "name": "$"}
        bag = {}
        model = {
            "args": {"type": "object", "properties": {}},
            "payload": {"type": "object", "properties": {}},
            "params": {"type": "object", "properties": {}},
            "output": {
                "type": "array",
                "properties": {
                    "type": "object",
                    "properties": {
                        "id": {"mapped": "id"},
                    },
                },
            },
        }
        with self.assertRaises(TypeError):
            _build_branch(
                branch, {}, Reader(), model["payload"], model["output"], model, bag,
            )

    def test_builder_flat_with_parent_rows_child(self):
        from yaal_builder import _build_branch

        class Reader:
            def get_sql(self, method, path):
                return None

            def list_sql(self, path):
                return []

        branch = {"path": "p", "method": "$", "name": "$"}
        bag = {}
        model = {
            "args": {"type": "object", "properties": {}},
            "payload": {"type": "object", "properties": {}},
            "params": {"type": "object", "properties": {}},
            "output": {
                "type": "array",
                "partition_by": "id",
                "properties": {
                    "id": {"mapped": "id"},
                    "details": {
                        "type": "object",
                        "parent_rows": True,
                        "properties": {
                            "name": {"mapped": "name"},
                        },
                    },
                },
            },
        }
        _build_branch(
            branch, {}, Reader(), model["payload"], model["output"], model, bag,
        )
        self.assertEqual(branch["output_type"], "array")
        self.assertEqual(branch["partition_by"], "id")
        self.assertEqual(len(branch["branches"]), 1)
        self.assertEqual(branch["branches"][0]["name"], "details")
        self.assertTrue(branch["branches"][0]["use_parent_rows"])


if __name__ == "__main__":
    unittest.main()

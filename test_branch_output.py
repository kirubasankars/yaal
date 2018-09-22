import unittest

from yaal import _execute_branch, create_context


class FakeDataProviderSimple:

    def begin(self):
        pass

    def end(self):
        pass

    def error(self):
        pass

    def execute(self, leaf, input_shape, helper):
        return [], 0


class FakeDataProviderDeep:

    def begin(self):
        pass

    def end(self):
        pass

    def error(self):
        pass

    def execute(self, leaf, input_shape, helper):
        return [{"name": "kiruba"}, {"name": "sankar"}], 0


class FakeDataProviderParentRows:

    def begin(self):
        pass

    def end(self):
        pass

    def error(self):
        pass

    def execute(self, leaf, input_shape, helper):
        if leaf["content"] == "parent":
            return [{"id": 1, "name": "kiruba"}, {"id": 2, "name": "sankar"}], 0
        return [], 0


class TestGravity(unittest.TestCase):

    def test_execute_branch_simple(self):
        descriptor = {
            "path": "user",
            "leafs": [],
            "input_type": "object",
            "output_type": "array",
            "_validators": None,
            "partition_by": None,
            "use_parent_rows": None,
            "branches": None
        }

        ctx = create_context(descriptor)
        rs, errors = _execute_branch(descriptor, FakeDataProviderSimple(), ctx, [], None)

        self.assertListEqual(rs, [])
        self.assertEqual(errors, None)

    def test_execute_branches_deep(self):
        descriptor = {
            "name": "$",
            "method": "$",
            "path": "path",
            "leafs": [

            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None,
            "partition_by": None,
            "use_parent_rows": None,
            "payload": {
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array"
                    }
                }
            },
            "_validators": {
                "payload": None
            },
            "branches": [
                {
                    "name": "items",
                    "method": "$.items",
                    "input_type": "array",
                    "partition_by": None,
                    "use_parent_rows": None,
                    "branches": None,
                    "leafs": [
                        {
                            "content": "select"
                        }
                    ]
                }
            ]
        }

        ctx = create_context(descriptor, payload={"items": [{}]})
        rs, errors = _execute_branch(descriptor, FakeDataProviderDeep(), ctx, [], None)

        self.assertListEqual(rs, [{"items": [{'name': 'kiruba'}, {'name': 'sankar'}]}])
        self.assertEqual(errors, None)

    def test_execute_branches_deep2(self):
        descriptor = {
            "name": "$",
            "method": "$",
            "path": "path",
            "leafs": [
                {
                    "content": "select"
                }
            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None,
            "partition_by": None,
            "use_parent_rows": None,
            "payload": {
                "type": "object",
                "properties": {
                    "items": {  # items are expected as available on branches
                        "type": "array"
                    }
                }
            },
            "_validators": {
                "payload": None
            },
            "branches": [
                {
                    "name": "items",
                    "method": "$.items",
                    "input_type": "array",
                    "partition_by": None,
                    "use_parent_rows": None,
                    "branches": None,
                    "leafs": [
                        {
                            "content": "select"
                        }
                    ]
                }
            ]
        }

        ctx = create_context(descriptor, payload={"items": [{}, {}]})
        rs, errors = _execute_branch(descriptor, FakeDataProviderDeep(), ctx, [], None)

        self.assertListEqual(rs, [{'name': 'kiruba',
                                   'items': [{'name': 'kiruba'}, {'name': 'sankar'}, {'name': 'kiruba'},
                                             {'name': 'sankar'}]}, {'name': 'sankar',
                                                                    'items': [{'name': 'kiruba'}, {'name': 'sankar'},
                                                                              {'name': 'kiruba'}, {'name': 'sankar'}]}])
        self.assertEqual(errors, None)

    def test_execute_branches_parent_rows(self):
        descriptor = {
            "name": "$",
            "method": "$",
            "path": "path",
            "leafs": [
                {
                    "content": "parent"
                }
            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None,
            "partition_by": "id",
            "use_parent_rows": None,
            "payload": {
                "type": "object",
                "properties": {
                    "items": {  # items are expected as available on branches
                        "type": "array"
                    }
                }
            },
            "_validators": {
                "payload": None
            },
            "branches": [
                {
                    "name": "items",
                    "method": "$.items",
                    "input_type": "array",
                    "partition_by": None,
                    "use_parent_rows": True,
                    "branches": None,
                    "leafs": [
                        {
                            "content": "select"
                        }
                    ]
                }
            ]
        }

        ctx = create_context(descriptor, payload={"items": [{}, {}]})
        rs, errors = _execute_branch(descriptor, FakeDataProviderParentRows(), ctx, [], None)

        self.assertListEqual(rs, [{'id': 1, 'name': 'kiruba', 'items': [{'id': 1, 'name': 'kiruba'}]},
                                  {'id': 2, 'name': 'sankar', 'items': [{'id': 2, 'name': 'sankar'}]}])
        self.assertEqual(errors, None)


if __name__ == "__main__":
    unittest.main()

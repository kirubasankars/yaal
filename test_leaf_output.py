import unittest

from gravity import _execute_leafs, create_context


class FakeExecutionContext:

    def begin(self):
        pass

    def end(self):
        pass

    def error(self):
        pass

    def execute(self, trunk, input_shape, helper):
        if trunk["content"] == "error":
            return [{"$http_status_code": 400, "$type": "error", "message": "message1"},
                    {"$type": "error", "message": "message2", '$http_status_code': 400}], 0

        if trunk["content"] == "cookie":
            return [{"$type": "cookie", "name": "name1", "value": "value"},
                    {"$type": "cookie", "name": "name2", "value": "value"}], 0

        if trunk["content"] == "header":
            return [{"$type": "header", "name": "name1", "value": "value"},
                    {"$type": "header", "name": "name2", "value": "value"}], 0

        if trunk["content"] == "params":
            return [{"$type": "params", "a": "1", "b": "2"}], 1

        if trunk["content"] == "break":
            return [{"$type": "break", "a": "1", "b": "2"}], 1


class TestGravity(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_trunk_with_error(self):
        trunk = {
            "leafs": [
                {
                    "content": "error"
                }
            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None
        }

        ctx = create_context(trunk, "", "user", None, None, None, None, None)

        p = {
            "db": FakeExecutionContext()
        }

        rs, errors = _execute_leafs(trunk, p, ctx, None)

        self.assertListEqual(errors, [{"$type": "error", "message": "message1", '$http_status_code': 400},
                                      {"$type": "error", "message": "message2", '$http_status_code': 400}])
        self.assertEqual(ctx.get_prop("$response.status_code"), 400)

    def test_trunk_with_cookie(self):
        trunk = {
            "leafs": [
                {
                    "content": "cookie"
                }
            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None
        }

        ctx = create_context(trunk, "", "user", None, None, None, None, None)

        p = {
            "db": FakeExecutionContext()
        }

        _execute_leafs(trunk, p, ctx, None)

        self.assertDictEqual(ctx.get_prop("$response.$cookie").get_data(),
                             {'name1': {"$type": 'cookie', 'name': 'name1', 'value': 'value'},
                              'name2': {"$type": 'cookie', 'name': 'name2', 'value': 'value'}})

    def test_trunk_with_header(self):
        trunk = {
            "leafs": [
                {
                    "content": "header"
                }
            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None
        }

        ctx = create_context(trunk, "", "user", None, None, None, None, None)

        p = {
            "db": FakeExecutionContext()
        }

        _execute_leafs(trunk, p, ctx, None)

        self.assertDictEqual(ctx.get_prop("$response.$header").get_data(),
                             {'name1': {"$type": 'header', 'name': 'name1', 'value': 'value'},
                              'name2': {"$type": 'header', 'name': 'name2', 'value': 'value'}})

    def test_trunk_with_params(self):
        trunk = {
            "leafs": [
                {
                    "content": "params"
                }
            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None
        }

        ctx = create_context(trunk, "namespace", "path", None, None, None, None, None)

        p = {
            "db": FakeExecutionContext()
        }

        _execute_leafs(trunk, p, ctx, None)

        d = ctx.get_prop("$params").get_data()
        self.assertDictEqual(d, {"$last_inserted_id": 1, "namespace": 'namespace', "path": "path",
                                 "$type": 'params', 'a': '1', 'b': '2'})

    def test_trunk_with_break(self):
        trunk = {
            "leafs": [
                {
                    "content": "break",
                    "connection": "db"
                }
            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None
        }

        ctx = create_context(trunk, "", "user", None, None, None, None, None)

        p = {
            "db": FakeExecutionContext()
        }

        rs, errors = _execute_leafs(trunk, p, ctx, None)

        self.assertListEqual(rs, [{'a': '1', 'b': '2'}])

    def test_trunk_with_connection_missing(self):
        trunk = {
            "leafs": [
                {
                    "content": "break",
                    "connection": "db1"
                }
            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None
        }

        ctx = create_context(trunk, "", "user", None, None, None, None, None)

        p = {
            "db": FakeExecutionContext()
        }

        with self.assertRaises(Exception): _execute_leafs(trunk, p, ctx, None)


if __name__ == "__main__":
    unittest.main()

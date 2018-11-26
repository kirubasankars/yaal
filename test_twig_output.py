import unittest

from yaal import _execute_twigs, create_context


class FakeContextManager:

    def get_context(self):
        return FakeContextManager()


class FakeDataProvider:

    def begin(self):
        pass

    def end(self):
        pass

    def error(self):
        pass

    def execute(self, leaf, input_shape, helper):
        if leaf["content"] == "error":
            return [{"$action": "error", "$http_status_code": 400, "message": "message1"},
                    {"$action": "error", "$http_status_code": 400, "message": "message2"}], 0

        if leaf["content"] == "cookie":
            return [{"$action": "cookie", "name": "name1", "value": "value"},
                    {"$action": "cookie", "name": "name2", "value": "value"}], 0

        if leaf["content"] == "header":
            return [{"$action": "header", "name": "name1", "value": "value"},
                    {"$action": "header", "name": "name2", "value": "value"}], 0

        if leaf["content"] == "params":
            return [{"$action": "params", "a": "1", "b": "2"}], 1

        if leaf["content"] == "break":
            return [{"$action": "break", "a": "1", "b": "2"}], 1


class TestGravity(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_trunk_with_error(self):
        descriptor = {
            "path": "user",
            "twigs": [
                {
                    "content": "error"
                }
            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None
        }

        ctx = create_context(descriptor)

        rs, errors = _execute_twigs(descriptor, {"db": FakeDataProvider()}, ctx, None)

        self.assertListEqual(errors, [{"$action": "error", "message": "message1", '$http_status_code': 400},
                                      {"$action": "error", "message": "message2", '$http_status_code': 400}])
        self.assertEqual(ctx.get_prop("$response.status_code"), 400)

    def test_trunk_with_cookie(self):
        descriptor = {
            "twigs": [
                {
                    "content": "cookie"
                }
            ],
            "path": "user",
            "input_type": "object",
            "output_type": "array",
            "_validators": None
        }

        ctx = create_context(descriptor)

        _execute_twigs(descriptor, {"db":FakeDataProvider()}, ctx, None)

        self.assertDictEqual(ctx.get_prop("$response.$cookie").get_data(),
                             {'name1': {"$action": 'cookie', 'name': 'name1', 'value': 'value'},
                              'name2': {"$action": 'cookie', 'name': 'name2', 'value': 'value'}})

    def test_trunk_with_header(self):
        descriptor = {
            "twigs": [
                {
                    "content": "header"
                }
            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None,
            "path": "user"
        }

        ctx = create_context(descriptor)

        _execute_twigs(descriptor, {"db": FakeDataProvider()}, ctx, None)

        self.assertDictEqual(ctx.get_prop("$response.$header").get_data(),
                             {'name1': {"$action": 'header', 'name': 'name1', 'value': 'value'},
                              'name2': {"$action": 'header', 'name': 'name2', 'value': 'value'}})

    def test_trunk_with_params(self):
        descriptor = {
            "twigs": [
                {
                    "content": "params"
                }
            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None,
            "path": "path"
        }

        ctx = create_context(descriptor)

        _execute_twigs(descriptor, {"db": FakeDataProvider()}, ctx, None)

        d = ctx.get_prop("$params").get_data()

        self.assertDictEqual(d, {"$last_inserted_id": 1,
                                 "path": "path", "$action": 'params', 'a': '1', 'b': '2'})

    def test_trunk_with_break(self):
        descriptor = {
            "twigs": [
                {
                    "content": "break",
                    "connection": "db"
                }
            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None,
            "path": "user"
        }

        ctx = create_context(descriptor)
        rs, errors = _execute_twigs(descriptor, {"db":FakeDataProvider()}, ctx, None)

        self.assertListEqual(rs, [{'a': '1', 'b': '2'}])

    def test_trunk_with_connection_missing(self):
        descriptor = {
            "twigs": [
                {
                    "content": "break",
                    "connection": "db1"
                }
            ],
            "input_type": "object",
            "output_type": "array",
            "_validators": None,
            "path": "user"
        }

        ctx = create_context(descriptor)

        with self.assertRaises(Exception): _execute_twigs(descriptor, {"db":FakeDataProvider()}, ctx, None)


if __name__ == "__main__":
    unittest.main()

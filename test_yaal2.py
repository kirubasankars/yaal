import unittest

from yaal import Yaal, create_context, get_result


class FakeExecutionContext:

    def begin(self):
        pass

    def end(self):
        pass

    def error(self):
        pass

    def execute(self, leaf, input_shape, helper):
        return [input_shape._data], 0


class FakeContentReader:

    def get_sql(self, method, path):
        return "INSERT"

    def get_config(self, path):
        return {
            "input.model": {
                "payload": {
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "properties": {
                                "product": {
                                    "type": "object"
                                }
                            }
                        }
                    }
                }
            }
        }

    def list_sql(self, path):
        return ["$", "$.items"]

    @staticmethod
    def get_routes_config(path):
        return None


class TestGravity(unittest.TestCase):

    def setUp(self):
        self._gravity = Yaal("/path", FakeContentReader(), True)

    def tearDown(self):
        pass

    def test_simple_post_descriptor_check(self):
        descriptor = self._gravity.create_descriptor("post1/post")

        self.assertTrue(descriptor["name"] == "$")
        self.assertTrue(descriptor["method"] == "$")

        branches = descriptor["branches"]
        self.assertEqual(len(branches), 1)

        branch_data_items = branches[0]
        self.assertTrue(branch_data_items["name"] == "items")
        self.assertTrue(branch_data_items["method"] == "$.items")

        branches = branch_data_items["branches"]
        self.assertEqual(branches, None)

    def test_simple_get_shape_check(self):
        descriptor = self._gravity.create_descriptor("post1/post")
        input_shape = create_context(descriptor)

        self.assertIsNotNone(input_shape._shapes["items"])
        self.assertEqual(len(input_shape._shapes["items"]._shapes), 0)

    @staticmethod
    def get_data_provider():
        return FakeExecutionContext()

    def test_run(self):
        descriptor = self._gravity.create_descriptor("post1/post")
        d = {"Items": [{"A": 1}, {"b": 1}], "name": {"F": 1}}
        input_shape = create_context(descriptor, payload=d)

        rs = get_result(descriptor, self.get_data_provider, input_shape)

        self.assertListEqual(rs, [{"items": [{"a": 1}, {"b": 1}], "name": {"F": 1}}])

        self.assertDictEqual(input_shape.get_prop("name"), {"F": 1})


if __name__ == "__main__":
    unittest.main()

import unittest

from gravity import Gravity, create_context, get_result


class ContentReader:

    def __init__(self):
        pass

    def get_sql(self, method, path):
        if method == "$":
            return "SELECT 1 as test"

        return None

    def get_config(self, path):
        pass

    def list_sql(self, path):
        return ["$"]

    def get_routes_config(self, path):
        return None


class TestGravity(unittest.TestCase):

    def test_simple_get_trunk_check(self):
        g = Gravity("", ContentReader(), True)
        g.setup_data_provider("sqlite3:///")
        descriptor = g.create_descriptor("get/get")

        self.assertTrue(descriptor["name"] == "$")
        self.assertTrue(descriptor["method"] == "$")

        ctx = create_context(descriptor, "app", "get/get")

        r = get_result(descriptor, g.get_data_provider, ctx)

        self.assertListEqual([{"test": 1}], r)


if __name__ == "__main__":
    unittest.main()

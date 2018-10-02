import unittest

from yaal import Yaal, create_context, get_result


class ContentReader:

    def __init__(self):
        pass

    def get_sql(self, method, path):
        if path == "name/get":
            if method == "$":
                return "--(name)--\n" \
                       "SELECT {{name}} as name"

        return None

    def get_config(self, path):
        return {
            "input.model": {
                "payload": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string"
                        }
                    }
                }
            }
        }

    def list_sql(self, path):
        if path == "name/get":
            return ["$"]
        return None

    def get_routes_config(self, path):
        return None


class TestGravity(unittest.TestCase):

    def test_simple_get_trunk_check(self):
        g = Yaal("", ContentReader(), True)
        g.setup_data_provider("db", "sqlite3:///")
        descriptor = g.create_descriptor("name/get")

        self.assertTrue(descriptor["name"] == "$")
        self.assertTrue(descriptor["method"] == "$")

        ctx = create_context(descriptor, payload={"Name": "Kiruba"})

        r = get_result(descriptor, g.get_data_provider, ctx)

        self.assertListEqual([{"name": "Kiruba"}], r)


if __name__ == "__main__":
    unittest.main()

import unittest
import json
from yaal import Yaal, create_context


class ContentReader:

    def __init__(self):
        pass

    def get_sql(self, method, path):
        if path == "name/get":
            if method == "$":
                return "--(name string)--\n" \
                       "SELECT {{name}} || ' Last' as name"

        return None

    def get_config(self, path, mapper):
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

    def setUp(self):
        self._g = Yaal("", ContentReader(), True)
        self._g.setup_data_provider("db", "sqlite3:///")

    def tearDown(self):
        pass

    def test_simple_get_trunk_check(self):
        g = self._g

        descriptor = g.create_descriptor("name/get", None)
        self.assertTrue(descriptor["name"] == "$")
        self.assertTrue(descriptor["method"] == "$")

        descriptor_ctx = g.get_descriptor_path_by_route("name", "get")
        ctx = create_context(descriptor, payload={"Name": "First"})
        r = g.get_result_json(descriptor, descriptor_ctx, ctx)

        self.assertListEqual([{"name": "First Last"}], json.loads(r))

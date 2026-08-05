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


class TestYaal(unittest.TestCase):

    def setUp(self):
        self._yaal = Yaal("", ContentReader(), debug=True)
        self._yaal.setup_data_provider("db", "sqlite3:///")

    def tearDown(self):
        pass

    def test_simple_get_trunk_check(self):
        y = self._yaal

        descriptor = y.create_descriptor("name/get", None)
        self.assertTrue(descriptor["name"] == "$")
        self.assertTrue(descriptor["method"] == "$")

        ctx = create_context(descriptor, payload={"Name": "First"})
        r = y.get_result_json(descriptor, ctx)

        self.assertListEqual([{"name": "First Last"}], json.loads(r))

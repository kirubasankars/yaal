# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import unittest

from yaal import Yaal
from yaal_builder import create_trunk


class HeaderContentReader:
    def __init__(self, sql_by_method=None, files=None):
        self._sql = sql_by_method or {}
        self._files = files or ["$"]

    def get_sql(self, method, path):
        return self._sql.get(method)

    def get_config(self, path, output_mapper=None):
        return {"output.model": None}

    def list_sql(self, path):
        return list(self._files)


class TestInputFromHeaders(unittest.TestCase):

    def test_derives_args_and_payload_schema(self):
        reader = HeaderContentReader({
            "$": "--($args.id integer, name! string)--\nselect {{$args.id}}, {{name}}\n",
        })
        trunk = create_trunk("op", None, reader)
        args = trunk["model"]["args"]
        payload = trunk["model"]["payload"]
        self.assertEqual(args["properties"]["id"]["type"], "integer")
        self.assertNotIn("required", args)
        self.assertEqual(payload["properties"]["name"]["type"], "string")
        self.assertEqual(payload["required"], ["name"])

    def test_maps_bool_and_float_types(self):
        reader = HeaderContentReader({
            "$": "--(flag bool, amount float)--\nselect {{flag}}, {{amount}}\n",
        })
        trunk = create_trunk("op", None, reader)
        props = trunk["model"]["payload"]["properties"]
        self.assertEqual(props["flag"]["type"], "boolean")
        self.assertEqual(props["amount"]["type"], "number")

    def test_required_missing_soft_error(self):
        yaal = Yaal("", HeaderContentReader({
            "$": "--(id! integer, name! string)--\nselect {{id}} as id, {{name}} as name\n",
        }), debug=True)
        yaal.setup_data_provider("db", "sqlite3:///")
        result = yaal.query("op", payload={"name": "x"})
        self.assertIn("errors", result)
        self.assertTrue(any("id" in e.get("message", "") for e in result["errors"]))

    def test_type_mismatch_soft_error(self):
        yaal = Yaal("", HeaderContentReader({
            "$": "--(id integer)--\nselect {{id}} as id\n",
        }), debug=True)
        yaal.setup_data_provider("db", "sqlite3:///")
        result = yaal.query("op", payload={"id": "not-an-int"})
        self.assertIn("errors", result)

    def test_conflict_type_across_files(self):
        reader = HeaderContentReader(
            {
                "$": "--($args.id integer)--\nselect {{$args.id}} as id\n",
                "$.child": "--($args.id string)--\nselect {{$args.id}} as id\n",
            },
            files=["$", "$.child"],
        )
        with self.assertRaises(TypeError) as ctx:
            create_trunk("op", None, reader)
        self.assertIn("conflicting parameter", str(ctx.exception))

    def test_conflict_required_across_files(self):
        reader = HeaderContentReader(
            {
                "$": "--($args.id integer)--\nselect {{$args.id}} as id\n",
                "$.child": "--($args.id! integer)--\nselect {{$args.id}} as id\n",
            },
            files=["$", "$.child"],
        )
        with self.assertRaises(TypeError) as ctx:
            create_trunk("op", None, reader)
        self.assertIn("conflicting parameter", str(ctx.exception))

    def test_same_declaration_twice_ok(self):
        reader = HeaderContentReader(
            {
                "$": "--($args.id! integer)--\nselect {{$args.id}} as id\n",
                "$.child": "--($args.id! integer)--\nselect {{$args.id}} as id\n",
            },
            files=["$", "$.child"],
        )
        trunk = create_trunk("op", None, reader)
        self.assertEqual(trunk["model"]["args"]["required"], ["id"])
        self.assertEqual(trunk["model"]["args"]["properties"]["id"]["type"], "integer")

    def test_fixture_user_get_without_input_yaml(self):
        from pathlib import Path

        api = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "api"
        yaal = Yaal(str(api), debug=True)
        yaal.setup_data_provider("db", "sqlite3:///")
        # seed via existing fixture tests' schema is heavy; just ensure descriptor builds
        d = yaal.create_descriptor("user/get")
        self.assertEqual(d["model"]["args"]["properties"]["id"]["type"], "integer")
        self.assertNotIn("required", d["model"]["args"])


if __name__ == "__main__":
    unittest.main()

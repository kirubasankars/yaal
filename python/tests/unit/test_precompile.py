# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import json
import os
import tempfile
import unittest
from pathlib import Path

from yaal import Yaal
from yaal_cli import list_descriptors
from yaal_precompile import (
    compile_api,
    export_descriptor,
    import_descriptor,
    load_precompiled_file,
)


ROOT = Path(__file__).resolve().parents[3]
FIXTURE_API = ROOT / "tests" / "fixtures" / "api"
SCHEMA = ROOT / "docker" / "sqlite" / "schema.sql"


class TestPrecompile(unittest.TestCase):

    def test_export_keeps_token_arrays(self):
        y = Yaal(str(FIXTURE_API), debug=True)
        desc = y.create_descriptor("user/get")
        exported = export_descriptor(desc)
        self.assertNotIn("_validators", exported)
        twig = exported["twigs"][0]
        self.assertIsInstance(twig["content"], list)
        self.assertIsInstance(twig["content"][0], dict)
        self.assertIn("type", twig["content"][0])

    def test_import_rebuilds_validators(self):
        y = Yaal(str(FIXTURE_API), debug=True)
        desc = y.create_descriptor("user/get")
        exported = export_descriptor(desc)
        loaded = import_descriptor(exported)
        self.assertIn("_validators", loaded)
        self.assertIsNotNone(loaded["_validators"]["args"])

    def test_compile_and_query_matches_source(self):
        import sqlite3

        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            with sqlite3.connect(db_path) as con:
                con.executescript(SCHEMA.read_text())
            url = "sqlite3:///%s" % db_path

            with tempfile.TemporaryDirectory() as out:
                written = compile_api(str(FIXTURE_API), out)
                self.assertIn("user/get.json", written)

                src = Yaal(str(FIXTURE_API), debug=True)
                src.setup_data_provider("db", url)
                expected = src.query("user/get", args={"id": 1})

                pre = Yaal(str(FIXTURE_API), precompiled=out)
                pre.setup_data_provider("db", url)
                actual = pre.query("user/get", args={"id": 1})
                self.assertEqual(actual, expected)

                # Twig tokens still drive explain/compile
                explained = pre.explain_sql("user/get", args={"id": 1})
                self.assertTrue(explained)
                self.assertIn("select", explained[0]["sql"].lower())
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass

    def test_debug_ignores_precompiled(self):
        with tempfile.TemporaryDirectory() as out:
            compile_api(str(FIXTURE_API), out, list_paths=["user/get"])
            # Corrupt the artifact so load would fail if used
            path = Path(out) / "user" / "get.json"
            path.write_text("{not-json", encoding="utf-8")

            y = Yaal(str(FIXTURE_API), debug=True, precompiled=out)
            desc = y._load_descriptor("user/get")
            self.assertIn("twigs", desc)

    def test_missing_precompiled_raises(self):
        from yaal_errors import DescriptorNotFoundError

        with tempfile.TemporaryDirectory() as out:
            y = Yaal(str(FIXTURE_API), precompiled=out)
            with self.assertRaises(DescriptorNotFoundError):
                y._load_descriptor("user/get")


if __name__ == "__main__":
    unittest.main()

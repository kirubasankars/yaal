import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import yaal_cli

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_API = str(ROOT / "tests" / "fixtures" / "api")


class TestCli(unittest.TestCase):

    def _run(self, argv):
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = yaal_cli.main(argv)
        return code, buf.getvalue()

    def test_list_includes_user_get(self):
        code, out = self._run(["--api", FIXTURE_API, "list"])
        self.assertEqual(code, 0)
        lines = out.splitlines()
        self.assertIn("user/get", lines)
        self.assertIn("user/list", lines)
        self.assertIn("user/page", lines)
        self.assertIn("user/create", lines)

    def test_query_user_get(self):
        code, out = self._run([
            "--api", FIXTURE_API,
            "query", "user/get",
            "--arg", "id=1",
        ])
        self.assertEqual(code, 0)
        result = json.loads(out)
        self.assertEqual(result["id"], 1)
        self.assertEqual(result["name"], "admin")
        self.assertEqual(len(result["roles"]), 2)

    def test_explain_user_get(self):
        code, out = self._run([
            "--api", FIXTURE_API,
            "explain", "user/get",
            "--arg", "id=1",
        ])
        self.assertEqual(code, 0)
        self.assertIn("select", out.lower())
        self.assertIn("binds:", out)
        self.assertIn("1", out)

    def test_parse_arg_json_and_kv(self):
        class NS:
            args_json = '{"id": 2}'
            arg = ["status=\"open\""]

        # Replicate merge via private helpers used by CLI
        merged = {}
        parsed = json.loads(NS.args_json)
        merged.update(parsed)
        merged.update(yaal_cli._parse_kv(NS.arg))
        self.assertEqual(merged["id"], 2)
        self.assertEqual(merged["status"], "open")


if __name__ == "__main__":
    unittest.main()

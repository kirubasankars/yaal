#!/usr/bin/env python3
# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""CLI for quick Yaal demos and experiments (stdlib argparse only)."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEFAULT_API = ROOT / "tests" / "fixtures" / "api"
DEFAULT_SCHEMA = ROOT / "docker" / "sqlite" / "schema.sql"


def _parse_value(raw: str):
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw


def _parse_kv(items):
    out = {}
    for item in items or []:
        if "=" not in item:
            raise argparse.ArgumentTypeError(
                "expected KEY=VALUE, got %r" % item
            )
        key, value = item.split("=", 1)
        if not key:
            raise argparse.ArgumentTypeError(
                "expected KEY=VALUE, got %r" % item
            )
        out[key] = _parse_value(value)
    return out


def _merge_args(ns):
    merged = {}
    if ns.args_json is not None:
        parsed = json.loads(ns.args_json)
        if not isinstance(parsed, dict):
            raise SystemExit("--args must be a JSON object")
        merged.update(parsed)
    merged.update(_parse_kv(ns.arg))
    return merged or None


def _parse_payload(raw):
    if raw is None:
        return None
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise SystemExit("--payload must be a JSON object")
    return parsed


def list_descriptors(api_root: Path):
    """Return descriptor paths relative to api_root (folders with $.sql or *.sql)."""
    found = []
    api_root = api_root.resolve()
    if not api_root.is_dir():
        return found
    for dirpath, _dirnames, filenames in os.walk(api_root):
        sql_files = [f for f in filenames if f.endswith(".sql")]
        if not sql_files:
            continue
        rel = Path(dirpath).resolve().relative_to(api_root)
        found.append(str(rel).replace(os.sep, "/"))
    return sorted(found)


@contextlib.contextmanager
def _demo_db_url(schema_path: Path):
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        with sqlite3.connect(db_path) as con:
            con.executescript(schema_path.read_text())
        yield "sqlite3:///%s" % db_path
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass


def _build_parser():
    parser = argparse.ArgumentParser(
        prog="yaal",
        description="Query, explain, and list Yaal SQL→JSON descriptors.",
    )
    parser.add_argument(
        "--api",
        default=str(DEFAULT_API),
        help="Descriptor root (default: tests/fixtures/api)",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Database URL. If omitted, a temp SQLite DB is seeded from docker/sqlite/schema.sql",
    )
    parser.add_argument(
        "--provider",
        default="db",
        help="Data provider name (default: db)",
    )
    parser.add_argument(
        "--schema",
        default=str(DEFAULT_SCHEMA),
        help="SQLite schema used when --db is omitted",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Disable descriptor caching / force live SQL+YAML (ignores --precompiled)",
    )
    parser.add_argument(
        "--precompiled",
        default=None,
        help="Load descriptors from a directory produced by `yaal compile`",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="List descriptor paths under --api")

    compile_p = sub.add_parser(
        "compile",
        help="Precompile descriptors under --api to JSON (no database required)",
    )
    compile_p.add_argument(
        "--out",
        required=True,
        help="Output directory for *.json descriptor artifacts",
    )

    for name, help_text in (
        ("query", "Run Yaal.query and print nested JSON"),
        ("explain", "Run Yaal.explain_sql and print SQL + binds"),
    ):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("path", help="Descriptor path, e.g. user/get")
        p.add_argument(
            "--arg",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help="Operation arg (repeatable). Values parsed as JSON when possible",
        )
        p.add_argument(
            "--args",
            dest="args_json",
            default=None,
            help='Args as a JSON object, e.g. \'{"id":1}\'',
        )
        p.add_argument(
            "--payload",
            default=None,
            help="Payload as a JSON object",
        )

    return parser


def _with_yaal(ns, fn):
    from yaal import Yaal

    y = Yaal(ns.api, debug=ns.debug, precompiled=ns.precompiled)
    if ns.db:
        y.setup_data_provider(ns.provider, ns.db)
        return fn(y)
    schema = Path(ns.schema)
    if not schema.is_file():
        raise SystemExit("schema file not found: %s" % schema)
    with _demo_db_url(schema) as url:
        y.setup_data_provider(ns.provider, url)
        return fn(y)


def cmd_list(ns):
    for path in list_descriptors(Path(ns.api)):
        print(path)
    return 0


def cmd_compile(ns):
    from yaal_precompile import compile_api

    written = compile_api(ns.api, ns.out)
    for rel in written:
        print(rel)
    print("wrote %d descriptor(s) to %s" % (len(written), ns.out), file=sys.stderr)
    return 0


def cmd_query(ns):
    args = _merge_args(ns)
    payload = _parse_payload(ns.payload)

    def run(y):
        result = y.query(ns.path, args=args, payload=payload)
        print(json.dumps(result, indent=2))

    _with_yaal(ns, run)
    return 0


def cmd_explain(ns):
    args = _merge_args(ns)
    payload = _parse_payload(ns.payload)

    def run(y):
        for twig in y.explain_sql(ns.path, args=args, payload=payload):
            print(twig["sql"].strip())
            print("binds:", twig["parameters"])
            print()

    _with_yaal(ns, run)
    return 0


def main(argv=None):
    parser = _build_parser()
    ns = parser.parse_args(argv)
    handlers = {
        "list": cmd_list,
        "compile": cmd_compile,
        "query": cmd_query,
        "explain": cmd_explain,
    }
    return handlers[ns.command](ns)


if __name__ == "__main__":
    sys.exit(main())

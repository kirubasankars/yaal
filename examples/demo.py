#!/usr/bin/env python3
# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Run the shared fixture API against a temp SQLite DB and print each example."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from yaal import Yaal  # noqa: E402

API = ROOT / "tests" / "fixtures" / "api"
SCHEMA = ROOT / "docker" / "sqlite" / "schema.sql"
FLAGS_SCHEMA = ROOT / "docker" / "sqlite" / "flags_schema.sql"


def _print(title: str, value) -> None:
    print(f"-- {title} --")
    print(json.dumps(value, indent=2, default=str))
    print()


def main() -> int:
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    fd2, flags_path = tempfile.mkstemp(suffix=".db")
    os.close(fd2)
    try:
        sqlite3.connect(db_path).executescript(SCHEMA.read_text())
        sqlite3.connect(flags_path).executescript(FLAGS_SCHEMA.read_text())
        y = Yaal(str(API), debug=True)
        y.setup_data_provider("db", "sqlite3:///" + db_path)
        y.setup_data_provider("flags", "sqlite3:///" + flags_path)

        _print("user/get id=1", y.query("user/get", args={"id": 1}))
        _print("user/nested id=1", y.query("user/nested", args={"id": 1}))
        _print("user/list active=1", y.query("user/list", args={"active": 1}))
        _print(
            "user/list sort=name dir=desc",
            y.query("user/list", args={"sort": "name", "dir": "desc"}),
        )
        _print(
            "user/list sort=name,id dir=desc,asc (multi-column)",
            y.query("user/list", args={"sort": "name,id", "dir": "desc,asc"}),
        )
        _print(
            "user/page page=1 page_size=1",
            y.query("user/page", args={"page": 1, "page_size": 1}),
        )
        _print("report/summary", y.query("report/summary"))
        _print("user/combine id=1", y.query("user/combine", args={"id": 1}))

        print("-- explain user/list (active omitted → optional elided) --")
        for twig in y.explain_sql("user/list"):
            print(twig["sql"].strip())
            print("binds:", twig["parameters"])
            print()

        print("-- explain user/list active=1 --")
        for twig in y.explain_sql("user/list", args={"active": 1}):
            print(twig["sql"].strip())
            print("binds:", twig["parameters"])
            print()
        return 0
    finally:
        for path in (db_path, flags_path):
            try:
                os.unlink(path)
            except OSError:
                pass


if __name__ == "__main__":
    raise SystemExit(main())

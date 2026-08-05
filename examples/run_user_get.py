#!/usr/bin/env python3
"""Minimal Yaal example: SQL descriptors → nested JSON via sqlite."""

import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from yaal import Yaal

SCHEMA = ROOT / "docker" / "sqlite" / "schema.sql"
API = ROOT / "tests" / "fixtures" / "api"


def main():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        with sqlite3.connect(db_path) as con:
            con.executescript(SCHEMA.read_text())

        y = Yaal(str(API), debug=True)
        y.setup_data_provider("db", "sqlite3:///%s" % db_path)

        result = y.query("user/1", "get")
        print(json.dumps(result, indent=2))

        print("\n-- explain_sql (path.id present) --")
        for twig in y.explain_sql("user/1", "get"):
            print(twig["sql"].strip())
            print("binds:", twig["parameters"])
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass


if __name__ == "__main__":
    main()

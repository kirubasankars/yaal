# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Stage the shared library and test the Go package without YAAL_LIB."""

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GO = ROOT / "bindings" / "go"
NATIVE = GO / "native"

LIBRARIES = {
    "linux-x64": "libyaal_ffi.so",
    "osx-arm64": "libyaal_ffi.dylib",
    "win-x64": "yaal_ffi.dll",
}


def project_version():
    return (GO / "version.txt").read_text().strip()


def check(require):
    missing = []
    found = []
    for rid, name in LIBRARIES.items():
        path = NATIVE / rid / name
        if path.is_file() and path.stat().st_size > 0:
            found.append(rid)
            continue
        if rid in require:
            missing.append(f"{rid}/{name}")
    if not found:
        raise SystemExit(f"no staged natives in {NATIVE}")
    if missing:
        raise SystemExit(f"staged natives are missing {missing}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-stage", action="store_true", help="use natives already staged")
    parser.add_argument("--require", help="comma-separated runtime ids that must be staged")
    args = parser.parse_args()
    if not project_version():
        raise SystemExit("version not found in bindings/go/version.txt")
    if not args.no_stage:
        subprocess.run([sys.executable, str(GO / "stage.py")], check=True)
    require = [item for item in (args.require or "").split(",") if item]
    for rid in require:
        if rid not in LIBRARIES:
            raise SystemExit(f"unknown runtime id {rid}")
    check(set(require))
    env = os.environ.copy()
    env.pop("YAAL_LIB", None)
    subprocess.run(["go", "test", "."], cwd=GO, env=env, check=True)
    print(f"yaal-go {project_version()}")


if __name__ == "__main__":
    main()

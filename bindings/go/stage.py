# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Copy the built shared library into the Go package."""

import argparse
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NATIVE = ROOT / "bindings" / "go" / "native"

LIBRARIES = {
    "linux-x64": "libyaal_ffi.so",
    "osx-arm64": "libyaal_ffi.dylib",
    "win-x64": "yaal_ffi.dll",
}


def runtime():
    if sys.platform == "win32":
        return "win-x64", LIBRARIES["win-x64"]
    if sys.platform == "darwin":
        return "osx-arm64", LIBRARIES["osx-arm64"]
    return "linux-x64", LIBRARIES["linux-x64"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lib", type=Path, help="shared library to stage")
    parser.add_argument("--rid", help="runtime id, such as linux-x64")
    args = parser.parse_args()
    rid, name = runtime()
    if args.rid:
        if args.rid not in LIBRARIES:
            raise SystemExit(f"unknown runtime id {args.rid}")
        rid = args.rid
        name = LIBRARIES[rid]
    lib = args.lib
    if lib is None:
        target = Path(os.environ.get("CARGO_TARGET_DIR") or ROOT / "target")
        lib = target / "release" / name
    if not lib.is_file():
        raise SystemExit(f"shared library not found: {lib}")
    dest = NATIVE / rid / name
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(lib, dest)
    print(dest)


if __name__ == "__main__":
    main()

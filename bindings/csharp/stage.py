# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Copy the built shared library into the NuGet runtime folder."""

import argparse
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIMES = ROOT / "bindings" / "csharp" / "Yaal" / "runtimes"


def runtime():
    if sys.platform == "win32":
        return "win-x64", "yaal_ffi.dll"
    if sys.platform == "darwin":
        return "osx-arm64", "libyaal_ffi.dylib"
    return "linux-x64", "libyaal_ffi.so"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lib", type=Path, help="shared library to stage")
    parser.add_argument("--rid", help="NuGet runtime identifier")
    args = parser.parse_args()
    rid, name = runtime()
    if args.rid:
        rid = args.rid
    lib = args.lib
    if lib is None:
        target = Path(os.environ.get("CARGO_TARGET_DIR") or ROOT / "target")
        lib = target / "release" / name
    if not lib.is_file():
        raise SystemExit(f"shared library not found: {lib}")
    dest = RUNTIMES / rid / "native" / lib.name
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(lib, dest)
    print(dest)


if __name__ == "__main__":
    main()

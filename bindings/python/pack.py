# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Build a wheel that contains the Yaal shared library."""

import argparse
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "bindings" / "python" / "yaal"


def library_name():
    return {
        "win32": "yaal_ffi.dll",
        "darwin": "libyaal_ffi.dylib",
    }.get(sys.platform, "libyaal_ffi.so")


def platform_tag(system, machine):
    machine = machine.lower()
    if system == "win32":
        if machine in {"amd64", "x86_64"}:
            return "win_amd64"
        if machine == "arm64":
            return "win_arm64"
        if machine in {"x86", "i386", "i686"}:
            return "win32"
    if machine in {"amd64", "x86_64"}:
        machine = "x86_64"
    elif machine == "arm64" and system == "linux":
        machine = "aarch64"
    if system == "linux":
        return f"linux_{machine}"
    if system == "darwin":
        return f"macosx_11_0_{machine}"
    raise SystemExit(f"unsupported platform {system} {machine}")


def default_platform_tag():
    return platform_tag(sys.platform, platform.machine())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lib", type=Path, help="shared library to bundle")
    parser.add_argument("--platform-tag", help="wheel platform tag")
    parser.add_argument("--outdir", type=Path, default=ROOT / "artifacts")
    args = parser.parse_args()

    target = Path(os.environ.get("CARGO_TARGET_DIR") or ROOT / "target")
    lib = args.lib or (target / "release" / library_name())
    if not lib.is_file():
        raise SystemExit(f"shared library not found: {lib}")

    dest = PACKAGE / lib.name
    outdir = args.outdir
    outdir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(lib, dest)
    try:
        subprocess.run(
            [sys.executable, "-m", "build", "--wheel", "--outdir", str(outdir)],
            cwd=ROOT,
            check=True,
        )
        built = sorted(outdir.glob("yaal_sql-*-py3-none-any.whl"))
        if len(built) != 1:
            raise SystemExit(f"expected one new wheel, found {built}")
        tag = args.platform_tag or default_platform_tag()
        subprocess.run(
            [
                sys.executable,
                "-m",
                "wheel",
                "tags",
                "--remove",
                "--python-tag",
                "py3",
                "--abi-tag",
                "none",
                "--platform-tag",
                tag,
                str(built[0]),
            ],
            check=True,
        )
    finally:
        dest.unlink(missing_ok=True)
        shutil.rmtree(ROOT / "build", ignore_errors=True)
        shutil.rmtree(ROOT / "bindings" / "python" / "yaal_sql.egg-info", ignore_errors=True)


if __name__ == "__main__":
    main()

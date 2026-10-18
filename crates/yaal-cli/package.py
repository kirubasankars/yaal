# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Pack a yaal CLI binary into a release archive."""

import argparse
import io
import json
import platform
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGETS = {
    "linux-x64": "yaal",
    "macos-arm64": "yaal",
    "windows-x64": "yaal.exe",
}


def project_version():
    manifest = ROOT / "crates" / "yaal-cli" / "Cargo.toml"
    for line in manifest.read_text().splitlines():
        if line.startswith('version = "'):
            return line.split('"', 2)[1]
    raise SystemExit(f"version not found in {manifest}")


def host_target():
    machine = platform.machine().lower()
    if sys.platform.startswith("linux") and machine in {"x86_64", "amd64"}:
        return "linux-x64"
    if sys.platform == "darwin" and machine in {"arm64", "aarch64"}:
        return "macos-arm64"
    if sys.platform == "win32" and machine in {"amd64", "x86_64"}:
        return "windows-x64"
    raise SystemExit(
        f"no release archive for {sys.platform} {machine}; pass --target and --binary"
    )


def smoke(binary, version):
    if not binary.is_file() or binary.stat().st_size == 0:
        raise SystemExit(f"missing binary {binary}")

    completed = subprocess.run(
        [str(binary), "--version"], check=False, capture_output=True, text=True
    )
    if completed.returncode != 0:
        raise SystemExit(completed.stderr.strip() or f"{binary} --version failed")
    got = completed.stdout.strip()
    expect = f"yaal {version}"
    if got != expect:
        raise SystemExit(f"--version printed {got!r}, expected {expect!r}")

    sql = (
        "--(id)--\n"
        "select * from users u where optional(u.user_id in ({{id}}...))\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "query.sql"
        path.write_text(sql, newline="\n")
        completed = subprocess.run(
            [str(binary), "compile", "--sql", str(path), "--param", "id=[1, 2]"],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise SystemExit(completed.stderr.strip() or "compile failed")
        compiled = json.loads(completed.stdout)
        statement = compiled["statements"][0]
        if statement["sql"] != "\nselect * from users u where (u.user_id in (?, ?))\n":
            raise SystemExit(f"unexpected sql {statement['sql']!r}")
        if statement["parameters"] != [
            {"name": "id", "type": "integer", "value": 1},
            {"name": "id", "type": "integer", "value": 2},
        ]:
            raise SystemExit(f"unexpected parameters {statement['parameters']!r}")

    completed = subprocess.run(
        [str(binary), "compile", "--placeholder", "numbered", "--param", "id=1"],
        input=b"--(id)--\nselect {{id}}\n",
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise SystemExit(completed.stderr.decode().strip() or "stdin compile failed")
    compiled = json.loads(completed.stdout)
    if compiled["statements"][0]["sql"] != "\nselect $1\n":
        raise SystemExit(f"unexpected numbered sql {compiled['statements'][0]['sql']!r}")

    completed = subprocess.run(
        [str(binary), "compile", "--placeholder", "nope"],
        input=b"--(id)--\nselect {{id}}\n",
        check=False,
        capture_output=True,
    )
    if completed.returncode == 0:
        raise SystemExit("unknown placeholder exited 0")


def readme(version):
    return (
        f"Yaal SQL compiler CLI {version}\n"
        "\n"
        "Run `yaal --help`.\n"
        "Documentation: https://kirubasankars.github.io/yaal/cli/\n"
        "License: MIT (see LICENSE).\n"
    )


def package(binary, target, version, out_dir):
    exe_name = TARGETS[target]
    out_dir.mkdir(parents=True, exist_ok=True)
    license_path = ROOT / "LICENSE"
    note = readme(version)
    if target == "windows-x64":
        dest = out_dir / f"yaal-{version}-windows-x64.zip"
        with zipfile.ZipFile(dest, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.write(binary, exe_name)
            archive.write(license_path, "LICENSE")
            archive.writestr("README.txt", note)
        return dest

    dest = out_dir / f"yaal-{version}-{target}.tar.gz"
    with tarfile.open(dest, "w:gz") as archive:
        info = archive.gettarinfo(binary, arcname=exe_name)
        info.mode = 0o755
        with binary.open("rb") as handle:
            archive.addfile(info, handle)
        archive.add(license_path, arcname="LICENSE")
        data = note.encode()
        text = tarfile.TarInfo("README.txt")
        text.size = len(data)
        text.mode = 0o644
        archive.addfile(text, io.BytesIO(data))
    return dest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=sorted(TARGETS))
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--out", type=Path, default=ROOT / "artifacts")
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()

    target = args.target or host_target()
    binary = args.binary or (ROOT / "target" / "release" / TARGETS[target])
    version = project_version()
    if args.smoke:
        smoke(binary, version)
    dest = package(binary, target, version, args.out)
    print(dest)


if __name__ == "__main__":
    main()

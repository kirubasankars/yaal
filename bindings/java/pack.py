# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Build a jar that contains the Yaal shared library."""

import argparse
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
JAVA = ROOT / "bindings" / "java"
STAGED = JAVA / "target" / "staged" / "yaal" / "native"

LIBRARIES = {
    "linux-x64": "libyaal_ffi.so",
    "osx-arm64": "libyaal_ffi.dylib",
    "win-x64": "yaal_ffi.dll",
}


def project_version():
    for line in (JAVA / "pom.xml").read_text().splitlines():
        if line.startswith("  <version>") and line.endswith("</version>"):
            return line.removeprefix("  <version>").removesuffix("</version>")
    raise SystemExit("project version not found in bindings/java/pom.xml")


def staged_entries():
    if not STAGED.is_dir():
        raise SystemExit(f"staged natives not found: {STAGED}")
    entries = []
    for path in sorted(STAGED.rglob("*")):
        if path.is_file():
            entries.append("yaal/native/" + path.relative_to(STAGED).as_posix())
    if not entries:
        raise SystemExit(f"no staged natives in {STAGED}")
    return entries


def check_jar(jar, require):
    names = zipfile.ZipFile(jar).namelist()
    missing = [entry for entry in staged_entries() if entry not in names]
    for rid in require:
        if rid not in LIBRARIES:
            raise SystemExit(f"unknown runtime id {rid}")
        entry = f"yaal/native/{rid}/{LIBRARIES[rid]}"
        if entry not in names:
            missing.append(entry)
    if missing:
        raise SystemExit(f"{jar} is missing {missing}")


def run_jar(jar):
    classes = JAVA / "target" / "test-classes"
    if classes.exists():
        shutil.rmtree(classes)
    classes.mkdir(parents=True)
    javac = os.environ.get("JAVAC") or "javac"
    java = os.environ.get("JAVA") or "java"
    subprocess.run(
        [
            javac,
            "--release",
            "22",
            "-d",
            str(classes),
            "-cp",
            str(jar),
            *sorted((JAVA / "test").glob("yaal/*.java")),
        ],
        check=True,
    )
    env = os.environ.copy()
    env.pop("YAAL_LIB", None)
    subprocess.run(
        [
            java,
            "--enable-native-access=ALL-UNNAMED",
            "-cp",
            os.pathsep.join((str(jar), str(classes))),
            "yaal.YaalTest",
        ],
        cwd=ROOT,
        env=env,
        check=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-stage", action="store_true", help="use natives already staged")
    parser.add_argument("--require", help="comma-separated runtime ids that the jar must contain")
    parser.add_argument("--outdir", type=Path, default=ROOT / "artifacts")
    args = parser.parse_args()
    if not args.no_stage:
        subprocess.run([sys.executable, str(JAVA / "stage.py")], check=True)
    mvnw = JAVA / ("mvnw.cmd" if sys.platform == "win32" else "mvnw")
    subprocess.run([str(mvnw), "-B", "package"], cwd=JAVA, check=True)
    jar = JAVA / "target" / f"yaal-{project_version()}.jar"
    if not jar.is_file():
        raise SystemExit(f"jar not found: {jar}")
    require = [item for item in (args.require or "").split(",") if item]
    check_jar(jar, require)
    run_jar(jar)
    args.outdir.mkdir(parents=True, exist_ok=True)
    dest = args.outdir / jar.name
    shutil.copy2(jar, dest)
    print(dest)


if __name__ == "__main__":
    main()

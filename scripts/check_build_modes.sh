#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
python3 - "$root" <<'PY'
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

root = sys.argv[1]
cases = [
    ("ios", ["--device"], ["aarch64-apple-ios"]),
    ("ios", [], ["aarch64-apple-darwin", "aarch64-apple-ios", "aarch64-apple-ios-sim"]),
    ("ios", ["--check"], ["aarch64-apple-darwin", "aarch64-apple-ios-sim"]),
    ("kotlin", [], ["arm64-v8a", "armeabi-v7a"]),
    ("kotlin", ["--host"], ["host"]),
    ("kotlin", ["--abis"], ["arm64-v8a", "armeabi-v7a", "x86_64", "x86"]),
    ("kotlin", ["--release"], ["arm64-v8a", "armeabi-v7a"]),
]
for platform, args, expected in cases:
    result = subprocess.run(["bash", f"{root}/scripts/build_{platform}.sh", *args],
                            env={**os.environ, "DBLAYER_BUILD_TARGETS_ONLY": "1"},
                            capture_output=True, text=True)
    assert result.returncode == 0 and result.stdout.splitlines() == expected, (platform, args, result)
for platform in ("ios", "kotlin"):
    for args in (["--unknown"], ["--check", "extra"]):
        result = subprocess.run(["bash", f"{root}/scripts/build_{platform}.sh", *args],
                                env={**os.environ, "DBLAYER_BUILD_TARGETS_ONLY": "1"},
                                capture_output=True, text=True)
        assert result.returncode == 2 and "usage:" in result.stderr, (platform, args, result)
with tempfile.TemporaryDirectory() as directory:
    fixture = Path(directory)
    (fixture / "scripts").mkdir()
    (fixture / "core/src").mkdir(parents=True)
    script = fixture / "scripts/native_source_hash.sh"
    shutil.copyfile(Path(root) / "scripts/native_source_hash.sh", script)
    for name in ("Cargo.toml", "Cargo.lock", "core/Cargo.toml", "core/src/lib.rs"):
        (fixture / name).write_text("initial\n")
    subprocess.run(["git", "init", "-q", directory], check=True)
    subprocess.run(["git", "-C", directory, "add", "."], check=True)
    before = subprocess.check_output(["bash", str(script)], text=True)
    (fixture / "Cargo.toml").write_text("[profile.release]\nopt-level = 3\n")
    after = subprocess.check_output(["bash", str(script)], text=True)
    assert before != after, "Android SOURCE_HASH ignores the workspace release profile"
    (fixture / "gen").mkdir()
    (fixture / "gen/z.rs").write_text("added\n")
    untracked = subprocess.check_output(["bash", str(script)], text=True)
    subprocess.run(["git", "-C", directory, "add", "gen/z.rs"], check=True)
    tracked = subprocess.check_output(["bash", str(script)], text=True)
    assert untracked == tracked, "SOURCE_HASH changes when the same file becomes tracked"
    (fixture / "gen/z.rs").write_text("changed\n")
    changed = subprocess.check_output(["bash", str(script)], text=True)
    assert changed != tracked, "SOURCE_HASH ignores a change of a tracked source"
print("build modes: all target sets and invalid arguments passed")
print("Android SOURCE_HASH includes the workspace release profile")
PY

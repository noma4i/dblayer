#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
python3 - "$root" <<'PY'
import os
import subprocess
import sys

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
print("build modes: all target sets and invalid arguments passed")
PY

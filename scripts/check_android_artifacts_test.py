import os
import shutil
import subprocess
import tempfile
from pathlib import Path

source = Path(__file__).resolve().parent / "check_android_artifacts.sh"
ABIS = ["arm64-v8a", "x86_64"]


def run(args=(), declared=ABIS, built=ABIS, stamp="h1", current="h1", expect=0, contains=None):
    with tempfile.TemporaryDirectory() as base:
        root = Path(base)
        (root / "scripts").mkdir()
        shutil.copy2(source, root / "scripts/check_android_artifacts.sh")
        (root / "scripts/build_kotlin.sh").write_text("#!/bin/sh\nprintf '%s\\n' arm64-v8a x86_64\n")
        (root / "scripts/native_source_hash.sh").write_text(f"#!/bin/sh\necho {current}\n")
        quoted = ", ".join(f'"{abi}"' for abi in declared)
        (root / "kotlin").mkdir()
        (root / "kotlin/build.gradle.kts").write_text(f"    abiFilters += listOf({quoted})\n")
        jni = root / "kotlin/src/main/jniLibs"
        for abi in built:
            (jni / abi).mkdir(parents=True)
            (jni / abi / "libdblayer.so").write_text("elf")
        jni.mkdir(parents=True, exist_ok=True)
        if stamp:
            (jni / "SOURCE_HASH").write_text(stamp)
        env = dict(os.environ, ANDROID_NDK_HOME=str(root))
        result = subprocess.run(["bash", str(root / "scripts/check_android_artifacts.sh"), *args], env=env, capture_output=True, text=True)
    output = result.stdout + result.stderr
    assert result.returncode == expect, f"rc={result.returncode} output={output}"
    if contains:
        assert contains in output, f"expected {contains!r} in {output!r}"


run(contains="android artifacts match the core sources")
run(["--abis"], declared=["x86_64"], contains="android artifacts match the core sources")
run(["--bogus"], expect=2, contains="usage: check_android_artifacts.sh")
run(declared=["arm64-v8a"], expect=1, contains="abiFilters in kotlin/build.gradle.kts differ")
run(built=["arm64-v8a"], expect=1, contains="android artifact missing: x86_64/libdblayer.so")
run(stamp="old", expect=1, contains="android artifacts are stale")
run(stamp="", expect=1, contains="android artifacts are stale")
print("check_android_artifacts_test: ok")

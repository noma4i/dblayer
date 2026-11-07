import shutil
import subprocess
import tempfile
import time
from pathlib import Path

here = Path(__file__).resolve().parent


def run(base, args, expect, contains=None):
    result = subprocess.run(["bash", str(base / "scripts/check_bounded.sh"), *args], capture_output=True, text=True, timeout=15)
    output = result.stdout + result.stderr
    assert result.returncode == expect, f"{args}: rc={result.returncode} output={output}"
    if contains:
        assert contains in output, f"expected {contains!r} in {output!r}"


def alive(pid):
    return subprocess.run(["kill", "-0", str(pid)], capture_output=True).returncode == 0


with tempfile.TemporaryDirectory() as tmp:
    base = Path(tmp)
    (base / "scripts").mkdir()
    for name in ("check_bounded.sh", "check_steps.sh"):
        shutil.copy2(here / name, base / "scripts" / name)
    (base / "scripts/check_processes_test.sh").write_text("#!/bin/sh\necho processes selftest ran\n")

    run(base, ["5", "green", "true"], 0)
    run(base, ["5", "red", "bash", "-c", "exit 7"], 7)
    run(base, ["--selftest"], 0, "processes selftest ran")

    pidfile = base / "child.pid"
    run(base, ["1", "slow", "bash", "-c", f"sleep 300 & echo $! > {pidfile}; wait"], 124, "check: step 'slow' passed 1 s, its process group was killed")
    child = int(pidfile.read_text())
    deadline = time.monotonic() + 5
    while alive(child) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not alive(child), f"child {child} survived the kill"
print("check_bounded_test: ok")

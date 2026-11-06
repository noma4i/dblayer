#!/usr/bin/env python3
"""Wait rule 4 for Rust tests: a test never waits without a bound.

Usage: check_waits_rust.py <dir>...  Prints one line per hit and exits 1 when there is any.
Only test_wait.rs may poll or block: it owns within, spin_until, yield_until, recv_within, recv_or_closed, join_within and Rendezvous.
"""
import re
import sys
from pathlib import Path

AWAIT = re.compile(r"\.(?:recv|next|changed|notified|join_next)\([^()]*\)\s*\.await")
BOUND = re.compile(r"\b(?:within|within_secs|serving|timeout)\(")
LOOP = re.compile(r"^\s*(?:while|loop)\b")
POLL = re.compile(r"\byield_now\(|\bsleep\(")
LIMIT = re.compile(r"Instant::now\(\)|\bdeadline\b|\bfor _ in\b|\b(?:within|timeout|spin_until|yield_until)\(")
ABSENCE = re.compile(r"recv_timeout\([^)]*\)\s*\.is_err\(\)")
BLOCKING = (
    (re.compile(r"\.recv\(\)(?!\s*\.await)"), "a blocking recv without a bound (use recv_within or recv_or_closed)"),
    (re.compile(r"\.join\(\)"), "a thread join without a bound (use join_within)"),
    (re.compile(r"\.wait\(\)"), "a barrier wait without a bound (use Rendezvous::meet)"),
)


def is_test_file(path):
    return path.suffix == ".rs" and path.name != "test_wait.rs" and (path.name.endswith("_tests.rs") or "tests" in path.parts)


def hits(path):
    lines = path.read_text(encoding="utf-8").split("\n")
    for number, line in enumerate(lines):
        if line.lstrip().startswith("//"):
            continue
        where = f"{path}:{number + 1}"
        if AWAIT.search(line) and not BOUND.search(line):
            yield f"{where}: rust: an await without a bound (use within): {line.strip()}"
        if LOOP.match(line) and POLL.search("\n".join(lines[number:number + 4])) and not LIMIT.search("\n".join(lines[max(number - 3, 0):number + 6])):
            yield f"{where}: rust: a poll loop without a deadline (use spin_until or yield_until): {line.strip()}"
        if ABSENCE.search(line):
            yield f"{where}: rust: a timeout is no proof of absence (assert on the state): {line.strip()}"
        for pattern, message in BLOCKING:
            if pattern.search(line) and not BOUND.search(line):
                yield f"{where}: rust: {message}: {line.strip()}"


def main(roots):
    found = [hit for root in roots for path in sorted(Path(root).rglob("*.rs")) if is_test_file(path) for hit in hits(path)]
    print("\n".join(found))
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
python3 - "$root" <<'PY'
import re
import sys
from collections import defaultdict
from pathlib import Path

root = Path(sys.argv[1])
source = root / "core" / "src"
allowlist_path = root / "scripts" / "zoo_allowlist.txt"
allowed = set()
if allowlist_path.exists():
    for line in allowlist_path.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            allowed.add(line)

files = [path for path in source.rglob("*.rs") if not path.name.endswith("_tests.rs")]
findings = []

definitions = defaultdict(list)
pattern = re.compile(r"^\s*pub (?:struct|enum|trait|type) ([A-Z][A-Za-z0-9]*)", re.M)
for path in files:
    for name in pattern.findall(path.read_text()):
        definitions[name].append(str(path.relative_to(root)))
for name, places in sorted(definitions.items()):
    if len(places) > 1 and f"type:{name}" not in allowed:
        findings.append(f"Z2 duplicate public type {name}: {', '.join(sorted(places))}")

clocks = [str(path.relative_to(root)) for path in files if re.search(r"^\s*pub fn set_clock\b", path.read_text(), re.M)]
if len(clocks) > 1 and "set_clock" not in allowed:
    findings.append(f"Z1 several set_clock methods: {', '.join(sorted(clocks))}")

supports = sorted(str(path.relative_to(root)) for path in source.rglob("support.rs"))
if len(supports) > 1 and "support" not in allowed:
    findings.append(f"Z6 several support.rs files: {', '.join(supports)}")

diagnostics = []
for path in files:
    for name in re.findall(r"^\s*pub struct ([A-Za-z]*(?:Counters|Diagnostics|Stats))\b", path.read_text(), re.M):
        diagnostics.append(f"{name} ({path.relative_to(root)})")
if len(diagnostics) > 1 and "diagnostics" not in allowed:
    findings.append("Z1 several diagnostics structs: " + ", ".join(sorted(diagnostics)))

for path in files:
    relative = str(path.relative_to(root))
    if "/store/" in relative or relative.endswith(("db/commit.rs", "fixtures.rs")):
        continue
    if re.search(r"\.commit_with\(|\bstore\s*\.commit\(", path.read_text()) and f"commit:{relative}" not in allowed:
        findings.append(f"Z7 commit outside db/commit.rs skips the parent pointer: {relative}")

for path in files:
    relative = str(path.relative_to(root))
    if relative.endswith("db/merge.rs") or "/schema/" in relative:
        continue
    if re.search(r"(?<!\bdb)(?<!\bself)\.merged\(", path.read_text()):
        findings.append(f"Z8 server rows merge through Db::merged only: {relative}")

for path in files:
    relative = str(path.relative_to(root))
    if relative.endswith(("db/swap.rs", "commit/mod.rs")):
        continue
    if re.search(r"Replaced \{ by:", path.read_text()):
        findings.append(f"Z9 a temp row is replaced in db/swap.rs only: {relative}")

for path in files:
    relative = str(path.relative_to(root))
    if "/sync/invalidate/" in relative:
        continue
    if re.search(r"\binvalidate_model\(", path.read_text()):
        findings.append(f"Z10 lists are marked stale and reloaded in sync/invalidate only: {relative}")

test_only = ("/fixtures.rs", "/spec_fixtures.rs", "/rig.rs", "/stress/", "/transport/mock.rs", "/invariants/", "/bin/")
panic_call = re.compile(r"\b(?:expect|unwrap)\(|\b(?:panic|unreachable|todo|unimplemented)!\(")
panic_allowed = [entry[len("panic:"):] for entry in allowed if entry.startswith("panic:")]
for path in files:
    relative = str(path.relative_to(root))
    if any(marker in "/" + relative for marker in test_only):
        continue
    for number, line in enumerate(path.read_text().splitlines(), 1):
        calls = panic_call.findall(line)
        if not calls:
            continue
        covering = {fragment for line_file, fragment in map(lambda entry: entry.split(":", 1), panic_allowed) if line_file == relative and fragment in line}
        if len(calls) <= sum(len(panic_call.findall(fragment)) for fragment in covering):
            continue
        findings.append(f"Z11 a phone library never panics on the environment, return an error or a degraded answer: {relative}:{number}: {line.strip()}")

if findings:
    print("zoo check found", len(findings), "problem(s):")
    for finding in findings:
        print(" -", finding)
    sys.exit(1)
print("zoo check: clean")
PY

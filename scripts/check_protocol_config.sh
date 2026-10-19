#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
python3 - "$root" "${1:-}" <<'PY'
import re
import sys
import tempfile
from pathlib import Path

root = Path(sys.argv[1])
exceptions = {
    "fixtures/change-events/kat.json",
    "fixtures/bootstrap-config/bootstrap_kat.json",
    "fixtures/encrypted-http/kat.json",
}
pattern = re.compile(r"/api/v3|[Yy][Uu][Pp][Ii]|[Zz]ooko|\bV3\b|\bv3[._:]")

def scan(directory):
    findings = []
    for name in ("core/src", "swift/Sources", "swift/Tests", "kotlin/src/main", "kotlin/src/test", "gen", "fixtures", "docs", "README.md"):
        base = directory / name
        paths = [base] if base.is_file() else sorted(base.rglob("*"))
        for path in paths:
            if not path.is_file() or str(path.relative_to(directory)) in exceptions:
                continue
            if path.suffix not in (".rs", ".swift", ".kt", ".json", ".md", ".dbl"):
                continue
            for number, line in enumerate(path.read_text().splitlines(), 1):
                if pattern.search(line):
                    findings.append(f"{path.relative_to(directory)}:{number}")
    return findings

if sys.argv[2] == "--selftest":
    with tempfile.TemporaryDirectory() as temporary:
        probe = Path(temporary)
        (probe / "core/src").mkdir(parents=True)
        path = probe / "core/src/foreign.rs"
        path.write_text('const PATH: &str = "/api/v3/graphql";\n')
        assert scan(probe) == ["core/src/foreign.rs:1"]
        print("protocol config selftest: forbidden foreign file is red")
        path.write_text('const PATH: &str = "/api/graphql";\n')
        assert scan(probe) == []
        print("protocol config selftest: neutral foreign file is green")
    sys.exit(0)

findings = scan(root)
if findings:
    print("protocol config check: forbidden server values\n" + "\n".join(findings))
    sys.exit(1)
print("protocol config check: clean")
PY

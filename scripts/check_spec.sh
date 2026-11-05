#!/bin/bash
# Checks the machine specs: specs/machines/<owner>.md (table format: specs/machines/design.md section 4).
# Exit 1 on: an empty cell, a cell or property without a test name in the Test column, a named test that does not exist
# in the code, a bare "не бывает" cell, a banned word (etc, usually, if possible, и т.п., обычно, по возможности).
# SPEC_DIR (default specs/machines) and CODE_DIR (default the repository) can be overridden. Mode `selftest` checks the checker on stubs.
set -uo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"

selftest() {
  local tmp rc=0 header='| Состояние | Вход | Условие | Следующее | Наблюдаемое (API) | Эффекты | Test |'
  tmp="$(mktemp -d)" || return 2
  mkdir -p "$tmp/code/core" "$tmp/clean" "$tmp/bad"
  printf 'fn real_test() {}\nfunc swiftTest() {}\nfun kotlinTest() {}\n' > "$tmp/code/core/stub.rs"
  local table="$header
| --- | --- | --- | --- | --- | --- | --- |"
  local props='| Свойство | Формула | Test |
| --- | --- | --- |'
  write() { printf '# Owner\n\n%s\n|%s|\n\n%s\n|%s|\n' "$table" "$1" "$props" "$2" > "$3"; }
  write ' Idle | Open | always | Open | opened | none | `real_test` ' ' P-OWN-01 | all delivered | `swiftTest` ' "$tmp/clean/owner.md"
  SPEC_DIR="$tmp/clean" CODE_DIR="$tmp/code" bash "${BASH_SOURCE[0]}" > /dev/null 2>&1 || { echo "selftest FAIL: a clean spec was rejected"; rc=1; }
  local line rest name row prop
  while IFS= read -r line; do
    name="${line%%@@*}"; rest="${line#*@@}"; row="${rest%%@@*}"; prop="${rest#*@@}"
    rm -f "$tmp/bad"/*.md
    write "$row" "$prop" "$tmp/bad/owner.md"
    if SPEC_DIR="$tmp/bad" CODE_DIR="$tmp/code" bash "${BASH_SOURCE[0]}" > /dev/null 2>&1; then
      echo "selftest FAIL: $name was accepted"; rc=1
    fi
  done <<'CASES'
empty cell@@ Idle | Open |  | Open | opened | none | real_test @@ P-OWN-01 | all | swiftTest
empty test@@ Idle | Open | always | Open | opened | none |  @@ P-OWN-01 | all | swiftTest
missing test@@ Idle | Open | always | Open | opened | none | no_such_test @@ P-OWN-01 | all | swiftTest
bare impossible@@ Idle | Open | always | не бывает | opened | none | real_test @@ P-OWN-01 | all | swiftTest
banned word@@ Idle | Open | usually | Open | opened | none | real_test @@ P-OWN-01 | all | swiftTest
property without test@@ Idle | Open | always | Open | opened | none | real_test @@ P-OWN-01 | all |
property missing test@@ Idle | Open | always | Open | opened | none | real_test @@ P-OWN-01 | all | no_such_test
CASES
  rm -rf "$tmp"
  [ "$rc" -eq 0 ] && echo "check_spec selftest: ok"
  return "$rc"
}

if [ "${1:-}" = "selftest" ]; then
  selftest
  exit $?
fi

spec_dir="${SPEC_DIR:-$root/specs/machines}"
code_dir="${CODE_DIR:-$root}"
if [ ! -d "$spec_dir" ]; then
  echo "check_spec: $spec_dir is absent (specs are ignored by git), nothing to check"
  exit 0
fi

exec python3 -I - "$spec_dir" "$code_dir" <<'PY'
import os, re, sys

spec_dir, code_dir = sys.argv[1:3]
skip_files = {"design.md", "corpus.md"}
banned = re.compile(r"\betc\b|\busually\b|if possible|и т\.п\.|обычно|по возможности", re.IGNORECASE)

code = []
for base, dirs, files in os.walk(code_dir):
    dirs[:] = [d for d in dirs if not d.startswith("target") and d not in {".build", "build", "node_modules", "vendor", ".git", "worktrees"}]
    for name in files:
        if name.endswith((".rs", ".swift", ".kt")):
            with open(os.path.join(base, name), encoding="utf-8", errors="replace") as handle:
                code.append(handle.read())
code_text = "\n".join(code)

def test_exists(name):
    return re.search(r"\b(fn|func|fun)\s+`?" + re.escape(name) + r"\b", code_text) is not None

def cells(line):
    return [c.strip() for c in line.strip().strip("|").split("|")]

errors = []
for name in sorted(os.listdir(spec_dir)):
    if not name.endswith(".md") or name in skip_files:
        continue
    path = os.path.join(spec_dir, name)
    lines = open(path, encoding="utf-8").read().split("\n")
    for number, line in enumerate(lines, 1):
        if banned.search(line):
            errors.append(f"{name}:{number}: banned word")
    index = 0
    while index < len(lines):
        line = lines[index]
        header = cells(line)[0] if line.startswith("|") else ""
        if header not in ("Состояние", "Свойство"):
            index += 1
            continue
        width = 7 if header == "Состояние" else 3
        if header == "Состояние" and cells(line)[-1] != "Test":
            index += 1
            continue
        if len(cells(line)) != width or cells(line)[-1] != "Test":
            errors.append(f"{name}:{index + 1}: the table needs {width} columns ending with Test")
        index += 2
        while index < len(lines) and lines[index].startswith("|"):
            row = cells(lines[index])
            where = f"{name}:{index + 1}"
            if len(row) != width:
                errors.append(f"{where}: {len(row)} cells, need {width}")
            for position, cell in enumerate(row):
                if cell == "":
                    errors.append(f"{where}: empty cell {position + 1}")
                elif re.fullmatch(r"не бывает[.]?", cell.lower()):
                    errors.append(f"{where}: 'не бывает' needs the type that forbids the input")
            if header == "Свойство" and row and not re.fullmatch(r"P-[A-Z]+-\d\d", row[0]):
                errors.append(f"{where}: property name must look like P-<OWNER>-NN")
            if row and row[-1]:
                for item in row[-1].split(","):
                    test = re.split(r"::|\.", item.strip().strip("`").replace("()", ""))[-1].strip("`")
                    if not test_exists(test):
                        errors.append(f"{where}: test '{test}' is not in the code")
            index += 1

for error in errors:
    print(error)
sys.exit(1 if errors else 0)
PY

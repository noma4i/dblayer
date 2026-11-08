#!/usr/bin/env bash
# Self test of check_gen_decisions.sh: a new filter_map exits 1, a listed one and a clean tree exit 0, a stale or non-norm line exits 1.
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
work="$(mktemp -d)"
trap '/bin/rm -rf "$work"' EXIT
mkdir -p "$work/gen/src/decl/lower"
: > "$work/allow.txt"
file="$work/gen/src/decl/lower/a.rs"

run() {
  CHECK_GEN_ROOT="$work" CHECK_GEN_ALLOW="$work/allow.txt" bash "$root/scripts/check_gen_decisions.sh" >/dev/null 2>&1
}

printf 'fn a() -> u8 { 1 }\n' > "$file"
run || { echo "check_gen_decisions_test: a clean tree failed" >&2; exit 1; }
printf '    let x = faults.iter().filter_map(|f| find(f)).collect();\n' > "$file"
if run; then echo "check_gen_decisions_test: a new filter_map passed" >&2; exit 1; fi
printf 'gen/src/decl/lower/a.rs\tlet x = faults.iter().filter_map(|f| find(f)).collect();\tM55\n' > "$work/allow.txt"
run || { echo "check_gen_decisions_test: a listed line failed" >&2; exit 1; }
printf 'gen/src/decl/lower/a.rs\tlet x = faults.iter().filter_map(|f| find(f)).collect();\tbecause\n' > "$work/allow.txt"
if run; then echo "check_gen_decisions_test: a non-norm ID passed" >&2; exit 1; fi
printf 'gen/src/decl/lower/a.rs\tlet x = faults.iter().filter_map(|f| find(f)).collect();\tM55\n' > "$work/allow.txt"
printf 'fn a() -> u8 { 1 }\n' > "$file"
if run; then echo "check_gen_decisions_test: a stale line passed" >&2; exit 1; fi
echo "check_gen_decisions_test: ok"

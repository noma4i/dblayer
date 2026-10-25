#!/usr/bin/env bash
# Self test of check_silent.sh: a new try? or runCatching {...}.getOrNull() exits 1, a listed one and a clean tree exit 0, a stale list line exits 1.
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
work="$(mktemp -d)"
trap '/bin/rm -rf "$work"' EXIT
mkdir -p "$work/swift/Sources/DBlayer" "$work/kotlin/src/main/kotlin"
: > "$work/allow.txt"

run() {
  CHECK_SILENT_ROOT="$work" CHECK_SILENT_ALLOW="$work/allow.txt" bash "$root/scripts/check_silent.sh" >/dev/null 2>&1
}

swift="$work/swift/Sources/DBlayer/Clean.swift"
kotlin="$work/kotlin/src/main/kotlin/Clean.kt"
printf 'func a() throws { try b() }\n' > "$swift"
printf 'fun a() = runCatching { b() }.getOrThrow()\n' > "$kotlin"
run || { echo "check_silent_test: a clean tree failed" >&2; exit 1; }

expect_red() {
  local file="$1" text="$2"
  printf '%s\n' "$text" > "$file"
  if run; then
    echo "check_silent_test: not caught: $text" >&2
    exit 1
  fi
}

expect_red "$swift" '  let row = try? DB.database.peekRow(model: m, rowId: id)'
printf 'func a() throws { try b() }\n' > "$swift"
expect_red "$kotlin" '    val row = runCatching { decoder.read(wire) }.getOrNull()'
expect_red "$kotlin" $'    val row = runCatching { decoder.read(wire) }\n        .onFailure { count++ }\n        .getOrNull()'

printf '%s\t%s\t%s\n' "kotlin/src/main/kotlin/Clean.kt" 'val row = runCatching { decoder.read(wire) }' 'reason' > "$work/allow.txt"
expect_red "$kotlin" 'fun a() = 1'
printf 'fun a() = runCatching { b() }.getOrThrow()\n' > "$kotlin"
if run; then echo "check_silent_test: a stale list line passed" >&2; exit 1; fi

printf '%s\t%s\t%s\n' "kotlin/src/main/kotlin/Clean.kt" 'val row = runCatching { decoder.read(wire) }' 'reason' > "$work/allow.txt"
printf '    val row = runCatching { decoder.read(wire) }.getOrNull()\n' > "$kotlin"
printf '%s\t%s\t%s\n' "kotlin/src/main/kotlin/Clean.kt" 'val row = runCatching { decoder.read(wire) }.getOrNull()' 'reason' > "$work/allow.txt"
run || { echo "check_silent_test: a listed line failed" >&2; exit 1; }
echo "check_silent_test: ok"

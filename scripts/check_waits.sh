#!/usr/bin/env bash
# INV-WAIT-04: a test never waits without a bound or pauses for a time. Swift waits through eventually and Cursor (Waiting.swift), Kotlin through bounded and eventually (TestWaiting.kt), Rust through within, spin_until and yield_until (test_wait.rs); only those three files may poll.
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
swift_dir="${CHECK_WAITS_SWIFT:-$root/swift/Tests}"
kotlin_dir="${CHECK_WAITS_KOTLIN:-$root/kotlin/src/test}"
rust_dirs=("${CHECK_WAITS_RUST:-$root/core}")
[ -n "${CHECK_WAITS_RUST:-}" ] || rust_dirs+=("$root/gen")
found=0

report() {
  local rule="$1" hits="$2"
  if [ -n "$hits" ]; then
    echo "check_waits: $rule" >&2
    echo "$hits" >&2
    found=1
  fi
}

swift_hits() {
  rg -n --no-heading -g '*.swift' -g '!Waiting.swift' "$1" "$swift_dir" || true
}

kotlin_hits() {
  rg -n --no-heading -g '*.kt' -g '!TestWaiting.kt' "$1" "$kotlin_dir" || true
}

report "swift: a while loop in a test (use eventually or Cursor)" "$(swift_hits '^\s*while\b')"
report "swift: a hand-written poll (use eventually)" "$(swift_hits 'for _ in 0 ?\.\.< ?\w+ where')"
report "swift: an unbounded stream read (use Cursor)" "$(swift_hits 'makeAsyncIterator\(\)|\bfor await\b')"
report "kotlin: runBlocking without a bound (use bounded)" "$(kotlin_hits '\brunBlocking\b')"
report "kotlin: a while loop with a delay or a sleep (use eventually)" "$(kotlin_hits '\bwhile \(.*\bdelay\(|Thread\.sleep.*while|while.*Thread\.sleep')"
report "swift: a pause in a test (wait for the event with eventually, or move the clock)" "$(swift_hits 'Task\.sleep|Thread\.sleep|\busleep\(|\bsleep\(')"
report "kotlin: a pause in a test (wait for the event with eventually, or move the clock)" "$(kotlin_hits '\bdelay\(|Thread\.sleep')"
report "rust: a wait in a test without a bound (use within, spin_until or yield_until)" "$(python3 -I "$root/scripts/check_waits_rust.py" "${rust_dirs[@]}" || true)"

if [ "$found" -eq 1 ]; then
  exit 1
fi
echo "check_waits: ok"

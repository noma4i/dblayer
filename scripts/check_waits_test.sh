#!/usr/bin/env bash
# Self test of check_waits.sh: each forbidden wait makes it exit 1, a clean tree makes it exit 0.
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
work="$(mktemp -d)"
trap '/bin/rm -rf "$work"' EXIT
mkdir -p "$work/swift" "$work/kotlin"

run() {
  CHECK_WAITS_SWIFT="$work/swift" CHECK_WAITS_KOTLIN="$work/kotlin" bash "$root/scripts/check_waits.sh" >/dev/null 2>&1
}

printf 'func a() async { await eventually("x") { true } }\n' > "$work/swift/Clean.swift"
printf 'fun a() = bounded { 1 }\n' > "$work/kotlin/Clean.kt"
run || { echo "check_waits_test: a clean tree failed" >&2; exit 1; }

expect_red() {
  local file="$1" text="$2"
  printf '%s\n' "$text" > "$file"
  if run; then
    echo "check_waits_test: not caught: $text" >&2
    exit 1
  fi
  /bin/rm -f "$file"
}

expect_red "$work/swift/Bad.swift" '  while list.replaced.isEmpty { await Task.yield() }'
expect_red "$work/swift/Bad.swift" '  for _ in 0 ..< 200 where entity.value == nil { try await Task.sleep(for: .milliseconds(10)) }'
expect_red "$work/swift/Bad.swift" '  var changes = await live.stream().makeAsyncIterator()'
expect_red "$work/swift/Bad.swift" '  for await frame in stream { }'
expect_red "$work/kotlin/Bad.kt" '    fun a() = runBlocking { 1 }'
expect_red "$work/kotlin/Bad.kt" '    withTimeout(5000) { while (x.size < 2) delay(10) }'
expect_red "$work/swift/Bad.swift" '  try await Task.sleep(for: .milliseconds(200))'
expect_red "$work/swift/Bad.swift" '  Thread.sleep(forTimeInterval: 0.1)'
expect_red "$work/kotlin/Bad.kt" '    delay(300)'
expect_red "$work/kotlin/Bad.kt" '    Thread.sleep(100)'
echo "check_waits_test: ok"

#!/usr/bin/env bash
# Self test of check_waits.sh: each forbidden wait makes it exit 1, a clean tree makes it exit 0.
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
work="$(mktemp -d)"
trap '/bin/rm -rf "$work"' EXIT
mkdir -p "$work/swift" "$work/kotlin" "$work/rust"

run() {
  CHECK_WAITS_SWIFT="$work/swift" CHECK_WAITS_KOTLIN="$work/kotlin" CHECK_WAITS_RUST="$work/rust" bash "$root/scripts/check_waits.sh" >/dev/null 2>&1
}

printf 'func a() async { await eventually("x") { true } }\n' > "$work/swift/Clean.swift"
printf 'fun a() = bounded { 1 }\n' > "$work/kotlin/Clean.kt"
printf 'async fn a() {\n    let event = within(rx.recv()).await;\n    spin_until("ready", || ready());\n    let deadline = Instant::now() + WAIT;\n    while !ready() && Instant::now() < deadline {\n        std::thread::yield_now();\n    }\n    assert!(rx.recv_timeout(WAIT).is_ok());\n}\n' > "$work/rust/clean_tests.rs"
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
expect_red "$work/rust/bad_tests.rs" '    let event = subscription.next().await;'
expect_red "$work/rust/bad_tests.rs" '    transport.started.notified().await;'
expect_red "$work/rust/bad_tests.rs" '    while let Some(done) = tasks.join_next().await { }'
expect_red "$work/rust/bad_tests.rs" '    while live() != Some(1) { tokio::task::yield_now().await; }'
expect_red "$work/rust/bad_tests.rs" $'    while !ready() {\n        std::thread::sleep(Duration::from_millis(10));\n    }'
expect_red "$work/rust/bad_tests.rs" '    assert!(rx.recv_timeout(WAIT).is_err());'
echo "check_waits_test: ok"

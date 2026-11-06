#!/usr/bin/env bash
# Self test of check_zoo.sh rules Z20 and Z21: each bypass makes it exit 1, the allowed forms exit 0.
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
work="$(mktemp -d)"
trap '/bin/rm -rf "$work"' EXIT
mkdir -p "$work/core/src" "$work/gen" "$work/scripts"
: > "$work/core/Cargo.toml"
: > "$work/gen/Cargo.toml"

run() {
  CHECK_ZOO_ROOT="$work" bash "$root/scripts/check_zoo.sh" >/dev/null 2>&1
}

probe="$work/core/src/probe.rs"

expect_green() {
  printf '%s\n' "$1" > "$probe"
  run || { echo "check_zoo_test: a clean source failed: $1" >&2; exit 1; }
}

expect_red() {
  printf '%s\n' "$1" > "$probe"
  if run; then
    echo "check_zoo_test: not caught: $1" >&2
    exit 1
  fi
}

expect_green 'fn read(inner: &Inner) { let count = inner.connection.query_row("SELECT 1"); }
fn guarded(store: &Store) { store.write(&epoch, |connection| { connection.execute("DELETE FROM t", []) }); }
fn waits(rx: &mut Rx) { let event = rx.recv().await; let unit = blocking_unit(|| condition.wait(guard)); }'

expect_red 'fn bypass(inner: &Inner) { inner.connection.execute("DELETE FROM t", []); }'
expect_red 'fn bypass(inner: &Inner) { let connection = &inner.connection; connection.execute("DELETE FROM t", []); }'
expect_red 'fn prune(connection: &Connection) { connection.execute("DELETE FROM t", []); }
fn bypass(inner: &Inner) { prune(&inner.connection); }'
expect_red 'fn prune(now: i64, connection: &Connection) { connection.execute("DELETE FROM t", []); }
fn bypass(inner: &Inner) { prune(5, &inner.connection); }'
expect_red 'fn bypass(inner: &Inner) { let first = &inner.connection; let second = first; second.execute("DELETE FROM t", []); }'
expect_red 'fn bypass(store: &Store, inner: &Inner) { inner.connection.execute("DELETE FROM t", []); store.write(&epoch, |connection| { connection.execute("DELETE FROM u", []) }); }'
expect_red 'fn bypass(store: &Store) { store.read_locked(|connection| { connection.execute("DELETE FROM t", []) }); }'

expect_red 'fn parked(condition: &Condvar, guard: Guard) { condition.wait(guard); }'
expect_red 'fn parked(condition: &Condvar, guard: Guard) { blocking_unit(|| 1); condition.wait(guard); }'
expect_red '#[cfg(test)]
fn probe() {}
fn parked(rx: &Receiver) { rx.recv(); }'

expect_green 'fn go(transport: &T, gate: &G) { send_in_epoch(gate, Some(epoch), || transport.query(request)); }'
expect_red 'fn go(transport: &T, gate: &G) { send_in_epoch(gate, Some(epoch), transport.query(request)); }'
expect_red 'fn go(transport: &T) { transport.query(request); }'
expect_red 'fn go(transport: &T, gate: &G) { transport.query(request); send_in_epoch(gate, Some(epoch), other()); }'
expect_red 'fn go(document: &str) { if document.starts_with("mutation") { send(); } }'
expect_green 'fn tick(clock: &Clock) { clock.now(); }'
expect_red 'fn tick() { let at = std::time::Instant::now(); }'

mkdir -p "$work/core/src/owners" "$work/core/src/ffi"
: > "$probe"
printf 'struct Owner { state: Mutex<u8> }\n' > "$work/core/src/owners/probe.rs"
if run; then echo "check_zoo_test: a lock in an owner was not caught" >&2; exit 1; fi
printf 'struct Owner { state: u8 }\n' > "$work/core/src/owners/probe.rs"
run || { echo "check_zoo_test: a clean owner failed" >&2; exit 1; }
printf 'impl Database { pub async fn create(&self, values: String) { let v = serde_json::from_str(&values); } }\n' > "$work/core/src/ffi/probe.rs"
if run; then echo "check_zoo_test: JSON parsed on the caller thread was not caught" >&2; exit 1; fi
printf 'impl Database { pub async fn create(&self, values: String) { self.owner.hand(values); } }\n' > "$work/core/src/ffi/probe.rs"
run || { echo "check_zoo_test: a clean public call failed" >&2; exit 1; }
/bin/rm -f "$work/core/src/owners/probe.rs" "$work/core/src/ffi/probe.rs"

mkdir -p "$work/core/src/zoo_probe"
printf '#[cfg(test)]\nmod zoo_probe;\n' > "$work/core/src/lib.rs"
printf 'fn fixture() { Some(1).unwrap(); }\n' > "$work/core/src/zoo_probe/harness.rs"
: > "$probe"
run || { echo "check_zoo_test: a cfg(test) module was scanned" >&2; exit 1; }
printf 'mod zoo_probe;\n' > "$work/core/src/lib.rs"
if run; then
  echo "check_zoo_test: an unwrap in a non-test module was not caught" >&2
  exit 1
fi
echo "check_zoo_test: ok"

#!/usr/bin/env bash
# Self test of check_zoo.sh rules Z18, Z19, Z20 and Z21: each bypass makes it exit 1, the allowed forms exit 0.
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

expect_green 'fn read(launch: Option<Launch>) { let job = crate::task::spawn_blocking_on(&handle, launch, move || work()); let own = crate::task::spawn_blocking(crate::runtime::running_launch(), move || work()); }'
expect_red 'fn read() { let job = crate::task::spawn_blocking(None, move || work()); }'
expect_red 'fn read() { let job = crate::task::spawn_blocking_on(&handle, None, move || work()); }'

expect_green '#[uniffi::export(with_foreign)]
pub trait Sink {}
#[derive(Default, uniffi::Record)]
pub struct Ports { pub sink: Option<Arc<dyn Sink>> }'
expect_red '#[uniffi::export(with_foreign)]
pub trait Sink {}
pub struct Holder { pub sink: Option<Arc<dyn Sink>> }'

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

expect_red '#[allow(clippy::too_many_arguments)] fn many(a: u8) {}'
expect_red '#![allow(unexpected_cfgs)]'

swift_probe="$work/swift/Sources/DBlayer/Probe.swift"
kotlin_probe="$work/kotlin/src/main/kotlin/com/noma4i/dblayer/Probe.kt"
mkdir -p "$(dirname "$swift_probe")" "$(dirname "$kotlin_probe")"
: > "$probe"
expect_wrapper() {
  local file="$1" verdict="$2" text="$3"
  printf '%s\n' "$text" > "$file"
  if [ "$verdict" = red ]; then
    if run; then echo "check_zoo_test: wrapper state not caught: $text" >&2; exit 1; fi
  else
    run || { echo "check_zoo_test: a clean wrapper failed: $text" >&2; exit 1; }
  fi
  /bin/rm -f "$file"
}
expect_wrapper "$swift_probe" green 'protocol Named { static var name: String { get } }
enum Slot { static var computed: Int { 1 } }'
expect_wrapper "$swift_probe" red 'enum Slot { private static var slot: Database? }'
expect_wrapper "$swift_probe" red 'enum Slot { nonisolated(unsafe) static var held: [String: Int] = [:] }'
expect_wrapper "$swift_probe" red 'enum Slot { static let registry = [String: Int]() }
enum Other { static let table = [1: 2] }'
expect_wrapper "$kotlin_probe" green 'object Names {
    val fixed = 1
    fun run() { var local = 0; local += 1 }
}'
expect_wrapper "$kotlin_probe" red 'object Hook {
    var delivered: (() -> Unit)? = null
}'
expect_wrapper "$kotlin_probe" red 'class Holder { @Volatile private var live: Int? = null }'

expect_green 'async fn waits(signal: &Notify) { signalled(signal, "arrived", || ready()).await; settled(&rig, "shown", || shown()).await; }'
expect_red 'async fn waits() { while !ready() { tokio::task::yield_now().await; } }'
expect_red 'async fn waits() { yield_until("shown", || shown()).await; }'
expect_red 'fn waits() { spin_until("shown", limit, || shown()); }'
expect_red 'fn waits() { loop { if ready() { break; } std::thread::sleep(step); } }'
mkdir -p "$work/core/src/ffi/selftest"
printf 'fn land() { let envelope = BatchPlan::new(generation); }\n' > "$work/core/src/ffi/selftest/stand_tests.rs"
: > "$probe"
run || { echo "check_zoo_test: a clean public stand failed" >&2; exit 1; }
for bypass in 'let envelope = BatchPlan::from_server(generation);' 'life.commit_for_tests(batch);' 'life.stale_for_tests("Moment");' 'wiring.to_store(StoreInput::Commit(batch));'; do
  printf 'fn land() { %s }\n' "$bypass" > "$work/core/src/ffi/selftest/stand_tests.rs"
  if run; then echo "check_zoo_test: Z35 did not catch: $bypass" >&2; exit 1; fi
done
/bin/rm -f "$work/core/src/ffi/selftest/stand_tests.rs"
mkdir -p "$work/core/src/spec"
printf '#[cfg(test)]\nmod spec;\n' > "$work/core/src/lib.rs"
printf 'fn fixture() { Some(1).unwrap(); }\n' > "$work/core/src/spec/harness.rs"
: > "$probe"
run || { echo "check_zoo_test: a cfg(test) module was scanned" >&2; exit 1; }
printf 'mod spec;\n' > "$work/core/src/lib.rs"
if run; then
  echo "check_zoo_test: an unwrap in a non-test module was not caught" >&2
  exit 1
fi
at_red() {
  local file="$work/$1"
  mkdir -p "$(dirname "$file")"
  printf '%s\n' "$2" > "$file"
  if run; then echo "check_zoo_test: not caught in $1: $2" >&2; exit 1; fi
  /bin/rm -f "$file"
}
at_green() {
  local file="$work/$1"
  mkdir -p "$(dirname "$file")"
  printf '%s\n' "$2" > "$file"
  run || { echo "check_zoo_test: a clean source failed in $1: $2" >&2; CHECK_ZOO_ROOT="$work" bash "$root/scripts/check_zoo.sh" >&2 || true; exit 1; }
  /bin/rm -f "$file"
}
: > "$probe"
printf 'mod spec;\n' > "$work/core/src/lib.rs"

mkdir -p "$work/core/src/owners"
printf 'pub struct Twin;\n' > "$work/core/src/owners/first.rs"
/bin/rm -rf "$work/core/src/spec" "$work/core/src/lib.rs"
at_red core/src/owners/second.rs 'pub struct Twin;'
/bin/rm -f "$work/core/src/owners/first.rs"
at_red core/src/owners/merge_user.rs 'fn go(spec: &Spec) { spec.merged(None, &row); }'
at_red core/src/owners/swap_user.rs 'fn go() { let reason = DestroyReason::Replaced { by: id }; }'
at_red core/src/transport/watcher.rs 'fn go(connectivity: &Connectivity) {}'
at_green core/src/transport/retry.rs 'fn go(connectivity: &Connectivity) {}'
at_red core/src/transport/request.rs 'fn go(transport: &T) { transport.query(request); }'
at_red core/src/ffi/upload.rs 'fn go(db: &Db) { db.plan_update(model, row, values, args); }'
at_green core/src/ffi/write.rs 'fn go(db: &Db) { db.plan_update(model, row, values, args); }'
at_red core/src/owners/body.rs 'fn make() { let body = graphql_body(&request, true); }'
at_green core/src/owners/life/calls_runner.rs 'fn make() { let body = graphql_body(&request, true); }'
at_red core/src/owners/hash.rs 'fn make() { let body = hash_only(text); }'
at_red core/src/ffi/selftest/own_pair_tests.rs 'impl FakeHttp for Own { fn rule(&self) -> bool { sequence == last + 1 } }'
at_red core/src/ffi/selftest/own_code_tests.rs 'impl FakeTransport for Own { fn code(&self) -> &str { "SEQUENCE_CONFLICT" } }'
at_red core/src/owners/contract_user.rs 'use crate::server_contract::Contract;'
at_green core/src/ffi/contract.rs 'use crate::server_contract::Contract;'
at_red core/src/owners/effect.rs 'fn go(tx: &Tx) { tx.send(1); }'
at_red core/src/owners/spawned.rs 'fn go() { tokio::spawn(async {}); }'
at_red gen/src/probe.rs '#[allow(dead_code)] fn unused() {}'
at_green gen/src/probe.rs 'fn used() {}'

mkdir -p "$work/scripts"
printf 'x = ("owners/exists.rs", "owners/gone.rs")\n' > "$work/scripts/check_zoo.sh"
printf 'fn exists() {}\n' > "$work/core/src/owners/exists.rs"
if run; then echo "check_zoo_test: Z39 did not catch a rule that names a missing file" >&2; exit 1; fi
printf 'x = ("owners/exists.rs",)\n' > "$work/scripts/check_zoo.sh"
run || { echo "check_zoo_test: Z39 refused rules whose files exist" >&2; exit 1; }
/bin/rm -f "$work/scripts/check_zoo.sh" "$work/core/src/owners/exists.rs"

printf 'z18:kotlin/src/main/kotlin/com/noma4i/dblayer/Probe.kt:@Volatile private var live\n' > "$work/scripts/zoo_allowlist.txt"
mkdir -p "$work/kotlin/src/main/kotlin/com/noma4i/dblayer"
printf 'class Holder { @Volatile private var live: Int? = null }\nclass Other { @Volatile private var other: Int? = null }\n' > "$work/kotlin/src/main/kotlin/com/noma4i/dblayer/Probe.kt"
if run; then echo "check_zoo_test: a line allow of Z18 covered a second line of the file" >&2; exit 1; fi
printf 'class Holder { @Volatile private var live: Int? = null }\n' > "$work/kotlin/src/main/kotlin/com/noma4i/dblayer/Probe.kt"
run || { echo "check_zoo_test: the line allow of Z18 did not hold" >&2; exit 1; }
/bin/rm -f "$work/scripts/zoo_allowlist.txt" "$work/kotlin/src/main/kotlin/com/noma4i/dblayer/Probe.kt"
printf 'pub fn set_clock(&self) {}\n' > "$work/core/src/owners/clock_one.rs"
at_red core/src/owners/clock_two.rs 'pub fn set_clock(&self) {}'
/bin/rm -f "$work/core/src/owners/clock_one.rs"
printf 'pub fn alone() {}\n' > "$work/core/src/owners/support.rs"
at_red core/src/ffi/support.rs 'pub fn second() {}'
/bin/rm -f "$work/core/src/owners/support.rs"
printf 'pub struct EpochCounters;\n' > "$work/core/src/owners/counters_one.rs"
at_red core/src/owners/counters_two.rs 'pub struct WindowStats;'
/bin/rm -f "$work/core/src/owners/counters_one.rs"
printf '[features]\ntest-support = []\n' > "$work/core/Cargo.toml"
if run; then echo "check_zoo_test: Z22 did not catch the test-support feature" >&2; exit 1; fi
: > "$work/core/Cargo.toml"
at_red core/src/owners/gate.rs '#[cfg(feature = "test-support")] fn open() {}'
at_red core/src/owners/mod.rs '#[cfg(test)]
mod helpers;'
at_red core/src/owners/stand.rs '#[path = "x.rs"]
mod stand;'
for wrapper in 'swift/Sources/DBlayer/Probe.swift|func again() { sleep(1) }' 'kotlin/src/main/kotlin/com/noma4i/dblayer/Probe.kt|class A { val x = CableClient() }' 'kotlin/src/main/kotlin/com/noma4i/dblayer/Probe.kt|val s = flow.buffer(Channel.UNLIMITED)' 'swift/Sources/DBlayer/Probe.swift|let name = "Yupi"'; do
  at_red "${wrapper%%|*}" "${wrapper#*|}"
done
at_green swift/Sources/DBlayer/Probe.swift 'func fine() -> Int { 1 }'
echo "check_zoo_test: ok"

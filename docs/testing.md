# Testing

## In your app

Open a database in memory or in a temporary directory and give it a mock transport. `DB.close()` is synchronous and runs on the main actor, so a test off the main actor writes `await DB.close()`. The mock replies by operation name, can delay or fail replies, and can break a subscription.

```swift
import DBlayerTesting

let transport = MockTransport()
transport.enqueue("MomentsFeed", reply: feedJSON)
await DB.close()
try await DB.open(transport: transport, modelsJSON: modelsJSON)
```

```kotlin
val transport = MockTransport()
transport.enqueue("MomentsFeed", feedJson)
DBlayer.close()
DBlayer.open(context, transport = transport, modelsJson = modelsJSON)
```

A call with no queued reply fails at once with `Invalid` that names the operation and its variables, and the request is listed in `transport.unscripted`. The test that caused the call fails too, without any check in the test. Swift records a Swift Testing issue on the test that created the mock, so create the mock inside the test (or its suite initializer). Kotlin cannot throw on the core thread, so the next touch of any mock on a test thread throws `AssertionError` with the operation, the variables and the place that created the mock: `MockTransport()`, `enqueue`, `emit`, `breakSubscription`, `calls`, `cableFrames` and `unsubscribe`. A call that is still unreported when the JVM exits stops it with exit code 1. A test that expects such a call wraps it in `withKnownIssue` (Swift) or reads `transport.claimUnscripted()` (Kotlin). Pass `Unanswered.NetworkError` (Kotlin) or `unanswered: .networkError` (Swift) to the constructor when a test needs a missing reply to be a network error, which the core retries. `unsubscribe` with a token the mock never issued is recorded in `unknownTokens` and does not throw.

`transport.calls` lists the requests it received. `emit` pushes a subscription event and `breakSubscription` drops the subscription.

Keep native observer handles alive until every asynchronous assertion finishes. In Kotlin, enclose direct `liveEntities` tests in `use` and test delivery after `System.gc()`.
JVM locals can become unreachable after their last use, even while the enclosing test still awaits callbacks.

A live query loads by itself when something subscribes, and an explicit refresh can add a second request. Queue 2 or 3 replies per operation and assert the content of `transport.calls`, not their exact count.

Move time with a test clock instead of sleeping. `DB.openTesting` (Swift, `DBlayerTesting`) and `DBlayer.openTesting` (Kotlin) open the database on a clock that only `advance` moves, and return it. The clock drives `fresh_for`, load retries and ephemeral expiry. The production `open` has no clock parameter. In Kotlin, `openTesting` and `TestClock.advance` need `@OptIn(DBlayerTestingApi::class)` on the calling declaration.

```swift
let clock = try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
try await Moment.feed.load()
clock.advance(by: 5 * 60 + 1)   // 1 second past freshness
try await Moment.feed.load()    // requests again
```

```kotlin
import com.noma4i.dblayer.DBlayerTestingApi

@OptIn(DBlayerTestingApi::class)
suspend fun feedLoadsAgainAfterFreshness(transport: Transport) {
    val clock = DBlayer.openTesting(transport, modelsJSON)
    Moment.feed.load()
    clock.advance(5 * 60_000L + 1_000)
    Moment.feed.load()
}
```

Typical scenarios to cover:

- first page, next page, refresh
- fresh data is not requested again, parallel loads become one request
- a failed load is retried, a fatal failure is reported
- a mutation that gets no answer becomes `deliveryUnknown` and is sent again only by `retry()`
- optimistic create is replaced by the server row, refusal rolls it back
- a subscription event changes a row and your live result updates
- a dropped subscription reconnects and the relation is refetched

## In this repository

| Check | Command |
| ----- | ------- |
| Rust core and generator | `cargo test` |
| Everything, including Swift and Kotlin | `bash scripts/check_dblayer.sh` |
| Android R8 release and optimized JVM FFI calls | `gradle -p kotlin/r8-check probe` |
| Touched directories only | `bash scripts/check_dblayer.sh --fast` |
| Every invariant has a test with an assertion | `bash scripts/check_invariants.sh` |
| Check scripts leave unrelated processes alone | `bash scripts/check_bounded.sh --selftest` |
| A second check run in the same tree is refused (exit 3), a lock of a dead run is taken over | `bash scripts/check_lock_test.sh` |
| Duplicate mechanisms and names | `bash scripts/check_zoo.sh` |
| Docs reference real paths | `bash scripts/check_docs.sh` |
| No test waits without a bound or pauses for a time (`Task.sleep`, `Thread.sleep`, `delay(`) outside the wait helpers | `bash scripts/check_waits.sh` |
| No code swallows an error with `try?` or `runCatching {...}.getOrNull()` outside `scripts/silent_allowlist.txt` | `bash scripts/check_silent.sh` |
| No cable line that no test runs | `bash scripts/check_coverage.sh` (after the swift and gradle test lanes with coverage) |

The coverage reports cost time: the Swift lane with coverage takes about the same as without, the Kotlin lane roughly doubles because of the coverage report task. The rule covers the cable code only. The Swift lane counts with atomic profile counters, because plain counters lose increments when suites run in parallel and report an executed line as never run.

The R8 check uses the generated Kotlin bindings. Run `bash scripts/build_kotlin.sh --host` first when running it separately.
It builds a minimal Android release application with minification enabled and the package's consumer rules.
It also runs optimized JVM bytecode against the host Rust library, checking structures, callbacks, records, enums and errors.
Both mappings must rename `Config`, which rejects a package-wide keep rule.
Use `-Prules=/absolute/path/to/rules.pro` to check a changed rule file.
Android device execution remains a separate check.
See [Android R8 checks](android-r8.md) for the rules, mutation results and measured DEX sizes.
The [release profile and keep-rules report](release-profile-keep-report.md) records the combined checks and artifact sizes.

Test conventions:

- A test that needs a fresh database calls `DB.close()` (Swift) or `DBlayer.close()` (Kotlin) first, then opens. A second `open` without `close` fails. `DB.openTesting` and `DBlayer.openTesting` follow the same rule. Swift tests that open the shared database, including calls through `DB.open`, belong to the serialized `BridgeProbeTests` suite. Nest related suites there. Separate serialized suites still run concurrently and can replace each other's database during an awaited call.
- Kotlin tests that open the shared database run sequentially with JUnit 4. Keep that shared state sequential if changing the test runner. Tests using independent database handles can run concurrently on either platform.
- Before advancing manual cable clocks, await the registered deadline in Swift and Kotlin. A received frame's timestamp does not prove its asynchronous sleep has registered. A closed socket also precedes the connection actor's completion and its reconnect sleep registration. Read the actor's closed state, then await that sleep. After a reopen delay, await the new socket rather than a fixed settling interval. For terminal closure, await the client's start task before advancing time.
- A test waits only through `eventually` or `Cursor` (Swift, `Waiting.swift`) and `bounded` or `eventually` (Kotlin, `TestWaiting.kt`). Each wait has a timeout and its failure names the condition. `check_waits.sh` rejects loops, raw iterators, `runBlocking` and pauses (`Task.sleep`, `Thread.sleep`, `delay(`) in tests. A test that needs time to pass moves the test clock.
- A test that must see a row after upload runs or other commits reads storage (`Asset.find(id).current()`, `liveFind(...).current()`), not `peekRow`: a commit evicts the row from the row cache, so a cache read passes or fails with the timing of background runs. Run such a test with `cacheRows: 1` so it cannot depend on the cache.
- The step timer of `run_bounded` freezes the process tree (`SIGSTOP`) before it lists the descendants, so a step that spawns a child at the moment of expiry cannot leave it running. The self test of this (`inv_check_05_a_step_past_its_limit_leaves_no_process`) lets the limit expire at once and the child appear between the listing and the kill.
- Tests are never embedded in source files. A module is a directory with `mod.rs`, and the tests of `x.rs` are in `x_tests.rs` next to it.
- Every behavior test is written first and seen failing.
- Randomized tests use a fixed seed and compare every observer's last state with a fresh query.
- Tests count work (reads, calls, commits) instead of measuring time.

## Stress stand

A separate program runs random operations from several threads against one database. A fake server delays, refuses and repeats answers, goes offline and sends late events. The `features` step in `scripts/check_dblayer.sh` compiles all core targets with all features, including the stress tests. Run this step separately with `bash scripts/check_dblayer.sh --only features`. Run the stress tests with `cargo test -p dblayer --features stress sync::stress`. Race runs and Clippy remain manual checks.

```sh
cargo run -p dblayer --features stress --bin stress -- --seed 7 --threads 4 --seconds 20
```

| Flag | Meaning |
| ---- | ------- |
| `--seed N[,N...]` | One run per seed, default 1 |
| `--threads T` | Worker count, default 4. With 1 the run is deterministic: same seed, same final state |
| `--seconds S` | Time limit per run, default 10 |
| `--ops N` | Operations per worker instead of a time limit |
| `--trace` | Print every operation, server decision, queue and row list |
| `--no-minimize` | Skip the search for a shorter failing run |

Operations: create, update, destroy, like, replace, refresh, load, load more, retry, retry all, discard, ephemeral write, upload, reset, offline and online, clock steps, subscription events.

Checks while the run goes and at the end:

- Observers on lists, scopes, counts and single rows see growing revisions and a diff that leads from their previous state to the next. Their last state equals a fresh read.
- The row cache equals the database, ephemeral values included.
- `row_sync` equals the state derived from the open operations. A temp row always has an open create. No list repeats a row or points at a missing row.
- The server applies the writes of one record in the order they were made. A mutation is sent again only after a retry. Every write reaches the server, unless its record was discarded or its create was refused.
- After all operations finish, rows equal the server rows, and equal them again after a complete reload.
- A step that runs longer than 30 s fails with a dump of the queue. A blocked thread ends the run after 60 s with the dump of every worker.

A failing run prints the findings, the command to repeat it and, for one thread, the smallest `--ops` that still fails. With several threads the same seed repeats the choices, not the interleaving, so a race may need several runs.

Example models for tests are in `fixtures/` (`User`, `Moment`, `Vibe`, `Group`, `Note`).

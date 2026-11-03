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
DBlayer.open(context, transport = transport, modelsJson = modelsJSON, uploadNotification = notification)
```

A call with no queued reply fails at once with `Invalid` that names the operation and its variables, and the request is listed in `transport.unscripted`. The test that caused the call fails too, without any check in the test. Swift records a Swift Testing issue on the test that created the mock, so create the mock inside the test (or its suite initializer). Kotlin cannot throw on the core thread, so the next touch of any mock on a test thread throws `AssertionError` with the operation, the variables and the place that created the mock: `MockTransport()`, `enqueue`, `emit`, `breakSubscription`, `calls`, `cableFrames` and `unsubscribe`. A call that is still unreported when the JVM exits stops it with exit code 1. A test that expects such a call wraps it in `withKnownIssue` (Swift) or reads `transport.claimUnscripted()` (Kotlin). Pass `Unanswered.NetworkError` (Kotlin) or `unanswered: .networkError` (Swift) to the constructor when a test needs a missing reply to be a network error, which the core retries. `unsubscribe` with a token the mock never issued is recorded in `unknownTokens` and does not throw.

Background uploads on iOS go through an `UploadTaskSource`. `ScriptedUploadTasks` in `DBlayerTesting` is a source that a test drives: `startedTasks` and `cancelledTags` list what the library asked for, and `sendProgress`, `finish`, `fail` and `finishSessionEvents` report back through `UploadTaskEvents`. Each task is an `UploadTaskRequest` with its request, file and tag. Pass the source as `uploads` to `UrlSessionTransport`.

`transport.calls` lists the requests it received. `emit` pushes a subscription event and `breakSubscription` drops the subscription.

For model events, open the reader before calling `emit`: `let events = try await Message.events()` in Swift, `val events = Message.events()` in Kotlin. The call returns after the transport has registered the subscription sink, or throws if opening fails. Emit immediately after it returns, then read or collect the returned stream. No sleep, retry or request-count check is needed. Events sent before opening are not replayed.

Keep native observer handles alive until every asynchronous assertion finishes. In Kotlin, enclose direct `liveEntities` tests in `use` and test delivery after `System.gc()`.
JVM locals can become unreachable after their last use, even while the enclosing test still awaits callbacks.

A live query loads by itself when something subscribes, and an explicit refresh can add a second request. Queue 2 or 3 replies per operation and assert the content of `transport.calls`, not their exact count.

Move time with a test clock instead of sleeping. `DB.openTesting` (Swift, `DBlayerTesting`) and `DBlayer.openTesting` (Kotlin) open the database on a clock that only `advance` moves, and return it. The clock drives `fresh_for`, load retries and ephemeral expiry. `advance` expires due memory rows and resets due ephemeral values at once, with no waiting. The production `open` has no clock parameter. `openTesting` takes `preload`, default false: a scope declared with `preload` sends no request on open, session, network return or the end of power saving. Pass `preload: true` to load like the production `open`. In Kotlin, `openTesting` and `TestClock.advance` need `@OptIn(DBlayerTestingApi::class)` on the calling declaration.

`openTesting` takes `listening`, default true, like `open`. Pass false to hold model subscriptions until registration completes, then call `DB.setListening(true)` or `DBlayer.setListening(true)`. When declaring `uploads`, pass their `GraphQLOperation` definitions in `operations`.

`DB.enterForegroundForTesting()` in Swift (`DBlayerTesting`) and `DBlayer.enterForegroundForTesting()` in Kotlin (`@DBlayerTestingApi`) require a database opened with `openTesting`. They send the same core event as a system foreground notice, without posting notifications or changing the process lifecycle. A scope with `refresh_on_foreground: true` reloads when it has a reader, even while fresh. No reader means no request. The call schedules the event. Await the window's result to check its effect.

`resetForTesting` clears model data using the normal data reset while keeping the clock, transport, registered operations, and session. Call the explicit session reset when a test needs no session. Existing loads finish under their original generation. `settleForTesting` waits for background work and reports the busy part if it does not finish within 10 seconds. Both calls require `openTesting`.

Held entities and live queries reconnect and load again after `resetForTesting`.

```swift
try await DB.resetForTesting()
try await DB.settleForTesting()
```

```kotlin
DBlayer.resetForTesting()
DBlayer.settleForTesting()
```

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
- a mutation that gets no answer becomes `deliveryUnknown` and is sent again only by `retry(action)`
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
| Every check run keeps its logs in its own `target/check-logs/<UTC time>-<sha>-<lane>-<random>/` (step logs, `summary.txt`, `run.txt`, and the Gradle test result XML and report of `gradle-test` under their source paths in `gradle-test.artifacts/`), `target/check-logs/latest` points at the newest run, nothing is deleted | `bash scripts/check_logs_test.sh` |
| Duplicate mechanisms and names | `bash scripts/check_zoo.sh` |
| Docs reference real paths | `bash scripts/check_docs.sh` |
| No test waits without a bound or pauses for a time (`Task.sleep`, `Thread.sleep`, `delay(`) outside the wait helpers | `bash scripts/check_waits.sh` |
| No code swallows an error with `try?` or `runCatching {...}.getOrNull()` outside `scripts/silent_allowlist.txt` | `bash scripts/check_silent.sh` |
| Network code has executable coverage for every source file | `bash scripts/check_coverage.sh` (after the swift and gradle test lanes with coverage) |
| Coverage rejects missing reports and unexecuted lines on both platforms | `python3 -I scripts/check_coverage_test.py` |

The check command stops after 300 seconds and kills its own child processes.
Build the native artifacts before running checks. Unchanged native inputs reuse the built artifacts.

Coverage includes Swift network sources, `UrlSessionTransport.swift` and `UrlSessionCable.swift`. Kotlin includes encrypted HTTP, sockets, `Transports.kt` and `TransportFailure.kt`. Missing executable coverage for any included file fails the check. Unexecuted lines require an entry in `scripts/coverage_allowlist.txt`. The Swift tests use atomic profile counters to preserve increments from concurrent execution.

`GraphQLOperationKind.swift` declares enum cases without executable statements and is excluded from line coverage.

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
- Rust tests own storage, synchronization and ordering rules. Swift and Kotlin tests check typed calls, decoding, callbacks and platform APIs.
- Kotlin network tests use Robolectric because the Android OkHttp artifact requires Android APIs, including logging.
- Keep one test per requirement at the lowest level that identifies its failure. Combine similar cases in a table.
- Core stable-order tests own confirmed-row placement. Platform `LandsTests` and `LandsTest` check additional returned rows through the bridge.
- Core edge tests own connection isolation and value replacement. Platform edge tests check typed reads, nullable fields, aliases and enums.
- Core member tests own window isolation, page accumulation, freshness and retained totals. Platform member tests check arguments and typed snapshots.
- Core node tests own freshness, partial merges, response order and missing entries. Platform node tests check IDs, force, declared variables and typed results.
- Core upload tests check source ownership, failed admission cleanup and progress delivery. Tests await an observed state, never a revision change after a call returns.
- Core observation tests own read limits, pending entity ids and combined window expansions. Platform tests check typed delivery and cancellation through the bridge.
- Randomized tests use a fixed seed and compare every observer's last state with a fresh query.
- Tests count work (reads, calls, commits) instead of measuring time.

## Stress stand

A separate program runs random operations from several threads against one database. A fake server delays, refuses and repeats answers, goes offline and sends late events. The `cargo-test-workspace` step in `scripts/check_dblayer.sh` tests the workspace with all features, including stress checks and injected read failures. Run these tests separately with `cargo test -p dblayer --features stress sync::stress`. Race runs and Clippy remain manual checks.

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

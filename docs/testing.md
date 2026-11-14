# Testing

## In your app

Open a database in memory or in a temporary directory and give it a mock transport. `DB.close()` and `DBlayer.close()` are async. Calling it before the first `open` fails with `NotOpen`, and calling it on a closed database is a success. The mock replies by operation name, can delay or fail replies, and can break a subscription. A mock transport alone opens a database without a session, which serves local models. For server models, sign in with `openSigned` (see below).

```swift
import DBlayerTesting

let transport = MockTransport()
transport.enqueue("MomentsFeed", reply: feedJSON)
try await DB.open(transport: transport, modelsJSON: modelsJSON, uploadTaskTitle: "Uploading")
```

```kotlin
val transport = MockTransport()
transport.enqueue("MomentsFeed", feedJson)
DBlayer.open(context, transport = transport, modelsJson = modelsJSON, uploadNotification = notification)
```

A call with no queued reply fails at once with `Invalid` that names the operation and its variables, and the request is listed in `transport.unscripted`. The test that caused the call fails too, without any check in the test. Swift records a Swift Testing issue on the test that created the mock, so create the mock inside the test (or its suite initializer). Kotlin cannot throw on the core thread, so the next touch of any mock on a test thread throws `AssertionError` with the operation, the variables and the place that created the mock: `MockTransport()`, `enqueue`, `emit`, `calls` and `cableFrames`. A call that is still unreported when the JVM exits stops it with exit code 1. A test that expects such a call wraps it in `withKnownIssue` (Swift). In Kotlin the test reads `transport.unscripted` and asserts the failing touch of the mock with `assertFailsWith<AssertionError>`. Pass `Unanswered.NetworkError` (Kotlin) or `unanswered: .networkError` (Swift) to the constructor when a test needs a missing reply to be a network error, which the core retries.

The test doubles follow the rules of the real server, and the core decides them for `MockTransport` and `SealedServer` through one `ServerContract` object. A request that repeats a pair (author and request id) gets the reply of the first send. A second body under one pair is refused like a call with no queued reply. An unscripted request is refused. A hash-only request for a document the server does not know gets `PERSISTED_QUERY_NOT_FOUND` without a sequence, and the library sends the full document under the same number. A request with a wrong sequence number gets `SEQUENCE_CONFLICT`.

A hash-only request for a document the server does not know is refused. A request with the full document needs no earlier registration. `openSigned` registers the documents of its `operations` list on the server. For operations declared after the open, `registerStandOperations` registers the list on the server of the stand, when the test opened one, and in the database. Pass it the `Database` that the open returned. With `SealedServer` alone register documents with `registerDocuments`.

```swift
let database = try await openSigned(config: config, modelsJson: modelsJSON, transport: transport, operations: operationsJSON)
try await registerStandOperations(database, defsJson: lateOperationsJSON)
try server.registerDocuments(lateOperationsJSON)
```

```kotlin
val database = openSigned(config, modelsJson, transport, operations = operationsJson)
registerStandOperations(database, lateOperationsJson)
server.registerDocuments(lateOperationsJson)
```

Every open of the stand installs a `ScriptedPreparer`, so a source with `prepare` settings uploads in a test. The double has no codecs: its probe answers the kind the settings name, `run` copies the source to the result, and the upload sends the bytes of the source. Pass your own `preparer` to open the stand with another one. The outcome of `run` and the facts of the video probe are parameters of the double.

```swift
let database = try await openSigned(config: config, modelsJson: modelsJSON, transport: transport, operations: operationsJSON, preparer: ScriptedPreparer(outcome: .cannotDecode(message: "no decoder")))
```

```kotlin
val database = openSigned(config, modelsJson, transport, operations = operationsJson, preparer = ScriptedPreparer(PrepareOutcome.CannotDecode("no decoder")))
```

A test that calls `ServerContract` directly gets `Invalid` from `admit` and `settle` for text that is not valid JSON. A valid object with missing members is read with an empty request id, no sequence and no body. `admit` returns `stored`, `refused`, `twoBodies` or `fresh`. `registerDocument` adds a document and `document(hash)` finds it, `nil` or `null` when it is unknown.

When two windows of one operation differ only in their variables, answer them by content with `respondWith`. The responder gets the request and returns the reply, or `nil` to take the next reply queued with `enqueue`. A repeat of the same request gets the same reply.

```swift
transport.respondWith("MomentsFeed") { request in request.variablesJson.contains("\"first\":5") ? shortFeedJSON : nil }
```

```kotlin
transport.respondWith("MomentsFeed") { request -> if (request.variablesJson.contains("\"first\":5")) shortFeedJson else null }
```

`transport.calls` lists the requests it received. `emit` pushes a subscription event.

A file upload needs one scripted reply for each PUT: `enqueuePut(url, etag:)` in Swift, and `enqueuePut` in Kotlin, answer one PUT of that URL. A PUT without a queued reply fails like a call without one, and `putCalls` lists the URLs the library sent. A failure takes `enqueuePut(url, failure:)`.

A staged upload needs the replies of the steps of a multipart upload (`InitMultipart`, `PresignParts`, one `enqueuePut` for the part) and, for `commit`, `CompleteMultipart` and the final mutation. `stage` and `commit` use the same transport double as `send`, and `ScriptedPreparer` prepares a staged source without a change in the app.

For model events, open the reader before calling `emit`: `let events = try scope.events(Message.self)` in Swift, `val events = scope.events(Message).events(Message)` in Kotlin. The call returns after the stream is registered in the core. Emit immediately after it returns, then read or collect the returned stream. No sleep, retry or request-count check is needed. Events sent before opening are not replayed.

Keep the projections and streams of a test alive until every asynchronous assertion finishes. A stream ends in the core when its reader is released. A stream has one reader: a test that iterates one `ValueStream` twice sees `ended(.invalid)` on the second iterator.

A live query sends one request when its first reader subscribes. Another reader or `load()` of the same window sends nothing until its `freshness` ends, and the test clock moves only on `advance`. `refresh` always sends. Queue one reply per expected request and assert the exact list in `transport.calls`. A reply left in the queue answers the next request of its operation, so a spare reply hides an extra request.

Move time with a test clock instead of sleeping. `DB.openTesting` (Swift, `DBlayerTesting`) and `DBlayer.openTesting` (Kotlin) open the database on a clock that only `advance` moves, and return it. The clock drives `fresh_for`, load retries and ephemeral expiry. `advance` expires due memory rows and resets due ephemeral values at once, with no waiting. The production `open` has no clock parameter and no test hook: the test artifacts call the core with the clock and hand the opened database to the library with one setter. `openTesting` also takes `observers`, the platform observer that reports online, foreground and power saving; Swift reports online at once when none is given. In Kotlin, `openTesting` and `TestClock.advance` need `@OptIn(DBlayerTestingApi::class)` on the calling declaration.
The databases of the test functions (`DB.openTesting` and the stands of `DBlayerTesting` in Swift, `DBlayer.openTesting` and the test fixtures in Kotlin) do not ask for the complete set of operations of the models.

To run the real encrypted session in a test, use `SealedServer` and `EncryptedCablePeer`, both in `DBlayerTesting` (Swift) and in the `testFixtures` of the Maven artifact (Kotlin). `SealedServer` opens every sealed request with the fixture key and seals the answer. `EncryptedCablePeer` is the cable end on top of it: it completes the handshake, confirms subscriptions and pushes a data event with `event(operation, result)`. Pass the server as `http` and the peer as the cable of the transport.

When declaring `uploads`, pass their `GraphQLOperation` definitions in `operations`.

A test opens its database and closes it. There is no reset between tests: close the database, set the next transport and open again. `close` waits until every lease ended.

A database opened without a session keeps the rows of local models only, and `MockTransport` alone does not give it one. A send, a load and a create of a server model are refused there with `NoSession`. To test a server model, sign in with `openSigned` from `DBlayerTesting` (Swift) or `testFixtures` (Kotlin). It signs in over `SealedServer` and `EncryptedCablePeer`, bridges every query and mutation to the `MockTransport` you pass and returns the open database. Give the model its rows with the replies of that mock. `openSignedClocked` does the same on a test clock, `openSignedOffline` signs in and then reports no network, `loginAgain` signs in again after `sessionReset()`, and `closeForCleanup` closes a database a test may already have closed.

After `sessionReset()` the credential and the data of the account are gone. A test signs in again with `sessionCall`. A call of the epoch before the reset ends with `NoSession`.

A second `close` returns success. Any other call on a closed database fails with `Closed`, and any call before the first `open` fails with `NotOpen`.

`openTesting` also takes the encrypted session parameters of `open`: `encrypted`, `credentials` and `http`, all optional. With them the test runs the real encrypted session path and replaces only the HTTP exchange. Without them the database has no session client, as before.

```swift
let clock = try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, encrypted: encrypted, credentials: credentials, http: fakeServer)
try await DB.sessionCall("login_link", body: LoginLink(account: "A"))
```

```kotlin
val clock = DBlayer.openTesting(transport, modelsJSON, encrypted = encrypted, credentials = credentials, http = fakeServer)
DBlayer.sessionCall<LoginLink, JsonElement>("login_link", LoginLink(account = "A"))
```

```swift
let transport = MockTransport()
transport.enqueue("MomentsFeed", reply: feedJSON)
try await openSigned(config: Config(path: nil), modelsJson: modelsJSON, transport: transport, operations: feedOperations)
```

```kotlin
val transport = MockTransport()
transport.enqueue("MomentsFeed", feedJson)
openSigned(Config(null), modelsJSON, transport, operations = feedOperations)
```

```swift
try await DB.close()
try await DB.openTesting(transport: next, modelsJSON: modelsJSON)
```

```kotlin
DBlayer.close()
DBlayer.openTesting(next, modelsJSON)
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

### Testing a restart

A test checks a restart by opening the same path twice. `openSignedClocked` with the same `path` and the same `MockTransport` closes the open database, keeps the credential of that path, opens the same files and signs in as a restored session. There is no new API: the test opens and closes the database itself.

```swift
let path = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString).path
try await openSignedClocked(modelsJson: modelsJSON, transport: transport, operations: operations, path: path)
// ... send a write and a file, then restart
try await openSignedClocked(modelsJson: modelsJSON, transport: transport, operations: operations, path: path)
```

```kotlin
val path = kotlin.io.path.createTempDirectory().toFile().resolve("restart.sqlite").absolutePath
openSignedClocked(modelsJSON, transport, operations, path = path)
// ... send a write and a file, then restart
openSignedClocked(modelsJSON, transport, operations, path = path)
```

Without the stand, `close` and `openTesting` with the same `path` do the same when `credentials` keeps the secret between the two opens.

```swift
try await DB.close()
try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, path: path, credentials: credentials)
```

```kotlin
DBlayer.close()
DBlayer.openTesting(transport, modelsJSON, path = path, credentials = credentials)
```

After the second open the test sees what an app sees after a restart:

- A saved write goes out again, once, in the order it was made.
- A saved transfer with its copy continues from its last confirmed step.
- A row with a deferred source reads `failed` with the error kind `missing_file`. `retry` without a source throws `Invalid`, and `retry` with a source starts a new cycle.

A call in flight at `close` ends with an error, while the same call in a killed process never answers.

### Failure values

A test shows a failed row without a server: the test products build the values from their fields. `operationFailure` takes `kind`, `reason` and `message`, and the other fields stay empty. `syncInfo` takes a state and a failure. `uploadInfo` takes a status, the sent share from 0 to 1 and a failure. Your app code does not build these values, and the app test reads no JSON for them.

```swift
import DBlayerTesting

let failure = operationFailure(kind: "Invalid", reason: "the cell is empty", message: "refused")
let sync = syncInfo(state: .failed, error: failure)
let upload = uploadInfo(.failed, progress: 0.4, error: failure)
```

```kotlin
val failure = operationFailure("Invalid", "the cell is empty", "refused")
val sync = syncInfo(SyncState.Failed, failure)
val upload = uploadInfo(UploadStatus.Failed, 0.4, failure)
```

Typical scenarios to cover:

- first page, next page, refresh
- fresh data is not requested again, parallel loads become one request
- a failed load is retried, a fatal failure is reported
- a mutation that gets no answer is sent again by the library with the same request id
- optimistic create is replaced by the server row, refusal rolls it back
- a subscription event changes a row and your live result updates
- a dropped subscription reconnects and the relation is refetched

## In this repository

`scripts/check_dblayer.sh` runs the cheap, cargo, swift and kotlin lanes at the same time (the steps of one lane run in order, each takes its own build slot), then the final lane when all of them passed, all within a 300 s limit. Every test step (`cargo-test-core`, `cargo-test-gen`, `swift-test`, `gradle-test`) has its own 300 s limit, compile steps keep theirs; a single lane run (`--only <lane>`) is bounded by its steps, not by the 300 s of the whole check.

The Swift and Kotlin lanes compile the generated models of the library fixtures with warnings treated as errors (`FixtureModelsCompileTests`, `:fixture-models-check`).

| Check                                          | Command                                |
| ---------------------------------------------- | -------------------------------------- |
| Rust core and generator                        | `cargo test`                           |
| Everything, including Swift and Kotlin         | `bash scripts/check_dblayer.sh`        |
| Android R8 release and optimized JVM FFI calls | `gradle -p kotlin/r8-check probe`      |
| Touched directories only                       | `bash scripts/check_dblayer.sh --fast` |

| Duplicate mechanisms and names | `bash scripts/check_zoo.sh` |
| Docs reference real paths | `bash scripts/check_docs.sh` |
| No test waits without a bound or pauses for a time (`Task.sleep`, `Thread.sleep`, `delay(`) outside the wait functions | `bash scripts/check_waits.sh` |
| No code swallows an error with `try?` or `runCatching {...}.getOrNull()` outside `scripts/silent_allowlist.txt` | `bash scripts/check_silent.sh` |
| Network code has executable coverage for every source file | `bash scripts/check_coverage.sh` (after the swift and gradle test lanes with coverage) |

The check command stops after 300 seconds and kills its own child processes.
The Swift tests run one at a time (`swift test --no-parallel`): the process holds one open database, so tests must not overlap.
Build the native artifacts before running checks. Unchanged native inputs reuse the built artifacts.

Coverage includes Swift network sources, `UrlSessionTransport.swift` and `UrlSessionCable.swift`. Kotlin includes encrypted HTTP, `OkHttpCable.kt`, `Http.kt` and `Transports.kt`. Missing executable coverage for any included file fails the check. Unexecuted lines require an entry in `scripts/coverage_allowlist.txt`. The Swift tests use atomic profile counters to preserve increments from concurrent execution.

The R8 check uses the generated Kotlin bindings. Run `bash scripts/build_kotlin.sh --host` first when running it separately.
It builds a minimal Android release application with minification enabled and the package's consumer rules.
It also runs optimized JVM bytecode against the host Rust library, checking structures, callbacks, records, enums and errors.
Both mappings must rename `Config`, which rejects a package-wide keep rule.
Use `-Prules=/absolute/path/to/rules.pro` to check a changed rule file.
Android device execution remains a separate check.
See [Android R8 checks](android-r8.md) for the rules.

Test conventions:

- A test that needs a fresh database calls `DB.close()` (Swift) or `DBlayer.close()` (Kotlin) first, then opens. A second `open` without `close` fails. `DB.openTesting` and `DBlayer.openTesting` follow the same rule. Swift tests that open the shared database, including calls through `DB.open`, belong to the serialized `BridgeProbeTests` suite. Nest related suites there. Separate serialized suites still run concurrently and can replace each other's database during an awaited call.
- Kotlin tests that open the shared database run sequentially with JUnit 4. Keep that shared state sequential if changing the test runner. Tests using independent database handles can run concurrently on either platform.
- Before advancing manual cable clocks, await the registered deadline in Swift and Kotlin. A received frame's timestamp does not prove its asynchronous sleep has registered. A closed socket also precedes the connection actor's completion and its reconnect sleep registration. Read the actor's closed state, then await that sleep. After a reopen delay, await the new socket rather than a fixed settling interval. For terminal closure, await the client's start task before advancing time.
- A test waits only through `eventually` or `Cursor` (Swift, `Waiting.swift`) and `bounded` or `eventually` (Kotlin, `TestWaiting.kt`). Each wait has a timeout and its failure names the condition. `check_waits.sh` rejects loops, raw iterators, `runBlocking` and pauses (`Task.sleep`, `Thread.sleep`, `delay(`) in tests. A test that needs time to pass moves the test clock.
- A test that must see a row after upload runs or other commits reads storage (`Query<Asset>().readRow(id)`), not `cachedRow`: a commit evicts the row from the row cache, so a cache read passes or fails with the timing of background runs.
- The step timer of `run_bounded` freezes the process tree (`SIGSTOP`) before it lists the descendants, so a step that spawns a child at the moment of expiry cannot leave it running.
- Tests are never embedded in source files. A module is a directory with `mod.rs`, and the tests of `x.rs` are in `x_tests.rs` next to it.
- Every behavior test is written first and seen failing.
- Rust tests own storage, synchronization and ordering rules. Swift and Kotlin tests check typed calls, decoding, callbacks and platform APIs.
- Kotlin network tests use Robolectric because the Android OkHttp artifact requires Android APIs, including logging.
- Keep one test per requirement at the lowest level that identifies its failure. Combine similar cases in a table.
- Core stable-order tests own confirmed-row placement. Platform tests do not repeat it.
- Core edge tests own connection isolation and value replacement. Platform edge tests check typed reads, nullable fields, aliases and enums.
- Core member tests own window isolation, page accumulation, freshness and retained totals. Platform member tests check arguments and typed snapshots.
- Core node tests own freshness, partial merges, response order and missing entries. Platform node tests check IDs, force, declared variables and typed results.
- Core upload tests check source ownership, failed admission cleanup and progress delivery. Tests await an observed state, never a revision change after a call returns.
- Core delivery tests own journals, resync and the end of a stream. Platform tests check typed delivery and cancellation through the bridge.
- Randomized tests use a fixed seed and compare every observer's last state with a fresh query.
- Tests count work (reads, calls, commits) instead of measuring time.

## Lifecycle sequences

`core/src/ffi/selftest/` runs the orders of the apps through the public `Database`. The session path is the real encrypted client and only the HTTP exchange is fake. Every call of a sequence runs inside a bound and fails instead of hanging.

| Group            | What it covers                                                                                                                                                                                                                       |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `app_tests`      | Login in the iOS and Android order, the exit with a held singleton and a pending wait, local-only exit, refused unregister, account switch A, B, A, the same account again, `ALREADY_AUTHENTICATED` then sign-out and a second login |
| `harness_tests`  | Target unset (Interrupted or Invalid), close, next target, a new database. A late answer of the old target. Reopen through the same opener. Tests that leave offline, a session reset and subscriptions behind                       |
| `calls_tests`    | Each explicit call in flight at the server when session reset or close arrives, with a late success or a late sealed refusal. A call the server never answers ends at close                                                          |
| `recovery_tests` | A round running at the session exit asks for the job of the next epoch, an active and a queued upload end at the exit and the next sign-in runs each once                                                                            |
| `windows_tests`  | Overlapping load-more and refresh in both answer orders, repeated online and power-saving signals, memory expiry on the last write                                                                                                   |

`core/src/ffi/selftest/manifest.tsv` is the fixed list of the release sequences. The `core` lane holds every sequence. The `swift` and `kotlin` lanes list only tests of behavior that the platform binding owns and the core does not have: value transfer, stream delivery, task cancellation, platform lifecycle, platform sockets and uploads. A row names the test of one lane, the state it had when written (green, red with its cause, flaky), what changes when the lifecycle rules change, and the app source line the order comes from. A core test checks that every row names an existing test, that every sequence test is listed, and that a test using the data logout or the old exit is marked as changing.

Swift and Kotlin keep the close with a held request and the session state parity through the bindings: `LifecycleSessionTests.swift`, `LifecycleSessionTest.kt`.

Example models for tests are in `fixtures/` (`User`, `Moment`, `Vibe`, `Group`, `Note`).

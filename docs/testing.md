# Testing

## In your app

Open a database in memory or in a temporary directory and give it a mock transport. The mock replies by operation name, can delay or fail replies, and can break a subscription.

```swift
import DBlayerTesting

let transport = MockTransport()
transport.enqueue("MomentsFeed", reply: feedJSON)
DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
```

```kotlin
val transport = MockTransport()
transport.enqueue("MomentsFeed", feedJson)
DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
```

`transport.calls` lists the requests it received. `emit` pushes a subscription event and `breakSubscription` drops the subscription.

A live query loads by itself when something subscribes, and an explicit refresh can add a second request. Queue 2 or 3 replies per operation and assert the content of `transport.calls`, not their exact count.

Use a manual clock to test freshness and load retries without sleeping.

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
| Duplicate mechanisms and names | `bash scripts/check_zoo.sh` |
| Docs reference real paths | `bash scripts/check_docs.sh` |
| No test waits without a bound | `bash scripts/check_waits.sh` |
| No cable line that no test runs | `bash scripts/check_coverage.sh` (after the swift and gradle test lanes with coverage) |

The coverage reports cost time: the Swift lane with coverage takes about the same as without, the Kotlin lane roughly doubles because of the coverage report task. The rule covers the cable code only.

Test conventions:

- A test waits only through `eventually` or `Cursor` (Swift, `Waiting.swift`) and `bounded` or `eventually` (Kotlin, `TestWaiting.kt`). Each wait has a timeout and its failure names the condition. `check_waits.sh` rejects loops, raw iterators and `runBlocking` in tests.
- Tests are never embedded in source files. A module is a directory with `mod.rs`, and the tests of `x.rs` are in `x_tests.rs` next to it.
- Every behavior test is written first and seen failing.
- Randomized tests use a fixed seed and compare every observer's last state with a fresh query.
- Tests count work (reads, calls, commits) instead of measuring time.

## Stress stand

A separate program runs random operations from several threads against one database. A fake server drops, delays, refuses and repeats answers, goes offline and pushes late and duplicate subscription events. The stand is not part of `cargo test` or `scripts/check_dblayer.sh`. Run it by hand, only when a change may open a race (queue, cache, observers, concurrency). Clippy is also outside the standard run.

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

`scripts/check_dblayer.sh` runs 3 fixed single-thread seeds (deterministic, blocking) and 3 four-thread seeds of 5 s (report only while open races remain).

Example models for tests are in `fixtures/` (`User`, `Moment`, `Vibe`, `Chat`, `Message`).

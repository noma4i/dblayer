# Testing

## In your app

Open a database in memory or in a temporary directory and give it a mock transport. The mock replies by operation name, can delay or fail replies, and can break a subscription.

```swift
let transport = MockTransport()
transport.enqueue("MomentsFeed", reply: feedJSON)
let db = try Database.open(config: .inMemory, models: models, transport: transport)
```

```kotlin
val transport = MockTransport()
transport.enqueue("MomentsFeed", reply = feedJson)
val db = Database.open(config = Config.inMemory, models = models, transport = transport)
```

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

Test conventions:

- Tests are never embedded in source files. A module is a directory with `mod.rs`, and the tests of `x.rs` are in `x_tests.rs` next to it.
- Every behavior test is written first and seen failing.
- Randomized tests use a fixed seed and compare every observer's last state with a fresh query.
- Tests count work (reads, calls, commits) instead of measuring time.

Example models for tests are in `fixtures/` (`User`, `Moment`, `Vibe`, `Chat`, `Message`).

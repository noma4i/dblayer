# Sync state of a record

Every record knows whether the server has seen it. The state is live like everything else, so you can build UI and logic on it: show a spinner on a moment that is still being sent, a retry button on a message that failed, a badge on a record that has unsaved edits.

## What a record exposes

| Property | Meaning |
| -------- | ------- |
| `sync.state` | `synced`, `pending`, `sending` or `failed` |
| `sync.actions` | actions waiting for the server, in order (`create`, `update`, `destroy`, custom actions) |
| `sync.dirtyFields` | fields changed locally and not yet confirmed |
| `sync.error` | the last error (`kind`, `message` and optional GraphQL `extensions`) when the state is `failed` |
| `sync.attempts` | how many times sending was tried since the last `retry(action)` |
| `sync.since` | when the record entered this state |
| `uploads` | one entry per declared upload action with its status, stage, bytes and error, empty when nothing is uploading |
| `errorMessage` | `sync.error?.message` |

| State | When |
| ----- | ---- |
| `synced` | nothing is waiting for the server |
| `pending` | a change is queued and sending has not started or it waits in the queue again (offline, busy slot), or an upload of the record has not started (`uploads[action].status` is `waiting` or `queued`) |
| `sending` | an attempt was written and no answer has arrived: the same word as the `status` of the write in the queue of operations |
| `failed` | the server refused the change and the record is kept for you to decide |

The error `kind` is `network`, `server`, `rejected`, `unrecognized` or `session:<CODE>`. A sealed session refusal of the encrypted session gives the wire name of its code, for example `session:UNAUTHENTICATED`, and `message` holds the detail text.

GraphQL `extensions.code` and `extensions.transactionDisposition` use the same types as [operation errors](models.md#graphql-errors) on Swift and Kotlin.
They persist for queued operations, record sync state and upload failures. Retrying clears them with the previous failure.

## Example: a moment that is still being sent

```swift
struct MomentRow: View {
  let moment: Moment

  var body: some View {
    HStack {
      Text(moment.text)
      if moment.sync.state == .pending || moment.sync.state == .sending { ProgressView() }
      if moment.sync.state == .failed {
        Button("Retry") { Task { try await moment.retry(action: nil) } }
      }
    }
  }
}
```

```kotlin
@Composable
fun MomentRow(moment: Moment) {
  Row {
    Text(moment.text)
    if (moment.sync.state == SyncState.Pending || moment.sync.state == SyncState.Sending) CircularProgressIndicator()
    if (moment.sync.state == SyncState.Failed) {
      Button(onClick = { scope.launch { moment.retry(null) } }) { Text("Retry") }
    }
  }
}
```

State changes arrive like any other change: the row is delivered again with its new state, as a `rewrite` edit of the stream.

## Queries by state

A query can filter on the sync state of the rows with the field name `sync`, whose values are `pending`, `sending`, `failed` and `synced`:

```swift
let failed = Query<Moment>().where(.equal("sync", .string("failed")))
let unsent = Query<Moment>().where(.inList("sync", [.string("pending"), .string("sending")]))
```

```kotlin
val failed = Query("Moment", Moment).where(Condition.equal("sync", JsonPrimitive("failed")))
val unsent = Query("Moment", Moment).where(Condition.inList("sync", listOf(JsonPrimitive("pending"), JsonPrimitive("sending"))))
```

Use them for an "unsent" banner, a retry-all button or a badge with the number of pending changes. `sync` is a reserved name: a model cannot have a field called `sync`.

`retry(action)` resets the one failed operation and returns a new receipt; read its `confirmed()` for the result of the retry. It throws errors to the caller.

## The queue of operations

`operations()` streams every write that has not ended, oldest first: the whole list when you open it and again after each change, then one `ended` item with the cause of the core (`OperationQueueItem.Ended` in Kotlin). A write enters the list before its receipt returns and leaves it when the server confirmed it or it rolled back.

Each item of the list is an `OperationInfo`:

| Field | Meaning |
| ----- | ------- |
| `seq` | the order of the write |
| `model`, `rowId`, `action` | the write; the same three arguments address `retry` and `discard` |
| `status` | `pending` (queued, not sent, or back in the queue after the network was lost), `sending` (an attempt was written and not answered) or `failed` (refused and kept, `on_failure: keep`); `sync.state` of the record shows the same word |
| `attempts` | how many attempts were written: 1 once the first request has left |
| `error` | the failure of a `failed` write |
| `createdAt` | when the write was queued |
| `blockedBy` | the `seq` of the failed write of the same row that this write waits behind |
| `upload` | the value of `uploads[action]` of an active file transfer, as the row shows it; `uploadInfo` gives it as `UploadInfo` (nil/null for a write that is no transfer) |

An active file transfer is in the list from its acceptance until it ends, also when its row does not exist, and after a restart. An entry that has an operation shows once and carries `upload`.

```swift
let scope = try DB.account()
for await item in scope.operations() {
  guard case let .queue(_, queue) = item else { break }
  badge = queue.filter { $0.status == .failed }.count
  if let refused = queue.first(where: { $0.status == .failed }) {
    try await scope.retry(model: refused.model, id: refused.rowId, action: refused.action)
  }
}
```

```kotlin
val scope = AccountScope.current()
scope.operations().collect { item ->
  val queue = (item as? OperationQueueItem.Queue)?.items ?: return@collect
  badge = queue.count { it.status == OperationQueueStatus.FAILED }
  queue.firstOrNull { it.status == OperationQueueStatus.FAILED }?.let { scope.retry(it.model, it.rowId, it.action) }
}
```

`retry` sends a failed write again and throws `NotFound` when the write is gone. `discard` drops a write that is not on its way and rolls back its optimistic change; it throws `InFlight` for a write that is on its way. A write queued behind a failed write of the same row waits and shows `blockedBy`.

## How a mutation is sent

The network decides what happens to a mutation that did not get an answer:

- A send that fails without an answer (no connection, connection refused, a dropped request): the library sends the same request again. The record stays `sending` while it waits between tries, `sync.attempts` counts the tries, and the next try comes after a growing pause with random spread. After 40 tries the change is `failed` for retry or discard. To send one such record again, call `retry(action)`.

Later changes to the same record wait behind it.
- Offline (the platform reports no network): the change waits and spends no attempts. When the network returns, waiting changes are sent by themselves.
- A plain transfer started without a confirmed answer, or the app stopped while sending: the library sends the same saved request again with the same request id, and the record stays `sending` until the server answers or the retries end in `failed`.
- Encrypted requests with saved Pending repeat temporary failures using the saved identity. See [Retries](server-sync.md#retries).

The pause uses Clock and the stored attempt count, so restarting preserves the count.
See [Retries](server-sync.md#retries) for all budgets and the distinction between plain HTTP and encrypted Pending.
Temporary GraphQL codes keep optimistic changes pending on both transports.

| How the send ends | State | What happens to the record |
| ----------------- | ----- | -------------------------- |
| The server confirms | `synced` | the server row replaces the local one, a temporary id is swapped for the real one, references to it are rewritten |
| The server permanently refuses | `failed` with `on_failure: :keep`, otherwise gone | with `:rollback` the change is rolled back in one commit and the error is reported |
| A temporary GraphQL code, HTTP 408 or HTTP 429 | `pending` until the outbox budget is exhausted | the optimistic edit remains and sending resumes after a pause |
| No network or a busy send slot | `pending` | the change waits in the queue and goes when the network or the slot is free |
| A send fails without an answer | `sending` while the library waits between attempts, then `failed` after 40 attempts | retains the edit and encrypted Pending owner for retry or discard |
| The action names a mutation that is not registered in `open(operations:)` | the call throws `Invalid` | nothing is written and nothing is sent |
| No answer after a plain transfer started, including HTTP 5xx | `sending` | the library repeats the saved request, the server answers it once |
| Temporary encrypted HTTP failure with saved Pending | `pending` until the outbox budget is exhausted | the next attempt uses the saved request identity |

## What happens when the server refuses

You choose per action, in the model file:

`on_failure: :rollback` applies only to confirmed server refusals, while 40 undelivered attempts always retain `failed` and the edit.

| Setting | Result |
| ------- | ------ |
| `on_failure: :rollback` (default) | the local change is rolled back in one commit and the error is reported: a created record disappears, an edit gives back only the fields it still owns |
| `on_failure: :keep` | the record stays with `state = failed` and its error, and waits for your decision |

```ruby
operation :create do
  mutation "CreateNote"
  on_failure :keep
end
operation :like do
  mutation "LikeMoment"
  optimistic likedByMe: true
  on_failure :keep
end
```

`destroy` has no `on_failure`: the record is hidden at once. A refusal of the server rolls the destroy back: the record comes back with its files and list positions, without a `failed` state, and `confirmed` throws `Rejected` with `kept: false`. After 40 unreached attempts the destroy is `failed` with the error, attempts and time in `sync`: `retry(action)` sends it again, `discard(action)` keeps the record and drops the destroy. Declaring `on_failure` on `destroy` is an error.

A record in `failed` has two gestures:

```swift
try await message.retry(action: nil)     // back to pending, attempts reset, sent again
try await message.discard(action: nil)   // roll back the local change and drop it from the queue
```

```kotlin
message.retry(null)     // back to pending, attempts reset, sent again
message.discard(null)   // roll back the local change and drop it from the queue
```

Later changes to the same record wait behind a `failed` change and are sent after `retry(action)`. Other records are not blocked. `discard(action)` of a created record also drops the changes queued on it.

## Online and offline

Opening starts network monitoring on both platforms. Kotlin requires an Android `Context` as the first argument. The bindings hand one observer object of the platform to the core: it starts with the database and the core stops it before `close` returns. Each OS value (network path, app foreground, power saving mode) goes to the core as one input. An app calls nothing for them.
Mutations, uploads and loads read the same core flag. While the network is away, changes and uploads wait, and a load or refresh fails with `Offline`. When the network returns, waiting changes and uploads are sent by themselves and open lists that failed on the network read again once.

## Refreshing

A whole list refreshes with `refresh()`. The load state of the relation (`phase`: `idle`, `loading`, `loaded`, `failed`, with `error`, `settled` and `hasMore`) is part of every list delivery, see [live data](live-data.md#state-of-a-stream).

Stored window read failures appear in the load snapshot, including its first delivery.
With rows, the phase is `loaded` with an error. Without rows, it is `failed`.
When a mutation reloads watched lists, its result reports all failed reloads and freshness reads.

## Guarantees

- The whole `sync` value is part of every delivery: a change of `state`, `actions`, `dirtyFields` or `error` without a data change still reaches observers, in order. The parent of an included row is delivered when the sync of that row changes.
- The state is written in the same commit that changes the queue, so a reader never sees a queued change without its state.
- Every queued change is durable: a record created offline stays `pending` across restarts and is sent when the queue next runs, for example after a reconnect. A change that was `sending` when the app stopped is sent again with the same request id after the restart.
- When the server answers, the temporary id is replaced by the real one in one commit and the state becomes `synced`. Readers see the swap as a `rewrite` edit with the `temporary` id.
- Rows and queued changes of other models that point at the temporary id through a declared association are updated in the same commit.
- A rollback restores the state saved in the change itself and only the parts the change still owns, see [writing data](writing-data.md#rolling-back).

## Uploads

A file upload is a pending operation of its record. While it runs, `record.sync.state` is `pending` or `sending`, and `record.uploads[action]` adds the stage, the sent bytes and the fraction. `retry(action)` and `discard(action)` work the same way. See [Files](files.md#uploads).

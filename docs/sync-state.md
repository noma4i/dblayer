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
| `pending` | a change is queued and sending has not started, or an upload of the record waits for the network (`uploads[action].waitReason == offline`) |
| `sending` | the request is on its way and no answer has arrived |
| `failed` | the server refused the change and the record is kept for you to decide |

The error `kind` is `network`, `server`, `rejected`, `unrecognized` or `session:<CODE>`. A sealed session refusal of the encrypted session gives the wire name of its code, for example `session:UNAUTHENTICATED`, and `message` holds the detail text.

Corrupt stored rollback data, revisions, sync states, and pending requests produce errors instead of empty or synced results.
An upload stops when its durable state cannot be saved.
If recording that failure also fails, database calls report a storage error and wake waiting operations.

GraphQL `extensions.code` and `extensions.transactionDisposition` use the same types as [operation errors](models.md#graphql-errors) on Swift and Kotlin.
They persist in SQLite node tables for queued operations, record sync state and upload failures. Retrying clears them with the previous failure.

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

State changes arrive like any other change: the row is delivered again with its new state, and the diff marks it `updated`.

## Queries by state

`sync` works in queries, and every model has ready-made lists:

```swift
Moment.where(Moment.sync == .pending)
Moment.unsynced        // everything not yet confirmed
Moment.failed          // records waiting for a decision
```

```kotlin
Moment.where(Moment.sync eq SyncState.Pending)
Moment.unsynced        // everything not yet confirmed
Moment.failed          // records waiting for a decision
```

Use them for an "unsent" banner, a retry-all button or a badge with the number of pending changes. `sync` is a reserved name: a model cannot have a field called `sync`.

`retry(action)` resets the one failed operation and returns a new receipt; read its `outcome()` for the result of the retry. It throws errors to the caller.

## The queue of operations

`operations()` streams every write that has not ended, oldest first: the whole list when you open it and again after each change. A write enters the list before its receipt returns and leaves it when the server confirmed it or it rolled back.

| Field | Meaning |
| ----- | ------- |
| `seq` | the order of the write |
| `model`, `rowId`, `action` | the write; the same three arguments address `retry` and `discard` |
| `status` | `pending` (queued, not sent), `sending` (an attempt was written and not answered) or `failed` (refused and kept, `on_failure: keep`) |
| `attempts` | how many attempts were written |
| `error` | the failure of a `failed` write |
| `createdAt` | when the write was queued |
| `blockedBy` | the `seq` of the failed write of the same row that this write waits behind |

```swift
let scope = try DB.account()
for try await queue in scope.operations() {
  badge = queue.filter { $0.status == .failed }.count
  if let refused = queue.first(where: { $0.status == .failed }) {
    try await scope.retry(model: refused.model, id: refused.rowId, action: refused.action)
  }
}
```

```kotlin
val scope = AccountScope.current()
scope.operations().collect { queue ->
  badge = queue.count { it.status == OperationQueueStatus.FAILED }
  queue.firstOrNull { it.status == OperationQueueStatus.FAILED }?.let { scope.retry(it.model, it.rowId, it.action) }
}
```

`retry` sends a failed write again and throws `NotFound` when the write is gone. `discard` drops a write that is not on its way and rolls back its optimistic change; it throws `InFlight` for a write that is on its way. A write queued behind a failed write of the same row waits and shows `blockedBy`.

## How a mutation is sent

The network decides what happens to a mutation that did not get an answer:

- Unreachable before the transfer starts (no connection, connection refused, offline): the request never left, so sending it again is safe. The record stays `pending`, `sync.attempts` counts the tries, and the next try comes after a growing pause with random spread. After 40 tries the change stays `failed` for retry or discard. To send one such record again, call `retry(action)`.

Later changes to the same record wait behind it.
- Offline (`setOnline(false)`): the change waits and spends no attempts. When `setOnline(true)` is called, waiting changes are sent by themselves.
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
| Unreachable before the transfer starts | `pending`, then `failed` after 40 attempts | retains the edit and encrypted Pending owner for retry or discard |
| The action names a mutation that is not registered in `open(operations:)` | `failed` whatever `on_failure` says, error `operation <name> is not registered` | nothing is sent, the record keeps its change, register the operation and call `retry(action)` |
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

`destroy` has no `on_failure`: the record is hidden at once, and if the destroy fails for good (a refusal or 40 unreached attempts) the record comes back with its files and list positions, `state = failed` and the error, attempts and time in `sync`. `retry(action)` hides the record again and sends the destroy. `discard(action)` keeps the record and drops the destroy. Declaring `on_failure` on `destroy` is an error.

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

Opening starts network monitoring on both platforms. Kotlin requires an Android `Context` as the first argument. The wrappers hand one observer object of the platform to the core: it starts with the database and the core stops it before `close` returns. Each OS value (network path, app foreground, power saving mode) goes to the core as one input. An app calls nothing for them.
Mutations, uploads and loads read the same core flag. While the network is away, changes and uploads wait, an action that needs the server fails at once with `Offline`, and a load or refresh fails with `Offline`. When the network returns, waiting changes and uploads are sent by themselves and open lists that failed on the network read again once.

## Refreshing

A whole list refreshes with `refresh()`. The load phase of the relation (`LoadPhase`: `notLoaded`, `loading`, `loaded`, `empty`, `failed`) is live, see [live data](live-data.md).

Stored window read failures appear in the load snapshot, including its first delivery.
With rows, the phase is `loaded` with an error. Without rows, it is `failed`.
When a mutation reloads watched lists, its result reports all failed reloads and freshness reads.

## Guarantees

- The whole `sync` value is part of every delivery: a change of `state`, `actions`, `dirtyFields` or `error` without a data change still reaches observers, in order. The parent of an included row is delivered when the sync of that row changes.
- The state is written in the same commit that changes the queue, so a reader never sees a queued change without its state.
- Every queued change is durable: a record created offline stays `pending` across restarts and is sent when the queue next runs, for example after a reconnect. A change that was `sending` when the app stopped is sent again with the same request id after the restart.
- When the server answers, the temporary id is replaced by the real one in one commit and the state becomes `synced`. Observers see the swap as `replaced`.
- Rows and queued changes of other models that point at the temporary id through a declared association are updated in the same commit.
- A rollback restores the state saved in the change itself and only the parts the change still owns, see [writing data](writing-data.md#rolling-back).

## Uploads

A file upload is a pending operation of its record. While it runs, `record.sync.state` is `pending` or `sending`, and `record.uploads[action]` adds the stage, the sent bytes and the fraction. `retry(action)` and `discard(action)` work the same way. See [Files](files.md#uploads).

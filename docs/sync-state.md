# Sync state of a record

Every record knows whether the server has seen it. The state is live like everything else, so you can build UI and logic on it: show a spinner on a moment that is still being sent, a retry button on a message that failed, a badge on a record that has unsaved edits.

## What a record exposes

| Property | Meaning |
| -------- | ------- |
| `sync.state` | `synced`, `pending`, `sending`, `failed` or `deliveryUnknown` |
| `sync.actions` | actions waiting for the server, in order (`create`, `update`, `destroy`, custom actions) |
| `sync.dirtyFields` | fields changed locally and not yet confirmed |
| `sync.error` | the last error (`kind` and `message`) when the state is `failed` or `deliveryUnknown` |
| `sync.attempts` | how many times sending was tried since the last `retry()` |
| `sync.since` | when the record entered this state |
| `sync.isRefreshing` | the record is being reloaded from the server right now |
| `isLocal` | the record has a temporary id and the server does not know it yet |
| `isSynced`, `isPending`, `isFailed` | shortcuts for the state |

| State | When |
| ----- | ---- |
| `synced` | nothing is waiting for the server |
| `pending` | a change is queued and sending has not started |
| `sending` | the request is on its way and no answer has arrived |
| `failed` | the server refused the change and the record is kept for you to decide |
| `deliveryUnknown` | the request was sent and no answer was seen: the connection broke, the server returned an error status, or the answer could not be understood |

The error `kind` is `network`, `server`, `rejected` or `unrecognized`.

## Example: a moment that is still being sent

```swift
struct MomentRow: View {
  let moment: Moment

  var body: some View {
    HStack {
      Text(moment.text)
      if moment.isLocal { ProgressView() }
      if moment.sync.state == .failed || moment.sync.state == .deliveryUnknown {
        Button("Retry") { Task { await moment.retry() } }
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
    if (moment.isLocal) CircularProgressIndicator()
    if (moment.sync.state == SyncState.Failed || moment.sync.state == SyncState.DeliveryUnknown) {
      Button(onClick = { scope.launch { moment.retry() } }) { Text("Retry") }
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

## How a mutation is sent

The network decides what happens to a mutation that did not get an answer:

- Unreachable before the transfer starts (no connection, connection refused, offline): the request never left, so sending it again is safe. The record stays `pending`, `sync.attempts` counts the tries, and the next try comes after a growing pause with random spread. After 40 tries the change fails like a refusal. To send every such record again, call `retryAll()` on the sync object:

```swift
try await sync.retryAll()
```

```kotlin
sync.retryAll()
```

Later changes to the same record wait behind it.
- Offline (`setOnline(false)`): the change waits and spends no attempts. When `setOnline(true)` is called, waiting changes are sent by themselves.
- The transfer started and no answer arrived (connection broke, 5xx status, unreadable answer), or the app stopped while sending: the server may already have applied the change, so it is never sent again by itself. Only you send it again, with `retry()`.

The pause is computed from the clock and the stored attempt count, so an app restart continues the count. Loads (queries) are different: they are retried within a budget, see [server sync](server-sync.md).

| How the send ends | State | What happens to the record |
| ----------------- | ----- | -------------------------- |
| The server confirms | `synced` | the server row replaces the local one, a temporary id is swapped for the real one, references to it are rewritten |
| The server refuses (GraphQL errors, a 4xx status) | `failed` with `on_failure: :keep`, otherwise gone | with `:rollback` the change is rolled back in one commit and the error is reported |
| Unreachable before the transfer starts | `pending`, then `failed` or gone after 40 attempts | sent again by itself, the final outcome follows `on_failure` like a refusal |
| No answer after the transfer started (broken connection, 5xx status, unreadable answer) | `deliveryUnknown` | the record stays as it is, whatever `on_failure` says, because the change may have reached the server |

## What happens when the server refuses

You choose per action, in the model file:

| Setting | Result |
| ------- | ------ |
| `on_failure: :rollback` (default) | the local change is rolled back in one commit and the error is reported: a created record disappears, an edit gives back only the fields it still owns |
| `on_failure: :keep` | the record stays with `state = failed` and its error, and waits for your decision |

```ruby
create with: CreateMessage, on_failure: :keep

action :like do
  mutation LikeMoment
  on_failure :keep
end
```

`destroy` has no `on_failure`: the record is hidden at once, and if the destroy fails for good (a refusal or 40 unreached attempts) the record comes back with its files and list positions, `state = failed` and the error, attempts and time in `sync`. `retry()` hides the record again and sends the destroy. `discard()` keeps the record and drops the destroy. Declaring `on_failure` on `destroy` is an error.

A record in `failed` or `deliveryUnknown` has two gestures:

```swift
await message.retry()     // back to pending, attempts reset, sent again
await message.discard()   // roll back the local change and drop it from the queue
```

```kotlin
message.retry()     // back to pending, attempts reset, sent again
message.discard()   // roll back the local change and drop it from the queue
```

Later changes to the same record wait behind a `failed` or `deliveryUnknown` change and are sent after `retry()`. Other records are not blocked. `discard()` of a created record also drops the changes queued on it.

## Online and offline

The platform reports the network with one call. Mutations, uploads and loads read the same flag.

```swift
sync.setOnline(false)   // changes wait, loads fail with an offline error, uploads wait
sync.setOnline(true)    // waiting changes and uploads are sent by themselves
```

```kotlin
sync.setOnline(false)   // changes wait, loads fail with an offline error, uploads wait
sync.setOnline(true)    // waiting changes and uploads are sent by themselves
```

## Refreshing

Two kinds of refreshing exist:

- A whole list: the loading state of the relation (`idle`, `loading`, `failed`) is live, see [live data](live-data.md).
- One record: `moment.refresh()` reloads it, and `moment.sync.isRefreshing` is true until the answer arrives. A record can be `synced` and refreshing at the same time.

## Guarantees

- The whole `sync` value is part of every delivery: a change of `state`, `actions`, `dirtyFields` or `error` without a data change still reaches observers, in order. The parent of an included row is delivered when the sync of that row changes.
- The state is written in the same commit that changes the queue, so a reader never sees a queued change without its state.
- Every queued change is durable: a record created offline stays `pending` across restarts and is sent when the queue next runs, for example after a reconnect. A change that was `sending` when the app stopped is `deliveryUnknown` after the restart.
- When the server answers, the temporary id is replaced by the real one in one commit and the state becomes `synced`. Observers see the swap as `replaced`.
- Rows and queued changes of other models that point at the temporary id through a declared association are updated in the same commit.
- A rollback restores the state saved in the change itself and only the parts the change still owns, see [writing data](writing-data.md#rolling-back).

## Uploads

A file upload is a pending operation of its record. While it runs, `record.sync.state` is `pending` or `sending`, and `record.upload` adds the stage, the sent bytes and the fraction. `retry()` and `discard()` work the same way. See [Files](files.md#uploads).

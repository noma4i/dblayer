# Sync state of a record

Every record knows whether the server has seen it. The state is live like everything else, so you can build UI and logic on it: show a spinner on a moment that is still being sent, a retry button on a message that failed, a badge on a record that has unsaved edits.

## What a record exposes

| Property | Meaning |
| -------- | ------- |
| `sync.state` | `synced`, `pending`, `sending` or `failed` |
| `sync.actions` | actions waiting for the server, in order (`create`, `update`, `destroy`, custom actions) |
| `sync.dirtyFields` | fields changed locally and not yet confirmed |
| `sync.error` | the last error (kind and text) when the state is `failed` |
| `sync.attempts` | how many times sending was tried |
| `sync.since` | when the record entered this state |
| `sync.isRefreshing` | the record is being reloaded from the server right now |
| `isLocal` | the record has a temporary id and the server does not know it yet |
| `isSynced`, `isPending`, `isFailed` | shortcuts for the state |

| State | When |
| ----- | ---- |
| `synced` | nothing is waiting for the server |
| `pending` | a change is queued and sending has not started (including no network) |
| `sending` | the change is on its way and no answer has arrived |
| `failed` | the server refused the change, or the answer could not be understood, and the record is kept for you to decide |

## Example: a moment that is still being sent

```swift
struct MomentRow: View {
  let moment: Moment

  var body: some View {
    HStack {
      Text(moment.text)
      if moment.isLocal { ProgressView() }
      if moment.sync.state == .failed {
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
    if (moment.sync.state == SyncState.Failed) {
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

Use them for an "unsent" banner, a retry-all button or a badge with the number of pending changes.

## What happens when the server refuses

You choose per action, in the model file:

| Setting | Result |
| ------- | ------ |
| `on_failure: :rollback` (default) | the local change is rolled back in one commit (a created record disappears) and the error is reported |
| `on_failure: :keep` | the record stays with `state = failed` and its error, and waits for your decision |

```ruby
create with: CreateMessage, on_failure: :keep
```

A kept record has two gestures:

```swift
await message.retry()     // back to pending, attempts reset
await message.discard()   // roll back the local change and drop it from the queue
```

```kotlin
message.retry()     // back to pending, attempts reset
message.discard()   // roll back the local change and drop it from the queue
```

A failed record does not block other records in the queue.

## Refreshing

Two kinds of refreshing exist:

- A whole list: the loading state of the relation (`idle`, `loading`, `failed`) is live, see [live data](live-data.md).
- One record: `moment.refresh()` reloads it, and `moment.sync.isRefreshing` is true until the answer arrives. A record can be `synced` and refreshing at the same time.

## Guarantees

- The state is part of every delivery: a state change without a data change still reaches observers, in order.
- A record created offline stays `pending` across restarts and is sent when the network is back.
- When the server answers, the temporary id is replaced by the real one in one commit and the state becomes `synced`. Observers see the swap as `replaced`.
- Rows of other models that point at the temporary id are updated in the same commit.

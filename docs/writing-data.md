# Writing data

All writes are optimistic: they apply to the local database at once, so every live read updates immediately. A write that must reach the server is also placed in a durable queue in the same commit.

```swift
let moment = try await Moment.create(text: "hello", media: nil)
try await moment.update(text: "hello world")
try await moment.destroy()
```

```kotlin
val moment = Moment.create(text = "hello", media = null)
moment.update(text = "hello world")
moment.destroy()
```

## What a write does

1. Plans the change: the row, cascades for `dependent: :destroy`, parent touches for `touch: true`.
2. Commits row and queue entry in one transaction.
3. Observers see the change as one transition.
4. Sync sends the queued mutation (see [server sync](server-sync.md)).

| Call | Local effect | Queued action |
| ---- | ------------ | ------------- |
| `create` | new row with a temporary id (`temp-...`) | `create` |
| `update` | patch merged into the current row, no commit if nothing changes | `update` |
| `destroy` | row and its dependent children removed | `destroy` |

`update` keeps fields you did not mention. A patch that changes nothing creates no commit and no queue entry.

Every record carries its sync state (`pending`, `sending`, `synced`, `failed`, `deliveryUnknown`), so you can show that a new moment is still being sent. See [sync state](sync-state.md).

## Temporary ids

A created row has a temporary id until the server answers. When the response arrives, the row's id is replaced in one commit. Observers receive the swap as a `replaced` entry in the diff, so the row does not disappear and appear again. Rows that point at the temporary id are updated in the same commit.

## Failures

- A mutation is sent once. It is never retried by itself, neither after a network error nor after a server error status.
- If the connection breaks after sending started, or no readable answer arrives, the record is marked `deliveryUnknown` and waits for you.
- A server refusal rolls the local change back with a commit and reports the error, observers see the reverse transition. With `on_failure: :keep` the record stays as `failed` and waits for you.
- A `failed` or `deliveryUnknown` record is sent again only by `retry()`, or rolled back by `discard()` ([sync state](sync-state.md)).
- Responses that cannot be understood are moved to quarantine instead of being lost.
- The queue lives in the database and every queued write is durable, so pending writes survive restarts and are sent when the queue next runs. A write that was `sending` when the app stopped is `deliveryUnknown` after the restart.

```swift
try await message.retry()      // send the same change again
try await message.discard()    // roll the change back and drop it
```

```kotlin
message.retry()      // send the same change again
message.discard()    // roll the change back and drop it
```

## Custom actions

An action is a named mutation with its own optimistic effect:

```ruby
action :like do
  mutation LikeMoment
  input    momentId: :id
  optimistic likedByMe: true
  refetch  :feed
end
```

`optimistic` is applied locally at once and rolled back if the server refuses. `refetch` marks the listed relations stale after success. See [server sync](server-sync.md).

## Replacing data

`replace` swaps the full content of a list in one commit: new rows are inserted or updated, rows that are gone are removed, rows with identical content are not touched. Observers get exactly one delivery and never an empty list in between. Pull-to-refresh uses it.

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

`create` takes the fields that the `create` line maps in `input:`, or every field when there is no `input:`. Add `local: [:userId]` to the `create` line for fields you set locally but do not send (the author of a message, for example). Fields the server owns (ids, counters, dates) are left out of the call and the local row gets a zero value for each until the answer arrives: `""`, `0`, `false`, `[]`, `{}`, the first value of an enum, and the current time for a date.

## What a write does

1. Plans the change: the row, cascades for `dependent: :destroy`, parent touches for `touch: true`.
2. Commits row and queue entry in one transaction.
3. Observers see the change as one transition.
4. Sync sends the queued mutation (see [server sync](server-sync.md)).

| Call | Local effect | Queued action |
| ---- | ------------ | ------------- |
| `create` | new row with a temporary id (`temp-...`). A `create` with an id that already exists is an error (`AlreadyExists`), change an existing row with `update` | `create` |
| `update` | patch merged into the current row, no commit if nothing changes | `update` |
| `destroy` | row and its dependent children removed | `destroy` |

`update` keeps fields you did not mention. A patch that changes nothing creates no commit and no queue entry.

Every record carries its sync state (`pending`, `sending`, `synced`, `failed`, `deliveryUnknown`), so you can show that a new moment is still being sent. See [sync state](sync-state.md).

## Temporary ids

A created row joins, at the front, the list of every remote relation whose params it carries and that has no constant variable filter, so a live list shows it at once. A created row has a temporary id until the server answers. When the response arrives, the row's id is replaced in one commit. Observers receive the swap as a `replaced` entry in the diff, so the row does not disappear and appear again. Rows and queued changes that point at the temporary id through a declared `belongs_to`, `has_many` or `has_one` are rewritten in the same commit, so a child created before its parent is sent carries the real parent id.

`settled()` waits until the server has answered every queued change of a record and throws on a refusal, which is how a screen shows a save error for an optimistic `update`:

```swift
try await user.update(name: name)
try await user.settled()
```

```kotlin
user.update(name = name)
user.settled()
```

To wait for the server id, for example to open the screen of the record you just created, call `confirmedId()`. It returns the real id once the queued `create` is answered and throws if the server refuses it or the device is offline:

```swift
let chat = try await Chat.create(name: "Trip", userIds: ids)
let chatId = try await chat.confirmedId()
```

```kotlin
val chat = Chat.create(name = "Trip", userIds = ids)
val chatId = chat.confirmedId()
```

## Rolling back

Every queued change keeps what it replaced: the old field values, or the destroyed rows with their files and list positions.

- A rollback gives back only what the change still owns. A field edited later, or overwritten by the server, keeps its newer value.
- Rolling back an older edit while a newer edit of the same field is queued hands the old value to the newer edit. Rolling back both, in either order, ends at the original value.
- A created row disappears with the changes queued on it, whether it has a temporary id or one you chose.
- Destroying a row with a temporary id whose `create` has not started removes the `create` and every change queued on it without any network call. If the `create` is already in flight, the destroy waits for the answer and is then sent with the real id.
- When the server deletes a row (an event or a list refresh), the changes still queued on it are closed without being sent and without a rollback, because a rollback would bring the deleted row back.
- A destroyed row keeps its file bytes until the server confirms the destroy. A refusal brings back the row, the files and the list positions.

## Failures

- If the network is unreachable before the transfer starts (no connection, connection refused, offline), the record stays `pending` and is sent again by itself with a growing pause, up to 40 attempts. Offline spends no attempts: the change waits for `setOnline(true)`. After 40 attempts the change fails like a refusal.
- If the connection breaks after the transfer started, or no readable answer arrives, the record is marked `deliveryUnknown` and is never sent again by itself, because the server may have applied it.
- A server refusal rolls the local change back with a commit and reports the error, observers see the reverse transition. With `on_failure: :keep` the record stays as `failed` and waits for you.
- A destroyed record disappears at once. If the destroy fails for good (a refusal or 40 unreached attempts), the record comes back with `state = failed` and the error, like a kept update. `retry()` hides it again and sends the destroy, `discard()` keeps the record and drops the destroy. `discard()` also takes back changes that are still `pending`, and refuses with `InFlight` while one of the changes is being sent. `on_failure: :keep` is not allowed on `destroy`.
- A `failed` or `deliveryUnknown` record is sent again only by `retry()`, or rolled back by `discard()` ([sync state](sync-state.md)). `retryAll()` does it for every such record.
- Responses that cannot be understood are moved to quarantine instead of being lost.
- The queue lives in the database and every queued write is durable, so pending writes survive restarts and are sent when the queue next runs. A write that was `sending` when the app stopped is `deliveryUnknown` after the restart.

```swift
try await message.retry()      // send the same change again
try await message.discard()    // roll the change back and drop it
try await sync.retryAll()      // send every failed and deliveryUnknown change again
```

```kotlin
message.retry()      // send the same change again
message.discard()    // roll the change back and drop it
sync.retryAll()      // send every failed and deliveryUnknown change again
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

An action can take arguments. `args` declares them (`:bool`, `:int`, `:num`, `:string`, `:id`). `input` and `optimistic` can use an argument by name:

```ruby
action :mute do
  mutation   ChatMute
  args       mute: :bool
  input      chatId: :id, mute: :mute
  optimistic muted: :mute
  wrap       :input
  root       "chatMute.chat"
end
```

```swift
try await chat.mute(mute: true)
```

```kotlin
chat.mute(mute = true)
```

`optimistic` is applied locally at once and rolled back if the server refuses. `refetch` marks the listed relations stale after success. See [server sync](server-sync.md).

## Replacing data

`replace` swaps the full content of a list in one commit: new rows are inserted or updated, rows that are gone are removed, rows with identical content are not touched. Observers get exactly one delivery and never an empty list in between. Pull-to-refresh uses it.

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

## Values the row does not store

A mutation may need a value that is not a field of the model, such as a signed upload id or the id of an attached record. Declare it with `args:` on `create` or `update`, and send it under its mutation variable with `input:`:

```ruby
create with: SendMessage, wrap: :input, input: { chatId: :chatId, body: :body, file: :file, attachedUserId: :attachedUserId }, args: { file: :string?, attachedUserId: :id? }
```

```swift
let message = try await Message.create(chatId: chat.id, body: "hi", attachedUserId: "42", file: signedId)
```

```kotlin
val message = Message.create(chatId = chat.id, body = "hi", attachedUserId = "42", file = signedId)
```

An arg has a kind (`:bool`, `:int`, `:num`, `:string`, `:id` or `:ids`). A trailing `?` makes it optional: the call may leave it out and the variable is then not sent. The generated call takes the args after the fields, in alphabetical order. The args of `update` are always optional.

An arg goes into the queued change and is never stored in the row, so no read shows it. The queue is durable: after a restart the change is sent with the same args. Every arg must be named by a value of `input:`, and an arg may not have the name of a field. An `:id` arg that holds a whole number is sent as a JSON number, like an `id` field. Actions take the same `args` (see Custom actions).

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

A created row joins, at the front, the list of every remote relation whose params it carries and that has no constant variable filter, so a live list shows it at once. A relation without params takes a created row only when it declares `joins: :created` (`remote :owned, joins: :created do ... end`): the list maps to a server query whose scope the client cannot evaluate, so the author says that every row this device creates belongs to it. A created row has a temporary id until the server answers. When the response arrives, the row's id is replaced in one commit. Observers receive the swap as a `replaced` entry in the diff, so the row does not disappear and appear again. Rows and queued changes that point at the temporary id through a declared `belongs_to`, `has_many` or `has_one` are rewritten in the same commit, so a child created before its parent is sent carries the real parent id.

`settled()` waits until the server has answered every queued change of a record and throws on a refusal, which is how a screen shows a save error for an optimistic `update`:

```swift
try await user.update(name: name)
try await user.settled()
```

```kotlin
user.update(name = name)
user.settled()
```

A refusal is kept until `settled()` or `confirmedId()` reads it, even when the server answered before you called them, and each refusal is reported once. Kept refusals live in memory: after a restart the row state (`failed`, `needsAttention`) is what remains. At most 2048 unread refusals are kept, the oldest is dropped past that and counted in the core counter `lost_outcomes`.

To wait for the server id, for example to open the screen of the record you just created, call `confirmedId()`. It returns the real id once the queued `create` is answered and throws if the server refuses it or the device is offline:

```swift
let chat = try await Chat.create(name: "Trip", userIds: ids)
let chatId = try await chat.confirmedId()
```

```kotlin
val chat = Chat.create(name = "Trip", userIds = ids)
val chatId = chat.confirmedId()
```

A server row can reach the device before the answer of the mutation that created it (an event of the sent message). Add `correlate: [:chatId, :userId, :kind, :body]` to the `create` line, or a `correlate :chatId, :body` line to a custom action, to name the fields that identify one sent record, dotted paths such as `"media.blobId"` read into objects. A new server row replaces the temporary row of a sent, not yet answered create when every named field is non-empty and equal on both. The swap is the same as for the answer: observers get a `replaced` entry, referencing rows and queued changes move to the real id, and the answer that follows finds nothing left to swap. A row that matches no sent record, or more than one, is stored as a new row.

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

Every action declares its local effect: `optimistic` fields, or `local_effect :none, reason: "..."` when the server alone owns the result. The generator refuses an action with neither. `UNDECLARED_ACTIONS` in `gen/src/check/mod.rs` lists the existing actions that still declare neither; the list only shrinks.

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

An array field holds records such as reactions. `optimistic` changes one element of it by a key and leaves the others, so two writers do not overwrite each other:

```ruby
action :react do
  mutation   MessageReact
  args       userId: :id, reactionBody: :string
  input      messageId: :id, reactionBody: :reactionBody
  optimistic reactions: { by: :userId, element: { userId: :userId, body: :reactionBody } }
end
```

The element replaces the stored element with the same `by` value, or is appended when there is none. When every field of the element except the key is empty (`null`, `""`, `[]`, `{}`), the stored element is removed instead. A refused mutation restores the whole row like any other optimistic change.

```swift
try await chat.mute(mute: true)
```

```kotlin
chat.mute(mute = true)
```

## What the server answered

`result` declares the scalar fields of the mutation payload that the app needs, for example the text a server shows after a send (`:string`, `:int`, `:num`, `:bool` or `:id`). The payload is the object the mutation field returns, the first part of `root`. An action with `result` returns an answer handle instead of the model: `model` is the optimistic record, `answer()` waits for the server and returns a generated type with one optional property per declared field.

```ruby
action :send_gift do
  mutation GiftSend
  args     giftId: :string
  input    userId: :id, giftId: :giftId
  wrap     :input
  root     "giftSend.user"
  result   successMessage: :string
end
```

```swift
let sent = try await user.sendGift(giftId: "g1")
let text = try await sent.answer().successMessage
```

```kotlin
val sent = user.sendGift(giftId = "g1")
val text = sent.answer().successMessage
```

`answer()` throws when the server refuses, even if the refusal came before the call, and when the device is offline. A field the server left out, or typed differently, is `nil` (`null` in Kotlin). The answer is read once and lives in memory like a kept refusal, so after a restart only the stored record remains.

`optimistic` is applied locally at once and rolled back if the server refuses. `refetch` marks the listed relations stale after success. See [server sync](server-sync.md).

## Replacing data

`replace` swaps the full content of a list in one commit: new rows are inserted or updated, rows that are gone are removed, rows with identical content are not touched. Observers get exactly one delivery and never an empty list in between. Pull-to-refresh uses it.

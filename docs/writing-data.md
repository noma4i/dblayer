# Writing data

Opening a database after a model schema change clears affected cached rows. Queued operations and session state remain.
Signing out is `sessionReset`. It ends the work of the old session, clears the credential and the account data:

```swift
try await DB.sessionReset()
```

```kotlin
DBlayer.sessionReset()
```


A write applies to the local database at once, so every open stream updates immediately. Every write that reaches the server (`create`, `update`, `act`, `destroy`, a send and a media upload) is also placed in a durable queue in the same commit. The queue keeps it while the network is away and after the app closes.

```swift
guard let moment = try await Moment.create(text: "hello", media: nil, in: scope).optimisticValue else { return }
try await moment.update(text: "hello world")
try await moment.destroy()
```

```kotlin
val moment = requireNotNull(Moment.create(text = "hello", media = null, account = scope).optimisticValue)
moment.update(text = GraphQLNullable.Value("hello world"))
moment.destroy()
```

In `update` and in `set`, an attribute that can be null takes three states: leave it alone, clear it, or set a value. Swift passes `String??` (omit, `.some(nil)`, `.some("x")`) and Kotlin passes `GraphQLNullable` (`Omitted`, `Null`, `Value`). An attribute that cannot be null takes a plain optional: omit it or set a value.

```swift
try await moment.update(text: .some(nil))
```

```kotlin
moment.update(text = GraphQLNullable.Null)
```

`create` takes the attributes that `operation :create` maps with `variables`, or every attribute when no mapping exists. Add `local :userId` inside `operation :create` for attributes you set locally but do not send. `local :userId, from: { of: CurrentUser, field: :id }` fills the attribute from a singleton row. Repeat `local` with different destinations and sources. Generated `create` makes each sourced argument optional. A non-null explicit value wins. An absent singleton leaves the attribute unset. Duplicate destinations and incompatible types fail generation. `update` accepts plain `local` and rejects `local from:`.

```swift
let message = try await Message.create(text: "hello", in: scope).optimisticValue
```

```kotlin
val message = Message.create(text = "hello", account = scope).optimisticValue
``` Attributes the server owns (ids, counters, dates) are left out of the call and the local row gets a zero value for each until the answer arrives: `""`, `0`, `false`, `[]`, `{}`, the first value of an enum, and the current time for a date.

## Values the row does not store

A mutation may need a value that is not an attribute of the model, such as a signed upload id or the id of an attached record. Declare it with `parameter` inside `operation :create` or `operation :update`. Map it to a mutation variable with `variables`:

```ruby
operation :create do
  mutation "SendNote"
  wrap "input"
  variables groupId: :groupId, body: :body, file: :file, attachedUserId: :attachedUserId
  parameter :file, :string, null: true
  parameter :attachedUserId, :id, null: true
end
```

```swift
let message = try await Note.create(groupId: group.id, body: "hi", attachedUserId: "42", file: signedId, in: scope).optimisticValue
```

```kotlin
val message = Note.create(groupId = group.id, body = "hi", attachedUserId = "42", file = signedId, account = scope).optimisticValue
```

A parameter has a type (`:bool`, `:int`, `:num`, `:string`, `:id` or `[:id]`). `null: true` makes it optional: the call may leave it out and the variable is then not sent. The generated call takes the parameters after the attributes, in alphabetical order. The parameters of `update` are always optional.

A parameter goes into the queued change and is never stored in the row, so no read shows it. The queue is durable: after a restart the change is sent with the same parameters. Every parameter must be used by a value of `variables`, and a parameter may not have the name of an attribute. An `:id` parameter that holds a whole number is sent as a JSON number, like an `:id` attribute. It fits a GraphQL `ID` and an `Int`. Custom operations take the same `parameter` lines (see Custom operations). `dblayer-gen --schema` checks each parameter against the input of the mutation (see [models](models.md#check-against-the-server-schema)).

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

`update` keeps attributes you did not mention. A patch that changes nothing creates no commit and no queue entry.

Every record carries its sync state (`pending`, `sending`, `synced`, `failed`), so you can show that a new moment is still being sent. See [sync state](sync-state.md).

## Temporary ids

A created row joins a list only by `insert_on: [:create]`: a loaded remote scope without parameters that declares it takes the row at the front (`scope :owned, insert_on: [:create] do ... end`). A scope with parameters or a column filter does not take a created row: the row enters it when a page of that scope lands, or, for a column filter, when a later write of the row makes it match. A created row has a temporary id until the server answers. When the response arrives, the row's id is replaced in one commit. Readers receive the swap as a `rewrite` edit that carries the `temporary` id, so the row does not disappear and appear again. Rows and queued changes that point at the temporary id through a declared `belongs_to`, `has_many` or `has_one` are rewritten in the same commit, so a child created before its parent is sent carries the real parent id.

A write that names a row whose `create` is not answered yet waits for that answer and goes out with the real id, at any depth of its values. If the `create` ends without a server row (the server refuses it, it is discarded or it expires), the waiting write ends rejected and sends no request. A query, a mutation or a session call whose values hold a temporary id is refused with `Invalid`. A `create` operation cannot map a variable to the row `id`: the generator reports it.

Each mutation returns a receipt. Its `optimisticValue` is the local value and its `revision` identifies the commit. `confirmed()` waits for that operation alone.

If the storage format changes, the library clears the server cache and keeps queued intentions.

```swift
let receipt = try await user.update(name: name)
let confirmed = try await receipt.confirmed()
```

```kotlin
val receipt = user.update(name = name)
val confirmed = receipt.confirmed()
```

The result is retained while the receipt is held. A confirmed result contains the server value. A session end throws `NoSession`, a closed database throws `Closed`.

`confirmed()` returns the value or throws. A refusal throws `Rejected` with the whole `OperationFailure` and `kept`: `true` when the failed change stays for `retry` or `discard`, `false` when it was rolled back. The end of the wait throws the error of its cause (`Closed`, `NoSession`).

Pass `discardOnFailure: true` to discard a failed operation before the call returns. The refusal keeps its original failure and reports `kept = false` after the rollback and file removal attempt.

```swift
let confirmed = try await receipt.confirmed(discardOnFailure: true)
```

```kotlin
val confirmed = receipt.confirmed(discardOnFailure = true)
```

```swift
do {
  let confirmed = try await receipt.confirmed()
  show(confirmed.value, confirmed.answerJson)
} catch let DbError.Rejected(failure, kept) {
  showRefusal(failure, kept: kept)
}
```

```kotlin
try {
    val confirmed = receipt.confirmed()
    show(confirmed.value, confirmed.answerJson)
} catch (error: DbException.Rejected) {
    showRefusal(error.failure, error.kept)
}
```

`EndCause` names why the command ended: `sessionEnded`, `closed`, `cancelled`, `discarded`, `invalid`, `storage` or `internal`. A command that ended is not retried in the next account.

```swift
let receipt = try await Note.create(groupId: group.id, body: "hi", in: scope)
let noteId = try await receipt.confirmed().value?.id
```

```kotlin
val receipt = Note.create(groupId = group.id, body = "hi", account = scope)
val noteId = receipt.confirmed().value?.id
```

A server row can reach the device before the answer of the mutation that created it (an event of the sent message). Add `match_by "groupId", "userId", "kind", "body"` to the `create` operation, or to a custom operation, to name the attributes that identify one sent record, dotted paths such as `"media.blobId"` read into objects. A new server row replaces the temporary row of a sent, not yet answered create when every named field is non-empty and equal on both. The swap is the same as for the answer: readers get a `rewrite` edit with the `temporary` id, referencing rows and queued changes move to the real id, and the answer that follows finds nothing left to swap. A row that matches no sent record, or more than one, is stored as a new row.

## Rolling back

Every queued change keeps what it replaced: the old field values, or the destroyed rows with their files and list positions.

- A rollback gives back only what the change still owns. A field edited later, or overwritten by the server, keeps its newer value.
- Rolling back an older edit while a newer edit of the same field is queued hands the old value to the newer edit. Rolling back both, in either order, ends at the original value.
- A created row disappears with the changes queued on it, whether it has a temporary id or one you chose.
- Destroying a row with a temporary id whose `create` has not started removes the `create` and every change queued on it without any network call. If the `create` is already in flight, the destroy waits for the answer and is then sent with the real id.
- When the server deletes a row (an event or a list refresh), the changes still queued on it are closed without being sent and without a rollback, because a rollback would bring the deleted row back.
- A destroyed row keeps its file bytes until the server confirms the destroy. A refusal brings back the row, the files and the list positions.

## Failures

If the mutation input type accepts `clientMutationId`, the generator marks it in the spec without an extra model field.
The library stores the key in the operation column before the first send. A retry of the same operation uses the same key after restart.
A new operation gets a new key. An explicit app value takes priority.
This rule is the same for Swift and Kotlin, including the final upload mutation.

[Batch uploads](files.md#create-several-records-with-uploads) create several local records in one operation and submit one ordered list after every remaining file finishes.
Their `match_by` attributes match each server record to its temporary record, independently of response order.
With `on_failure: :keep`, a refusal such as `IN_PROGRESS` or `DUPLICATE_CLIENT_KEY` keeps the row in `failed`.
`retry(action)` repeats the stored key.

- If the network is unreachable before the transfer starts (no connection, connection refused, offline), the record stays `pending` and is sent again by itself with a growing pause, up to 40 attempts. Offline spends no attempts: the change waits until the network returns. `on_failure: :rollback` applies only to confirmed server refusals, while 40 undelivered attempts always retain `failed` and the edit.
- If the connection breaks after the transfer started, or no readable answer arrives, the library sends the same saved request again with the same request id, so the server applies it once.
- A server refusal rolls the local change back with a commit and reports the error, observers see the reverse transition. With `on_failure: :keep` the record stays as `failed` and waits for you.
- A destroyed record disappears at once. A refusal of the server brings the record back as it was, without a `failed` state, and `confirmed` throws `Rejected` with `kept: false`. After 40 unreached attempts the destroy is `failed`: `retry(action)` sends it again, `discard(action)` keeps the record and drops the destroy. `discard(action)` also takes back changes that are still `pending`, and refuses with `InFlight` while one of the changes is being sent. `on_failure: :keep` is not allowed on `destroy`.
- A `failed` record is sent again only by `retry(action)`, or rolled back by `discard(action)` ([sync state](sync-state.md)).
- A temporary row left without its operation after a crash, and a damaged reset intent, are moved to quarantine instead of being lost.
- The queue lives in the database and every queued write is durable, so pending writes survive restarts and are sent when the queue next runs. A write that was `sending` when the app stopped is sent again with the same request id after the restart.

```swift
try await message.retry(action: nil)      // send the same change again
try await message.discard(action: nil)    // roll the change back and drop it
```

```kotlin
message.retry(null)      // send the same change again
message.discard(null)    // roll the change back and drop it
```

## Custom operations

A custom operation is a named mutation with its own optimistic effect:

```ruby
operation :like do
  mutation "LikeMoment"
  variables momentId: :id
  optimistic likedByMe: true
  refetch :feed
end
```

Every named model operation declares `optimistic` attributes, or `optimistic false` when only the server changes the row. The generator rejects a missing declaration. `create`, `update` and `destroy` need none.

An operation can take parameters. `parameter` declares them (`:bool`, `:int`, `:num`, `:string`, `:id`). `variables` and `optimistic` can use a parameter by name. A parameter that nothing uses is an error:

```ruby
operation :mute do
  mutation "GroupMute"
  parameter :mute, :bool
  variables groupId: :id, mute: :mute
  optimistic muted: :mute
  wrap "input"
  response from: "groupMute.group"
end
```

An enum parameter can choose the optimistic value: `optimistic connection: { from: :decision, map: { ACCEPT: "CONNECTED", REJECT: "NEVER_MET" } }` writes the literal of the value the call passes. The map covers every value of the schema enum, see [models](models.md). The refusal of the mutation restores the old value.

A bool parameter can set a list and a counter. `include:` puts the id of a row, usually the signed-in user, at the front of a list attribute when the flag is true, without a repeat, and takes it out when the flag is false. `at_least:` raises a number attribute to the given value when the flag is true and leaves it alone when the flag is false.

```ruby
operation :react do
  mutation "MomentReact"
  parameter :reacted, :bool
  variables momentId: :id, reacted: :reacted
  optimistic reacted: :reacted,
             visitorIds: { include: { of: CurrentUser, field: :id }, when: :reacted },
             visitsCount: { at_least: 1, when: :reacted }
end
```

```swift
try await moment.react(reacted: true)
```

```kotlin
moment.react(reacted = true)
```

The row named by `of:` is a singleton model, or any model with `row: :parameter`. A call that cannot find that row leaves the list unchanged. A refused mutation gives back the list and the number.

An array attribute holds records such as reactions. `optimistic` changes one element of it by a key and leaves the others, so two writers do not overwrite each other:

```ruby
operation :react do
  mutation "NoteReact"
  parameter :userId, :id
  parameter :reactionBody, :string
  variables messageId: :id, reactionBody: :reactionBody
  optimistic reactions: { by: :userId, element: { userId: :userId, body: :reactionBody } }
end
```

The element replaces the stored element with the same `by` value, or is appended when there is none. When every attribute of the element except the key is empty (`null`, `""`, `[]`, `{}`), the stored element is removed instead. A refused mutation restores the whole row like any other optimistic change.

When the nested object declares its model, `attribute :user, ReactionUser, null: true, ref: User, id: :userId do ... end`, the optimistic element carries that object built from the local row of `User` whose id is the element's `userId`. Each attribute of the object takes the value of the column with its name (or its `from` path) in that row, and `id` takes the key. An attribute that is not a column of the row stays absent, and without a local row, or without `ref:`, the object stays absent. `ref:` must name a declared model and `id:` an attribute declared beside the object, or generation fails. The server answer replaces the element as before.

`by` and `element` name declared attributes. An attribute such as `userId` with `from: "user.id"` reads that nested response path and keeps the same key for optimistic replacement and removal.

```swift
try await group.mute(mute: true)
```

```kotlin
group.mute(mute = true)
```

## Changes beyond the row

An operation writes its own row. Before the server answers it changes other rows and lists only through the four effects `removes_receiver`, `copies_receiver_to`, `joins_scope` and `leaves_scope` (see [models](models.md#effects-of-an-operation-before-the-answer)).
The rows the server answers with arrive through `upsert Model, from: "path"` and `response from:`. A scope that the answer changes is read again with `refetch :scope`.

```ruby
operation :block do
  mutation "UserBlock"
  variables userId: :id
  wrap "input"
  response :ignore
  refetch :blocked
end

operation :send_gift do
  mutation "GiftSend"
  parameter :giftId, :string
  parameter :chatId, :id
  variables userId: :id, giftId: :giftId
  wrap "input"
  upsert Message, from: "giftSend.message"
  returns do
    attribute :successMessage, :string
  end
  response from: "giftSend.user"
end
```

```swift
try await user.block()
let sent = try await user.sendGift(giftId: "g1", chatId: chat.id)
let confirmed = try await sent.confirmed()
```

```kotlin
user.block()
val sent = user.sendGift(giftId = "g1", chatId = chat.id)
val confirmed = sent.confirmed()
```

## Writing without a network

Every write is queued durably. It writes the row at once, queues the request, and survives a lost network and a closed app. When the network returns, the queue sends the operations by themselves, oldest first. One request is in the network at a time, in write order. A write never fails because the network is away.

```swift
let sent = try await Message.create(chatId: chat.id, text: "hi", in: scope)
let confirmed = try await sent.confirmed()   // throws Rejected, NoSession or Closed
```

```kotlin
val sent = Message.create(chatId = chat.id, text = "hi", account = scope)
val confirmed = sent.confirmed()             // throws Rejected, NoSession or Closed
```

Every queued write shows in `operations()` with its status, and a refused write is retried from that list: see [the queue of operations](sync-state.md#the-queue-of-operations).

An action that loses its meaning with time declares how long it may wait. `expires_after:` takes whole seconds:

```ruby
operation :typing, expires_after: 30 do
  mutation "ChatTyping"
  variables chatId: :chatId
end
```

When the library picks a queued operation for its first send and its waiting time is over, it does not send it. An operation that was sent at least once is never expired. It rolls back the optimistic change and ends the operation with the refusal `Rejected` and the failure kind `expired`. No timer runs: the check happens when the network returns, when a request ends or when the app opens. Without `expires_after` an operation waits as long as needed.

`confirmed()` waits for the one final value, also while the network is away. A command of an ended session ends with it and does not go to the next account.
A model with `storage: :local` writes rows that only this device holds, see [storage rules](storage-models.md).

## What the server answered

`returns` declares the attributes of the mutation payload that the app needs, such as a success message. An attribute may be an object with its own `do ... end` block, with the same syntax as the `returns` of a standalone operation; every field is nullable. The confirmed result contains them as `answerJson` on Swift and `answerJson` on Kotlin.

```ruby
operation :send_reward do
  mutation "RewardSend"
  parameter :rewardId, :string
  variables userId: :id, rewardId: :rewardId
  optimistic false
  wrap "input"
  returns do
    attribute :successNote, :string
  end
  response from: "rewardSend.user"
end
```

```swift
let sent = try await user.sendReward(rewardId: "g1")
let confirmed = try await sent.confirmed()
```

```kotlin
val sent = user.sendReward(rewardId = "g1")
val confirmed = sent.confirmed()
```

The receipt retains its result. Fields omitted by the server, or returned with another type, are null in the answer JSON.

`optimistic` is applied locally at once and rolled back if the server refuses. `refetch` marks the listed scopes stale after success. See [server sync](server-sync.md).

## Replacing data

Refresh applies the server list in one commit. Observers never receive an empty list between the old and new values.

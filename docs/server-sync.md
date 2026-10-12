# Server sync

DBlayer talks to a GraphQL endpoint. You describe operations and models once. Requests, pagination, retries, refetching and subscriptions are handled for you.

## Connecting

1. Create a transport with the endpoint and a token supplier. The transport reads the current token once per request.
2. Open the database with your models and the transport.
3. Pass your GraphQL operation types. The name and document come from the type, the response root is the first field of the document.

The transport is an interface with three calls: `query`, `mutate` and `subscribe`. Your app gets HTTP and WebSocket implementations for each platform. Tests use a mock transport. A REST transport implements the same interface.

## Responses become rows

Every response, whether from a query, a mutation or a subscription event, goes through the same rules:

| Rule | Behavior |
| ---- | -------- |
| Identity | the objects of a relation are rows of its model and need an `id`. Objects without one are dropped |
| Nesting | objects found at the `from:` path of an association (its name by default) become rows of the target model. `belongs_to` keeps the key (`userId`) on the outer row, `has_many` and `has_one` rows get the outer id, `references` keeps the ids |
| Lists and pages | lists and `edges { node }` with `pageInfo` are recognized from the document and keep server order |
| Partial responses | fields a query did not select are never erased, each field is updated on its own and the newer revision wins |
| Unknown fields | ignored |
| One commit | a whole response lands in one commit |

## Loading

```swift
Moment.feed                      // live; loads if stale, nothing to call
try await Moment.feed.loadMore() // next page
try await Moment.feed.refresh()  // pull to refresh, replaces in one commit
```

```kotlin
Moment.feed                      // live; loads if stale, nothing to call
Moment.feed.loadMore()           // next page
Moment.feed.refresh()            // pull to refresh, replaces in one commit
```

- Fresh data is not requested again. `fresh_for` sets how long data counts as fresh.
- Identical parallel loads become one request.
- Network and server errors of a load are retried within a budget; fatal errors are returned. Mutations are never retried by themselves, see [sync state](sync-state.md).
- The built-in transports tell the core whether the server answered. A GraphQL error, a 401 or a 5xx answer reaches the core as an HTTP error: a mutation is rejected (answers below 500) or delivery unknown (5xx) and is never sent again by itself, a load retries a 5xx. A request that never left the device (offline, host not found, connection refused) is a network error and is retried. A timeout or a lost connection after the request was sent is a network error for a load and delivery unknown for a mutation.
- A response older than the data already stored never overwrites it.
- A row that another response stored while this one was in flight keeps its stored data and still joins the list this response names.
- `max_pages` limits how many pages one load takes.

## Refresh merges, it does not replace

`refresh()` works on one list with its parameters. Only that list changes: other lists keep their content, and nothing is deleted just to be written again.

```swift
try await Moment.feed.refresh()
try await Moment.byUser(userId: 5).refresh()      // only user 5's list
```

```kotlin
Moment.feed.refresh()
Moment.byUser(userId = 5).refresh()               // only user 5's list
```

What a refresh does, in one commit:

- Rows from the response are merged field by field. Rows that did not change keep their revision and are not reported as updated.
- Records that the server has not confirmed yet (a message being sent, a failed one waiting for retry) stay in the list where they are.
- For a paged list the response is the first page. The last row of the response is looked up in the old list. If it is found, the new first page is followed by the old rows that came after it, so a long list that the user scrolled does not jump. If it is not found, the old tail is dropped and `loadMore()` loads the next pages again.
- Rows that are missing from the response leave the list but stay in the database, because they may belong to other lists. A row is deleted when the server says so (`deleted_by`, a null node, a delete event).
- A failed request changes nothing.

An event never loads a list again. A row that arrives by an event is stored like any other row (V2 does the same: the event inserts the row and no list is fetched). A relation that lists every row of its params (`remote :thread, params: [:chatId], joins: :events do ... end`) also gets the new row appended, when that list is loaded, in the same commit as the event. Declare `joins: :events` only for a relation without server-side filters (`variables`), because the library cannot tell from the row whether the server would have listed it. Other lists show the row on their next load. Only a row whose parent is not stored makes the library reload the lists of that parent model.

`refresh(replace: true)` replaces the whole list with the response (unconfirmed records still stay). It is meant for small lists where the server is the only authority.

## Mutations

```ruby
create  with: CreateMoment, input: { text: :text }
update  with: UpdateMoment
destroy with: DeleteMoment
```

Fields of the kind `id` that hold a whole number are sent as JSON numbers. A GraphQL `ID` accepts a number and so does an `Int`, while a string only fits an `ID`.

Servers that take one input object and answer with a payload (the Relay mutation style) use `wrap:` and `root:`:

```ruby
update  with: UpdateChat, wrap: :input, root: "chatUpdate.chat"
action :mute do
  mutation ChatMute
  wrap     :input
  root     "chatMute.chat"
  input    chatId: :id
end

action :send_gift do
  mutation GiftSend
  wrap     :input
  root     "giftSend.user"
  lands    Message, root: "giftSend.message"
  refetch_of CurrentUser, relation: :current
  input    userId: :id, giftId: :giftId
end
```

`response: :ignore` skips reading the response, for a mutation whose answer is not the model's row; add `refetch: [:profile]` (or a `refetch` line in an action) to reload the relation instead.

`wrap: :input` sends `{ input: { ...variables } }`. `root:` is the path of the row inside the response, by default the first field of the operation. `lands Model, root: "a.b"` writes the row at another path of the response into that model, once per line, so a payload that carries several rows lands all of them. The model must exist, and a path that the response lacks lands nothing. `refetch_of Model, relation: :name` marks a relation of another model stale after the action succeeds and reloads it when someone reads it, for counters that live on another row.

`create`, `update` and `destroy` call the named mutation. `input:` maps row fields to variables (same names by default). The response is normalized like any other response and replaces the optimistic row. See [writing data](writing-data.md) for the queue, retries and rollback.

## Subscriptions

```ruby
remote :feed do
  query MomentsFeed
  updates MomentEvents, deleted_by: :destroyed
end
```

- `root: "a.b"` is the path of the row inside the event payload (default: the first field), `id: "messageId"` is the key that holds its id (default: `id`). Use them when the server wraps the row or sends only an id for a delete.
- Events are normalized like responses. `deleted_by:` names the field that marks a deleted row.
- An event changes only the fields it carries. Fields it does not select keep their stored values. The read of the stored row and the write happen in one transaction, so a local edit made at the same moment is never overwritten by an older copy.
- A delete event removes the same rows as a local `destroy`: the row and its `dependent: :destroy` children, in one commit.
- An event that creates a row the library did not have makes the relations of that model stale, like a successful `create`, and refetches the ones someone is reading.
- If the first connection fails, the library retries it with the same growing delay and the same offline rule as a reconnect. `subscribe` does not throw for a refused connection.
- The subscription starts when the first reader connects, is shared by all readers and closes some time after the last reader leaves (30 seconds by default).
- After a dropped connection the library reconnects with a growing delay and refetches the relation, because events may have been missed. Offline (`setOnline(false)`) it does not reconnect and spends no attempts. `setOnline(true)` reconnects at once.
- Bursts of events are merged into one commit.

### Model-level subscriptions

`updates` lives while a screen reads the relation. For events that should reach the database for the whole session (presence, counters), declare `listen` on the model. It takes the same options as `updates`:

```ruby
model User do
  listen "userUpdated", debounce: 300.milliseconds
end
```

```swift
try DB.listen()      // after sign-in
try DB.unlisten()    // on sign-out
```

```kotlin
DBlayer.listen()
DBlayer.unlisten()
```

## Automatic refetching

| Event | Result |
| ----- | ------ |
| Successful `create` or `destroy` of a model, or a subscription event that creates a row | relations rooted in that model become stale and are refetched at once if someone is reading them |
| Successful mutation with `refetch` | the listed relations become stale and are refetched once the loads of the same list that were in flight at the commit have finished, so the reply that lands last is always issued after the commit |
| `fresh_for` expired | refetched when next read or when the app resumes |
| App resumes | the app calls `DB.resume()` (Swift) or `DBlayer.resume()` (Kotlin), stale relations with readers are refetched in small groups |
| Network is back | the queue is sent and stale relations are refetched |
| Subscription reconnects | the relation is refetched |

You never write "if the mutation worked, reload the list".

## State for the UI

Loading state (idle, loading, error with its kind, has more pages, last loaded time) and the outbox (pending writes, send errors) are live too.

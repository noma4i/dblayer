# Server sync

DBlayer talks to a GraphQL endpoint. You describe operations and models once. Requests, pagination, retries, refetching and subscriptions are handled for you.

## Connecting

1. Create a transport with the endpoint and headers (for example an auth token).
2. Open the database with your models and the transport.
3. Register your GraphQL operations: name, document and where the nodes sit in `data`.

The transport is an interface with three calls: `query`, `mutate` and `subscribe`. Your app gets HTTP and WebSocket implementations for each platform. Tests use a mock transport. A REST transport implements the same interface.

## Responses become rows

Every response, whether from a query, a mutation or a subscription event, goes through the same rules:

| Rule | Behavior |
| ---- | -------- |
| Identity | an object with `__typename` and `id` is a row of the model with that name |
| Nesting | a nested object of an association becomes a row of its own model, the outer row keeps the key (`userId`) |
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
- Network and server errors are retried within a budget; fatal errors are returned.
- A response older than the data already stored never overwrites it.
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
- Rows that are missing from the response leave the list but stay in the database, because they may belong to other lists. A row is deleted when the server says so (`deleted_by`, a null node, a delete event), or when the relation says `on_absent :destroy`.
- A failed request changes nothing.

```ruby
remote :by_user do
  query MomentsByUser
  fresh_for 5.minutes
  on_absent :destroy   # rows missing from a complete response are deleted, unconfirmed ones stay
end
```

Use `on_absent :destroy` when the same data is also read with a query on the table (`Moment.where(Moment.userId == id)`), so that rows deleted on the server disappear there too.

`refresh(replace: true)` replaces the whole list with the response (unconfirmed records still stay). It is meant for small lists where the server is the only authority.

## Mutations

```ruby
create  with: CreateMoment, input: { text: :text }
update  with: UpdateMoment
destroy with: DeleteMoment
```

`create`, `update` and `destroy` call the named mutation. `input:` maps row fields to variables (same names by default). The response is normalized like any other response and replaces the optimistic row. See [writing data](writing-data.md) for the queue, retries and rollback.

## Subscriptions

```ruby
remote :feed do
  query MomentsFeed
  updates MomentEvents, deleted_by: :destroyed
end
```

- Events are normalized like responses. `deleted_by:` names the field that marks a deleted row.
- The subscription starts when the first reader connects, is shared by all readers and closes some time after the last reader leaves (30 seconds by default).
- After a dropped connection the library reconnects with a delay and refetches the relation, because events may have been missed.
- Bursts of events are merged into one commit.

## Automatic refetching

| Event | Result |
| ----- | ------ |
| Successful `create` or `destroy` of a model | relations rooted in that model become stale and are refetched at once if someone is reading them |
| Successful mutation with `refetch` | the listed relations become stale |
| `fresh_for` expired | refetched when next read or when the app resumes |
| App resumes | stale relations with readers are refetched in small groups |
| Network is back | the queue is sent and stale relations are refetched |
| Subscription reconnects | the relation is refetched |

You never write "if the mutation worked, reload the list".

## State for the UI

Loading state (idle, loading, error with its kind, has more pages, last loaded time) and the outbox (pending writes, send errors) are live too.

# Live data and queries

Reading data means opening a stream of values from the core.
See [value bridge](value-bridge.md) for snapshots, deltas, epochs and commands.
One owner in the core serves each stream: the query, the count, the row by id, the events of a model and the status of an upload all work the same way.

## Streams

| Call | Delivers |
| --- | --- |
| `scope.stream(query)` | The rows of a query: a snapshot, then deltas. |
| `scope.count(query)` | The number of rows of a query. |
| `scope.entity(Model, id, fetch)` | One row by id. `fetch` tells when the library loads it from the server. |
| `scope.singleton(Model)` | The one row of a model that has a single record. |
| `scope.events(Model)` | The events of a model after the stream opened. Past events are not repeated. |
| `scope.changes()` | One item per commit of the database that changed rows, with the `(model, id)` pairs. It reads no rows and opens no query. |
| `scope.uploadStatus(row, action)` | The status of a media upload while it runs, with `localUrl` of the copy of the library and `processing` while the server works on the file. The last item is `ended` with the cause of the core. |

```swift
let scope = try DB.account()
let feed = Projected<Moment>(try scope.stream(Moment.feed))
let total = ProjectedCount(try scope.count(Moment.where(Moment.userId == id)))
let author = Projected<User>(try scope.entity(User.self, userId, fetch: .ifMissing))
for await item in scope.events(Message.self) { show(item) }
for await item in scope.changes() { refresh(item) }
```

```kotlin
val scope = AccountScope.current()
val feed = Projected(scope.stream(Moment.feed), Moment, screenScope)
val total = ProjectedCount(scope.count(Moment.where(Moment.userId eq id)), screenScope)
val author = Projected(scope.entity(User, userId, ReadFetch.IF_MISSING), User, screenScope)
scope.events(Message).events(Message).collect { item -> show(item) }
scope.changes().collect { item -> refresh(item) }
```

Every delivery of a list stream is one value with its rows, its `state` (`phase`, `error`, `loaded`, `hasMore`, `settled`, `total`) and its `revision`. A screen never reads the rows and the load state in two steps.

A stream lives while its reader lives. Release the model or close its scope and the core drops the stream. A stream has one reader: the first iterator or collector takes it, a second one ends at once with `ended(.invalid)`, and a reader that stops ends the stream in the core also while the `ValueStream` is held.

## What a delta contains

A delta carries the steps from the rows the reader holds to the rows of its revision.

| Part | Meaning |
| --- | --- |
| `edits` | The steps in order, as `ListEdit` values: `insert` and `rewrite` carry the row, `remove` carries the id, `move` carries two indexes. An index is a place in the list as it stands after the steps before. |
| `state` | The load state, `hasMore`, `total`, the error. |

A projection applies the steps in order and decides nothing: the core fixed the order.
A row that cannot be decoded asks the core for one new snapshot with `resync()`.

`Projected.edits` holds the edits of the last delivery. Apply them to a list you keep. A snapshot carries its own edits: the first one inserts every row, a snapshot after `resync()` removes, inserts, moves and rewrites only the rows that differ. A failure of a stream arrives as its last value `ended` with a cause (`Internal` for a delivery that could not be read); reading a stream never throws.
A `rewrite` that the server confirmed carries the `temporary` id the row had when the screen first showed it. A screen keeps the identity of the row with it.

## Reading before the first delivery

`query.cachedRow(id)` reads one row the core already holds and sends no request. The first value of a stream comes from the same cache.

```swift
let row = try await Moment.recent.cachedRow(id)
```

```kotlin
val row = Moment.recent.cachedRow(id)
```

`query.read()` and `query.readRow(id)` read storage once, also when the core holds nothing. They send no request and keep no stream.

```swift
let stored = try await Moment.recent.read()
let one = try await Moment.recent.readRow(id)
```

```kotlin
val stored = Moment.recent.read()
val one = Moment.recent.readRow(id)
```

## Ephemeral values

Ephemeral fields (see [models](models.md)) arrive in the same stream. A write or a timed reset puts the row in a delta, because the value is part of the row the reader holds.
An ephemeral value of an included row reaches the stream of its parent too.

```swift
let user = Projected<User>(try scope.entity(User.self, userId))
// user.value?.typing turns true on a write and false again after reset_after
```

```kotlin
val user = Projected(scope.entity(User, userId), User, screenScope)
// user.value?.typing turns true on a write and false again after reset_after
```

## Media library with marker and tail

A fresh scope sends no requests. A stale scope first reads the marker with `upToSequence` equal to the stored sequence.
Matching revision and prefix counts preserve coverage. If the sequence has grown, the core reads the entire tail by cursor.
A mismatch or a missing stored marker starts the full chain. `hasNextPage == false` ends the chain. A missing or repeated cursor rejects the load.
Rows and the confirmed marker enter one SQLite transaction. An error or cancellation keeps the previous rows and marker, and the stream state shows the error.
Markers are separate for each model, scope, parameter set, direction and database generation. Idle scopes have no periodic requests or wakeups.

Server lists and connections store each model id once, combining selected fields from repeated rows before notifying readers.
The last supplied field value wins. Declared merge rules still apply against stored data.
An explicit null in a top-level `from` path clears the projected nullable field. Missing path keys keep the stored value.

## Queries

```swift
try Moment.where(Moment.userId == id).order(Moment.createdAt.desc).limit(20).offset(40)
Moment.recent                       // a named scope
Moment.ids(["a", "b"])              // the rows with these ids
```

```kotlin
Moment.where(Moment.userId eq id).order(Moment.createdAt.desc).limit(20).offset(40)
Moment.recent                       // a named scope
Moment.ids(listOf("a", "b"))        // the rows with these ids
```

`limit` is at least 1 and `offset` is 0 or more, else the read fails with `Invalid`. A count ignores both.

The builders only fill a typed record that the core compiles to SQL. A relation is optional in it: a load, refresh or load more of a query without a relation fails with `Invalid`.
The count of a relation is a query: `scope.count(...)`. There are no counter caches.
A query is plain data (`model`, `where`, `order`, `limit`, `offset`). The core compiles it to SQL. Every scalar and enum field compares as its own SQLite column, and an index makes the comparison fast.
A row with a null field matches `!(field == value)`.

## Related rows

The rows of a server relation are a query with a `remote` relation: `Chat.list("SECONDARY")` lists the chats of one server list. A change of a row or of its membership in the relation updates the stream. No separate related call exists.

```swift
let secondary = Projected<Chat>(try scope.stream(try Chat.list("SECONDARY")))
```

```kotlin
val secondary = Projected(scope.stream(Chat.list("SECONDARY")), Chat, screenScope)
```

- The query owns the where, order, limit, load, refresh and load-more. `refresh()` and `loadMore()` of the stream act on it.
- `total` is the server count of the relation when the server sends one.

## State of a stream

| Field | Meaning |
| --- | --- |
| `phase` | `idle`, `loading`, `loaded` or `failed`. `failed` shows only while the stream has no rows. |
| `error` | The typed `DbError` (`DbException` on Kotlin) of the last load, `nil` when it succeeded. |
| `loaded` | The revision of the commit of the last load, to compare with the `revision` of a value. It is not a point in time. It is empty before the first load, and 0 for a fresh saved window that has not loaded since `open`. |
| `hasMore` | The relation has a next page that can be loaded. At `max_pages` it is false. |
| `settled` | The first load of the stream ended with an outcome. |
| `total` | The server total of the window, `nil` when the server sent none. |

A list is complete when `settled` is true and `hasMore` is false.

## Model events

An event of `scope.events(Model)` carries `position`, `area`, `kind`, `id` and the rows `before` and `after` the event, `nil` when the row did not exist on that side. A `gap` item tells that events were missed: the library rereads the watched reads of the model by itself, and the app does not read the rows again.

## Text search

Fields listed by `search :text, :tags` use SQLite FTS5. The index changes in the same transaction as the row and follows edits and deletes. The app never writes FTS syntax: the core quotes each word.

```swift
Query<Moment>().search("hel")                          // finds "hello": the last word is a prefix
Query<Moment>().search("hello wor", wholeWords: true)  // every word must match whole
```

```kotlin
Query("Moment", Moment).search("hel")                          // finds "hello": the last word is a prefix
Query("Moment", Moment).search("hello wor", wholeWords = true) // every word must match whole
```

The default match treats the last word as a prefix and the other words as whole words. A query without an order is ranked by relevance. A query has one search, and the model needs at least one searchable field.

## Edge values

The values of a connection edge are read with the `async` `feedEdge(of:)`, see [edge values](models.md#edge-values). They are not part of the row or of a stream, so a change of an edge value delivers nothing.

```swift
let uuid = try await Moment.feedEdge(of: id)?.impressionUuid
```

```kotlin
val uuid = Moment.feedEdge(id)?.impressionUuid
```

## Remote relations are live too

Opening a stream on a scope that has a `query` declaration loads it when its data is stale, keeps it fresh through its `subscribe` subscription while someone reads it, and lets go after the last reader leaves. User gestures stay explicit: `refresh()` and `loadMore()`.

| Call | Request | Keeps a subscription | Use it for |
| ---- | ------- | -------------------- | ---------- |
| a stream | when stale | yes, while read | a screen that shows the relation |
| `load()` | when stale, none when fresh | no | rows that need their relation once |
| `refresh()` | always | no | a user gesture (pull to refresh) |

`load()` makes the same decision as opening a stream: no request when the window is fresh, exactly 1 request when it is stale or never loaded. N parallel loads of one relation make 1 request.
`beginLoad()` starts the same load and returns the interest of the caller at once: `result()` waits for the end, `cancel()` gives up the interest. Callers of one relation share the request. It stops only when every caller cancelled, and the others get its result. `load()` is `beginLoad()` whose interest ends with the task or coroutine that waits. `loadAll()` waits for the same kind of request: cancelling its task or coroutine stops the pages that no other caller waits for.
A window that owes a read after a mutation is read again once while a stream observes it. Without a reader the debt stays until the next read, also after the database is opened again.

```swift
try await Moment.byUser(userId: 5).load()
let request = try Moment.feed.beginLoad()
request.cancel()
```

```kotlin
Moment.byUser(userId = 5).load()
val request = Moment.feed.beginLoad()
request.cancel()
```

See [server sync](server-sync.md) and [threading](threading.md).

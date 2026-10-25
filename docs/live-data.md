# Live data and queries

Server lists and connections store each model ID once, combining selected fields from repeated rows before notifying observers.
The last supplied field value wins. Declared merge rules still apply against stored data.
The `nodes` result retains repeated IDs in server order, while stored list membership remains unique.
An explicit null in a top-level `from` path clears the projected nullable field. Missing path keys preserve the stored value.

Every read of a model is live. A read gives you the current value and a stream of changes. There is no snapshot-only read and no separate observe call: connecting to data is reading it.

In Swift and Kotlin, saved page metadata reads membership and the last member from the current stored scope, without a separate copy of ids.

## What a change contains

| Field | Meaning |
| ----- | ------- |
| `revision` | Commit revision the state reflects. It only grows for one observer |
| `rows` | The first delivery: the whole list in order. Later deliveries: only the rows that were inserted, updated or replaced |
| `diff.inserted`, `removed`, `updated`, `moved` | Row identities that changed |
| `size` | The rows of the core's list after the delivery, before any decode failure of the Swift or Kotlin side. A delivery that does not give this size on the rebuilt list is refused |
| `diff.at` | The final index of every inserted, updated, moved and replaced id. Swift and Kotlin read it from `placed` and `row_indexes` |
| `rows[i].sync` | Sync state of the row, see [sync state](sync-state.md) |
| `diff.replaced` | Pairs `(old id, new id)`, for example a temporary id replaced by the server id, so a UI can keep the same element |

The first delivery contains every row as `inserted`. After it a commit of 1 row on a list of 2000 delivers 1 row, whatever the size of the list. Swift and Kotlin keep the array and apply each delivery in 4 steps: rename the `replaced` ids, drop `removed`, take the `placed` ids out of the array and insert them again by ascending index, then put each delivered row at its index. `Change::apply_to` is the reference of these steps.

A delivery that does not fit the rows of the reader (unknown id, index out of range, wrong `size`) is refused and counted. The reader opens the subscription again and its first delivery is the whole list. After 2 reopens in a row without a good delivery in between, or when a reopen fails, the reader stops, keeps the last good rows and reports `failure` (`Snapshot.failure`, `LiveList.failure`). A row that does not decode keeps its place in the list and is left out of the rows that the reader sees.

A commit of a row that is outside the list and fails the filter of a query on a whole model does not read the list and does not deliver.
A list of a server relation, such as a thread or a feed, follows a commit the same way: the work depends on the rows the commit touched, not on the size of the list.

## Changes without a list snapshot

`query.changes` returns `QueryChanges<Model>` in Swift and `Flow<ListChange<Model>>` in Kotlin.
`inserted` and `updated` contain decoded rows with their final indices.
`removed` contains ids, `moved` contains placements, and `replaced` contains renamed id pairs.
`revision` and `size` describe the delivered state. `failure` reports query or decoding errors.
The first event has kind `replaced` and contains every initial row in `inserted`.
Ordinary events have kind `delta` and contain only changed rows.
The core queues at most 64 unread events for each consumer.
On overflow, it replaces queued deltas with one `replaced` event containing the complete current snapshot.
When receiving `replaced`, clear the consumer list and install `inserted` in order before applying later deltas.
This bounds queued memory without losing the current rows, updates or removals.
Dropping or cancelling a Swift stream or cancelling Kotlin collection releases the subscription and unread events.
Existing `live`, `snapshots` and `LiveList.rows` keep their list behavior.

```swift
for await change in try await Note.where(Note.groupId == groupId).changes.stream() {
  if change.kind == .replaced { replaceAll(change.inserted.map(\.row)) }
  else { applyDelta(change) }
}
```

```kotlin
Note.where(Note.groupId eq groupId).changes.collect { change ->
    if (change.kind == ListChangeKind.Replaced) replaceAll(change.inserted.map { it.row })
    else applyDelta(change)
}
```

## Ephemeral values

Ephemeral fields (see [models](models.md)) arrive in the same change. A write or a timed reset puts the row in `diff.updated` and `revision` stays the same, because nothing was written to the database. Two deliveries of one observer can therefore share a revision, and the later one is always newer.

```swift
@Live var user: User?
// user.typing turns true on a write and false again after reset_after
```

```kotlin
val user by User.find(userId).collectAsLive()
// user.typing turns true on a write and false again after reset_after
```

A change of an ephemeral value on an included row reaches the observer of the parent too. When `User.typing` of a group author changes, an observer of groups that includes `author` gets the group in `diff.updated`, with the new value in `related`.

```swift
@Live var group: Group?
// group.author.typing changes -> the group row is in diff.updated
```

```kotlin
val group by Group.find(groupId).collectAsLive()
// group.author.typing changes -> the group row is in diff.updated
```

## Reading before the first delivery

A new subscription delivers its first rows from a library thread, so a screen that has to show cached rows at once cannot wait for it. `cached()` answers from memory, without reading the database file. The copy and the decode of the rows run off the caller thread, so you await it and the main thread never does that work. It gives `nil` (Swift) or `null` (Kotlin) when the query is not warm. That means "not in memory", never "no rows".

```swift
let list = Moment.recent
if let cached = try await list.cached() { show(cached.value, revision: cached.revision) }   // rows and revision, or nil
try await list.warm()                    // read once, keep in memory
let row = try await list.cachedRow(id)     // one row from the row cache, or nil
```

```kotlin
val list = Moment.recent
list.cached()?.let { show(it.value, it.revision) }   // rows and revision, or null
list.warm()                              // read once, keep in memory
val row = list.cachedRow(id)             // one row from the row cache, or null
```

- Connecting a subscription schedules its first read without waiting for storage. `cached()` remains empty until that read delivers rows.
- Load state is released when its last observer and request finish. Reopening reads the saved page information from SQLite.
- A query is warm after `warm()` and after any read of it through `live` or `snapshots`, even when that read has ended. The library keeps the 16 latest queries warm and drops the oldest. A warm query is not a subscription: the subscription count does not include it.
- A warm query follows every commit. `revision` grows with each commit that reaches the rows, so you can tell a stale value from a newer one. The value can be one delivery behind a commit that you just made.
- `cachedRow` reads the row cache, which holds a limited number of rows. A miss does not say the row is absent.
- `cached()` and `cachedRow` throw when the cache read or the decode of a row fails. They never drop a row that does not decode.
- `cached()` and `cachedRow` count `peek_hits` and `peek_misses` in the counters and read no rows from storage.

## What a query depends on

A query depends on its model, so a commit of any row of the model makes the library check it. One shape is cheaper: a query for a known set of ids is bound to those rows, not to the model. A commit of any other row does not read it and does not wake it.

The binding applies only when the result can never contain a row outside the id set: a top-level `id` equal or `id` in a list, alone or inside all-of, without a scope and without includes. Every other shape (any-of, not, not-in-list, a scope, includes, counts) depends on the model. A row that does not exist yet, or leaves a narrower filter, still reaches a bound query.

## Guarantees

- One commit is one visible transition: a parent with its cascade-deleted children, a replaced list and an id swap appear together.
- Deliveries for one observer are in revision order. Older state never arrives after newer state. Several quick commits may be merged into one delivery, and the last state always arrives.
- If nothing the query reads has changed, nothing is delivered.
- Everything in `sync` is something the query reads: a change of `state`, `actions`, `dirtyFields`, `error` or the upload progress of a row is a delivery. The same holds for an included row: when its `sync` changes, the parent is in `diff.updated`.
- A memory row that was written again after its lifetime ran out is not deleted. The new write wins.
- After a write call returns, any new read sees the write.
- Dropping the subscription stops delivery at once. A slow reader never builds a backlog: it gets the newest result, and the replaced ids of the results it missed are merged into it, so no pair is lost.
- Listeners are called from library threads. Hop to your main thread yourself.

## In your UI

You rarely iterate changes by hand. Declare the data where you use it.

**SwiftUI.** `@Live` holds the current rows and refreshes the view on every change. Rows, single records and counts use the same attribute:

```swift
struct ProfileView: View {
  let userId: RowId
  @Live var moments: [Moment]
  @Live var user: User?

  init(userId: RowId) throws {
    self.userId = userId
    _moments = Live(try Moment.where(Moment.userId == userId).order(Moment.createdAt.desc))
    _user    = Live(User.find(userId))
  }

  var body: some View {
    List(moments) { MomentRow(moment: $0) }
      .navigationTitle(user?.name ?? "")
  }
}
```

**Equality.** Two snapshots of a row are equal when every field and the sync state match (a Swift model compares like a Kotlin data class), so a view that diffs by value sees a field change. Hashing uses the id only.

**One snapshot.** Every delivery of a live query, find, count, window or has-many relation carries the rows, the phase (`LoadPhase`: `opening`, `notLoaded`, `loading`, `loaded`, `empty`, `failed`, `closed`), the load flags, `hasMore`, `total`, `loadedAt`, the error and a `seq` that only grows. The first delivery comes after the first read of the rows and holds them, so there is no `loaded` or `empty` delivery with 0 rows before it. `empty` and `loaded` follow the `size` of the same delivery. After a load lands, the phase `loaded` or `empty` arrives only when the subscription has read the rows of that load, and until then the delivery shows `loading`. A subscription call never fails: a query the core refuses gives one delivery with the phase `failed` and the error kind `invalid`. A failed read of the rows, a failed load and a failed server subscription show in the `error` of the delivery until the next good read or recovery.

**Load phase.** `query.snapshots` is the one stream of a list: its `Snapshot` carries the rows, the phase (`opening` before the first delivery, then `notLoaded`, `loading`, `loaded`, `empty`, `failed`, `closed`), whether another page exists (`hasMore`), when the relation last loaded (`loadedAt`) and the `total` the server reports for the whole list (`totalCount` of the connection, or the field named by `total from:` of the scope, nil when the document does not select it). `error` is the typed last failure (Swift `any Error`, Kotlin `Throwable`: `DbError.Session` or `DbException.Session` with its `code` and `detail`, `Http` with its `status`, or `Invalid` for a row that does not decode) and `errorKind` names its category, so a screen reads a refused session from the same snapshot as its rows and phase, without a second subscription. The application decides what a session failure means, `seq` only grows, `undecodable` lists the ids of rows that this build cannot decode (they are not in `rows`) and `error` with `errorKind` `invalid` says why the last of them failed:

```swift
for await snapshot in try await Moment.feed.snapshots.stream() { if snapshot.isLoading { showSpinner() } }
```

```kotlin
Moment.feed.snapshots.flow.collect { snapshot -> if (snapshot.isLoading) showSpinner() }
```

While a load runs the snapshot also tells which load it is: `isInitialLoading` (the first load of a list without rows, for a skeleton), `isRefreshing` (a reload of a list with rows) and `isFetchingMore` (the next page, for a footer spinner). Concurrent loads of the same list do not mix, each flag follows its own loads. A stream that cannot attach (the database is not open) throws; `LiveList` shows it as one snapshot with the phase `failed` and the error kind `subscription`.

A load that ends with an error reports `failed` when the list has no rows. A list with rows reports `loaded` and keeps the error in the snapshot, so the rows stay on screen. A load started while the device is offline ends at once with a `network` error. When the device goes online again, each list that is observed and failed by network is loaded once more. A cache reset clears `loadedAt` and the error, and drops the window facts of the lists that nobody observes.

**Pages of a related list.** A list that lives under a node, such as the members of a group, pages with `loadMore()` and shows `hasMore` and `total` in its snapshot (each order of the list, `order: .ownerFirst`, has its own window) (`total` is nil when the connection has no `totalCount` and the scope has no `total from:`). See [node reads](models.md#node-reads). `loadMore()` returns `loaded(hasMore:)` when it fetched a page, `noMore` when the chain has no next page and `alreadyLoading` when another load of the same list was in flight: it never says `noMore` for a load that was only busy.

**Reading a whole list.** `loadAll()` (Swift, `async throws`) and `loadAll()` (Kotlin, `suspend`) read every page of a related list to its confirmed end and return nothing. After it returns, `snapshots.current()` holds all rows. `loadAll(force: false)` uses the current freshness and the pages that are stored, so a complete fresh list makes no request. `loadAll(force: true)` refreshes the first page and reads the rest. Callers that run together share the page requests. It throws when a page fails, when `max_pages` stops it with pages left, when the cursor does not advance, or when the query has no remote relation. The pages are not atomic: rows of earlier pages stay after a failure, and a cancelled call keeps the pages it has read. Cancelling the call ends its page request and leaves nothing running.

```swift
let members = try User.groupMembers(groupId, order: .ownerFirst)
try await members.load()
for await snapshot in try await members.snapshots.stream() { footer.isHidden = !snapshot.hasMore }
try await members.loadMore()                       // .loaded(hasMore: true) while another page exists
```

```kotlin
val members = User.groupMembers(groupId, order = GroupMemberOrder.OwnerFirst)
members.load()
members.loadMore()
```

**Replaced ids.** A record created offline is shown under a temporary id until the server answers (or a row that matches it lands first, see `match_by` in [writing data](writing-data.md)). The delivery that swaps the ids tells it: `query.snapshots` yields the rows with `replaced`, a map from the real id to the temporary id that it replaced, and `LiveList` keeps the map of its last delivery. A list that animates insertions uses it to move the existing row instead of removing one and inserting another.

```swift
let thread = LiveList(try Note.thread(groupId))   // thread.rows, thread.replaced, thread.ids
```

`ids` changes only when a delivery inserts, removes, moves or replaces a row, and not when a row is updated. A view that reads `ids` and gets each row from its [entity](entities.md) redraws on those deliveries only.

```kotlin
val thread = LiveList(Note.thread(groupId), viewModelScope)   // thread.rows, thread.replaced, thread.ids are StateFlow
```

**Edits.** A delivery also tells how the rows changed: `Snapshot.edits` is the ordered steps `rewrite(index, row)`, `insert(index, row)` and `remove(index, id)` from the rows of the previous delivery to the new rows. Each step carries the value of its own moment, so the steps of several deliveries replay in order (`rows.replay(edits)`) and the last write of an id wins. A slow reader gets the steps of the deliveries it missed joined in order. `LiveList.takeUpdate()` returns the newest `rows`, the steps since the reader last took them and the `revision` of that delivery, all from one delivery, and then counts the reader as up to date. In Kotlin it never waits for a delivery that is being published. A reader that mirrors the rows patches its mirror with the steps and reads only the changed rows. `edits` is nil, and the reader takes `rows` whole, for the first delivery, a whole list, a list that holds a row that did not decode, and a run of steps longer than the list. The library still looks up an id by scan for a replaced, removed or moved row, moves the tail of the array for an insert or remove in the middle, and copies the array once per delivery when a reader still holds the previous one.

```swift
for await snapshot in try await Note.thread(groupId).snapshots.stream() {
  if let edits = snapshot.edits { mirror.replay(edits) } else { mirror = snapshot.rows }
}
```

```kotlin
Note.thread(groupId).snapshots.collect { snapshot ->
    val edits = snapshot.edits
    if (edits != null) mirror.replay(edits) else mirror = snapshot.rows.toMutableList()
}
```

```swift
let update = thread.takeUpdate()
if let edits = update.edits { mirror.replay(edits) } else { mirror = update.rows }
```

```kotlin
val update = thread.takeUpdate()
val edits = update.edits
if (edits != null) mirror.replay(edits) else mirror = update.rows.toMutableList()
```

**View models.** `LiveList` keeps the newest snapshot of a query and moves the relation. It ends with its owner: there is nothing to start or stop. Creating one is cheap on the main thread: the subscription and the first read run on a background task (Swift) or on `Dispatchers.Default` (Kotlin), and `rows` stay empty until the first delivery arrives:

```swift
let feed = LiveList(Moment.feed)      // feed.snapshot, feed.rows are observable
try await feed.refresh()
try await feed.loadMore()
```

A `loadMore` that the server refuses with the error code `INVALID_CURSOR` restarts the list: the window keeps only the new first page and its cursor, and the snapshot shows no error.

A list often shows records that its rows refer to by id (the users of a group, the last message of a group). Read those through [entities](entities.md): one live value per id, no second subscription to write.

```kotlin
val feed = LiveList(Moment.feed, viewModelScope)   // feed.snapshot, feed.rows are StateFlow
feed.refresh()
feed.loadMore()
```

A screen that needs fresh history on every entry can set `refreshOnConnect`. The initial connection then forces 1 request, including when cached data is fresh. `waitForInitialLoad()` waits for that request and throws its error. It does not request again. Cached rows remain visible during loading and after failure. Ending the list cancels its wait without cancelling a request shared with other readers.

```swift
let thread = LiveList(try Note.thread(groupId), refreshOnConnect: true)
try await thread.waitForInitialLoad()
```

```kotlin
val thread = LiveList(Note.thread(groupId), viewModelScope, refreshOnConnect = true)
thread.waitForInitialLoad()
```

**Windows.** A group shows its last rows, not its whole history. `query.window(tail: n)` keeps the last `n` rows of an ordered query in memory (the query has no limit, include or search). A relation that lists by a param column, such as `scope :thread, insert_on: [:event, :response]` with `where groupId: :groupId` and an `order`, is a valid query: its members are the rows the relation loaded or received. A relation without an `order` has no local sort and is refused with `NotWindowable`. A scope declared with `stable_order: true` (see [models](models.md#scopes)) lists its rows in the order they were shown: a confirmed message, an event that changes a `sequenceNumber` and a reloaded page do not move a row that is on screen. A row outside the window is not decoded and not held, and a change outside the window delivers nothing. `LiveWindow` is to `Window` what `LiveList` is to `Query`: it keeps the rows, `hasBefore`, `hasAfter`, `total` and the typed `failure` of its snapshot, and it has `takeUpdate()` with the steps of [Edits](#in-your-ui) in window positions. `hasBefore` and `hasAfter` tell whether the database holds rows beyond each edge, and `total` is the number of rows of the query. The window follows the tail: a new last row enters and the oldest row of the window leaves. `expand(before:)` adds up to that many older rows from the database, `expand(after:)` adds newer rows, and neither adds a row that does not exist. The window ends with its owner. The library keeps the ids and sort keys of the whole query, not its rows. Windows over the same query share one set of sort keys when they open, so the second window reads no keys from the database. The window reads the local database only: it loads no page from the server yet.

```swift
let thread = LiveWindow(try Note.thread(groupId).window(tail: 50))   // rows keep their place when the server confirms a message
```

```kotlin
val thread = LiveWindow(Note.thread(groupId).window(tail = 50), viewModelScope)
```

```swift
let group = LiveWindow(try Note.where(Note.groupId == groupId).order(Note.createdAt.asc).window(tail: 50))
if group.hasBefore { group.expand(before: 50) }                 // group.rows, group.total are observable
let update = group.takeUpdate()
if let edits = update.edits { mirror.replay(edits) } else { mirror = update.rows }
```

```kotlin
val group = LiveWindow(Note.where(Note.groupId eq groupId).order(Note.createdAt.asc).window(tail = 50), viewModelScope)
if (group.edges.value.hasBefore) group.expand(before = 50)      // group.rows, group.edges are StateFlow
val update = group.takeUpdate()
val edits = update.edits
if (edits != null) mirror.replay(edits) else mirror = update.rows.toMutableList()
```

**UIKit and Android views.** Read `changes` and apply each `ListChange` to a diffable data source, a collection view or a `ListAdapter` (`dataSource` and `adapter` are app code). The change already tells which rows were inserted, removed, updated, moved or replaced, so animations are correct and a refresh does not flicker:

```swift
for await change in try await Moment.where(Moment.userId == userId).changes.stream() {
  if change.kind == .replaced { dataSource.reload(change.inserted.map(\.row)) } else { dataSource.applyDelta(change) }
}
```

```kotlin
Moment.where(Moment.userId eq userId).changes.collect { change ->
    if (change.kind == ListChangeKind.Replaced) adapter.submitList(change.inserted.map { it.row }) else adapter.applyDelta(change)
}   // RecyclerView ListAdapter
```

**Jetpack Compose.** `collectAsLive()` returns Compose state that follows the live result. Rows, single records and counts work the same way:

```kotlin
@Composable
fun ProfileScreen(userId: RowId) {
  val moments by Moment.where(Moment.userId eq userId).order(Moment.createdAt.desc()).collectAsLive()
  val user by User.find(userId).collectAsLive()

  Column {
    Text(user?.name.orEmpty())
    LazyColumn { items(moments) { MomentRow(it) } }
  }
}
```

**ViewModels and plain Kotlin.** A live query is a `Flow<Change>` with a `rows` shortcut: `Moment.recent.rows` is `Flow<List<Moment>>`, and `.stateIn(scope)` gives a `StateFlow`.

**Low level.** Without a UI framework, iterate the stream:

```swift
for await change in Moment.recent { handle(change.rows) }
```

```kotlin
Moment.recent.collect { change -> handle(change.rows) }
```

A live query that cannot start throws its error from `current()` and `stream()`. A cancelled caller gets the cancellation error, not the initial value, and a cancellation is never shown as a load failure.

## Queries

```swift
Moment.find(id)
try Moment.where(Moment.userId == id).order(Moment.createdAt.desc).limit(20)
Moment.recent                       // a named scope
moment.author                       // belongs_to
moment.vibes                        // has_many
moment.vibes.count                  // counted by SQLite, no cache field
```

```kotlin
Moment.find(id)
Moment.where(Moment.userId eq id).order(Moment.createdAt.desc()).limit(20)
Moment.recent                       // a named scope
moment.author                       // belongs_to
moment.vibes                        // has_many
moment.vibes.count                  // counted by SQLite, no cache field
```

## Related rows of a query

`Query.related(Moment.Relations.author)` returns a `RelatedQuery<User>` with `live` and `snapshots`. It reads the targets of one association for every row of the source query. The generated `Relations` of a model lists its `belongs_to`, `has_one`, `has_many` and `references` links with their target type.

```swift
let authors = try Moment.where(Moment.userId == id).order(Moment.createdAt.desc).limit(20).related(Moment.Relations.author)
for try await snapshot in try await authors.snapshots.stream() { show(snapshot.rows, snapshot.hasMore) }
```

```kotlin
val authors = Moment.where(Moment.userId eq id).order(Moment.createdAt.desc).limit(20).related(Moment.Relations.author)
authors.snapshots.flow.collect { show(it.rows, it.hasMore) }
```

- The source query owns the where, order, limit, remote relation, load, refresh and load-more. `RelatedQuery` has none of its own, so call them on the source query.
- Targets follow the order of the source rows, then the order of the link. A target that several parents share appears once, at its first position.
- A change of a parent row, a link or a target updates the result. A target that arrives after its link joins at the position of its parent. No parents give an empty result.
- A snapshot carries the phase, error, `hasMore` and `isRefreshing` of the source load in one delivery. `total` is nil, because the source count is not the number of targets.
- For one parent, select it with `limit(1)`. There is no hidden first.
- The properties of a model instance (`moment.author`) use the same path with a query for the one id.

Query description is plain data (`model`, `where`, `order`, `limit`, `offset`); the core compiles it to SQL. Every scalar and enum field compares as its own SQLite column; an index makes the comparison fast.

Null handling follows one rule: a row with a null field matches `!(field == value)`, like a person would expect.

## Text search

Fields listed by `search :text, :tags` use SQLite FTS5. The index changes in the same transaction as the row and follows edits and deletes. The app never writes FTS syntax: the core quotes each word.

```swift
Query<Moment>().search("hel")                          // finds "hello": the last word is a prefix
Query<Moment>().search("hello wor", wholeWords: true)  // every word must match whole
try Query<Moment>().search("cats").where(Moment.userId == id).limit(20)
```

```kotlin
Query("Moment", Moment).search("hel")                          // finds "hello": the last word is a prefix
Query("Moment", Moment).search("hello wor", wholeWords = true) // every word must match whole
Query("Moment", Moment).search("cats").where(Moment.userId eq id).limit(20)
```

The default match treats the last word as a prefix and the other words as whole words. A query without an order is ranked by relevance. A query has one search, and the model needs at least one searchable field. Search combines with filters, scopes and limits, and an observed search re-runs when a searchable field changes.

## Edge values

The values of a connection edge are read with the `async` `feedEdge(of:)`, see [edge values](models.md#edge-values). They are not part of the row, of a live list or of a window, so a change of an edge value delivers nothing. A list that shows the feed reads the value for the row it presents.

```swift
let list = Moment.feed
let uuid = try Moment.feedEdge(of: id)?.impressionUuid
```

```kotlin
val list = Moment.feed
val uuid = Moment.feedEdge(id)?.impressionUuid
```

## Remote relations are live too

Connecting to a scope that has a `query` declaration loads it when its data is stale, keeps it fresh through its `subscribe` subscription while someone reads it, and lets go a while after the last reader leaves. User gestures stay explicit: `refresh()` and `loadMore()`.

Three calls read a remote relation, each for one need:

| Call | Request | Keeps a subscription | Use it for |
| ---- | ------- | -------------------- | ---------- |
| live (`live`, `LiveList`, `@Live`) | when stale | yes, while read | a screen that shows the relation |
| `load()` | when stale, none when fresh | no | rows that need their relation once: 45 rows of a list, each loads its own relation |
| `refresh()` | always | no | a user gesture (pull to refresh) |

`load()` makes the same decision as connecting a live read: no request when the window is fresh and not invalidated, exactly 1 request when it is stale or never loaded. N parallel loads of one relation make 1 request, shared with a live connect and with `refresh()`. A failed load throws like `refresh()`. Cancelling the caller stops only its own wait: the load goes on for the other readers.

To test the 24 h or 5 minute windows, open the database with the test clock and `advance` it past `freshness expires_in`: the next `load()` requests again, and 1 second before the limit it does not. See [testing](testing.md).

```swift
try await Moment.byUser(userId: 5).load()
```

```kotlin
Moment.byUser(userId = 5).load()
```

A row that does not fit the generated model is skipped and logged as `Row of <Model> failed to decode` (Swift: `os` subsystem `DBlayer`, category `decode`; Kotlin: the JVM logger named `DBlayer`). An empty list with this line in the log means the model and the server schema disagree.

See [server sync](server-sync.md) and [threading](threading.md).

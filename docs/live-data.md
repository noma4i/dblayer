# Live data and queries

Every read of a model is live. A read gives you the current value and a stream of changes. There is no snapshot-only read and no separate observe call: connecting to data is reading it.

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

A change of an ephemeral value on an included row reaches the observer of the parent too. When `User.typing` of a chat author changes, an observer of chats that includes `author` gets the chat in `diff.updated`, with the new value in `related`.

```swift
@Live var chat: Chat?
// chat.author.typing changes -> the chat row is in diff.updated
```

```kotlin
val chat by Chat.find(chatId).collectAsLive()
// chat.author.typing changes -> the chat row is in diff.updated
```

## Reading before the first delivery

A new subscription delivers its first rows from a library thread, so a screen that has to show cached rows at once cannot wait for it. `cached()` answers from memory, without reading the database file, so it is safe on the main thread. It gives `nil` (Swift) or `null` (Kotlin) when the query is not warm. That means "not in memory", never "no rows".

```swift
let list = Moment.recent
if let cached = list.cached() { show(cached.value, revision: cached.revision) }   // rows and revision, or nil
try await list.warm()                    // read once, keep in memory
let row = list.cachedRow(id)             // one row from the row cache, or nil
```

```kotlin
val list = Moment.recent
list.cached()?.let { show(it.value, it.revision) }   // rows and revision, or null
list.warm()                              // read once, keep in memory
val row = list.cachedRow(id)             // one row from the row cache, or null
```

- A query is warm after `warm()` and after any read of it through `live` or `snapshots`, even when that read has ended. The library keeps the 16 latest queries warm and drops the oldest. A warm query is not a subscription: the subscription count does not include it.
- A warm query follows every commit. `revision` grows with each commit that reaches the rows, so you can tell a stale value from a newer one. The value can be one delivery behind a commit that you just made.
- `cachedRow` reads the row cache, which holds a limited number of rows. A miss does not say the row is absent.
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

  init(userId: RowId) {
    self.userId = userId
    _moments = Live(Moment.where(Moment.userId == userId).order(Moment.createdAt.desc()))
    _user    = Live(User.find(userId))
  }

  var body: some View {
    List(moments) { MomentRow(moment: $0) }
      .navigationTitle(user?.name ?? "")
  }
}
```

**Equality.** Two snapshots of a row are equal when every field and the sync state match (a Swift model compares like a Kotlin data class), so a view that diffs by value sees a field change. Hashing uses the id only.

**Loading state.** A remote relation reports `idle`, `loading` or `failed`, whether another page exists, when it last loaded and the `total` the server reports for the whole list (`totalCount` of the connection, nil when the document does not select it):

```swift
for await state in await Moment.feed.state.stream() { if state.isLoading { showSpinner() } }
```

```kotlin
Moment.feed.state.flow.collect { state -> if (state.isLoading) showSpinner() }
```

While a load runs the state also tells which load it is: `isInitialLoading` (the first load of a list without rows, for a skeleton), `isRefreshing` (a reload of a list with rows) and `isFetchingMore` (the next page, for a footer spinner). Concurrent loads of the same list do not mix, each flag follows its own loads.

**Replaced ids.** A record created offline is shown under a temporary id until the server answers (or a row that matches it lands first, see `correlate` in [writing data](writing-data.md)). The delivery that swaps the ids tells it: `query.snapshots` yields the rows with `replaced`, a map from the real id to the temporary id that it replaced, and `LiveList` keeps the map of its last delivery. A list that animates insertions uses it to move the existing row instead of removing one and inserting another.

```swift
let thread = LiveList(Message.thread(chatId))   // thread.rows, thread.replaced, thread.ids
```

`ids` changes only when a delivery inserts, removes, moves or replaces a row, and not when a row is updated. A view that reads `ids` and gets each row from its [entity](entities.md) redraws on those deliveries only.

```kotlin
val thread = LiveList(Message.thread(chatId), viewModelScope)   // thread.rows, thread.replaced, thread.ids are StateFlow
```

**Edits.** A delivery also tells how the rows changed: `Snapshot.edits` is the ordered steps `rewrite(index, row)`, `insert(index, row)` and `remove(index, id)` from the rows of the previous delivery to the new rows. Each step carries the value of its own moment, so the steps of several deliveries replay in order (`rows.replay(edits)`) and the last write of an id wins. A slow reader gets the steps of the deliveries it missed joined in order. `LiveList.takeEdits()` returns the steps since the reader last took them and then counts the reader as up to date. A reader that mirrors the rows patches its mirror with the steps and reads only the changed rows. `edits` is nil, and the reader takes `rows` whole, for the first delivery, a whole list, a list that holds a row that did not decode, and a run of steps longer than the list. The library still looks up an id by scan for a replaced, removed or moved row, moves the tail of the array for an insert or remove in the middle, and copies the array once per delivery when a reader still holds the previous one.

```swift
for await snapshot in Message.thread(chatId).snapshots {
  if let edits = snapshot.edits { mirror.replay(edits) } else { mirror = snapshot.rows }
}
```

```kotlin
Message.thread(chatId).snapshots.collect { snapshot ->
    val edits = snapshot.edits
    if (edits != null) mirror.replay(edits) else mirror = snapshot.rows.toMutableList()
}
```

```swift
if let edits = thread.takeEdits() { mirror.replay(edits) } else { mirror = thread.rows }
```

```kotlin
val edits = thread.takeEdits()
if (edits != null) mirror.replay(edits) else mirror = thread.rows.value.toMutableList()
```

**View models.** `LiveList` keeps the rows and the state of a query and moves the relation. It ends with its owner: there is nothing to start or stop. Creating one is cheap on the main thread: the subscription and the first read run on a background task (Swift) or on `Dispatchers.Default` (Kotlin), and `rows` stay empty until the first delivery arrives:

```swift
let feed = LiveList(Moment.feed)      // feed.rows, feed.state are observable
try await feed.refresh()
try await feed.loadMore()
```

A list often shows records that its rows refer to by id (the users of a chat, the last message of a chat). Read those through [entities](entities.md): one live value per id, no second subscription to write.

```kotlin
val feed = LiveList(Moment.feed, viewModelScope)   // feed.rows, feed.state are StateFlow
feed.refresh()
feed.loadMore()
```

A screen that needs fresh history on every entry can set `refreshOnConnect`. The initial connection then forces 1 request, including when cached data is fresh. `waitForInitialLoad()` waits for that request and throws its error. It does not request again. Cached rows remain visible during loading and after failure. Ending the list cancels its wait without cancelling a request shared with other readers.

```swift
let thread = LiveList(Message.thread(chatId), refreshOnConnect: true)
try await thread.waitForInitialLoad()
```

```kotlin
val thread = LiveList(Message.thread(chatId), viewModelScope, refreshOnConnect = true)
thread.waitForInitialLoad()
```

**Windows.** A chat shows its last rows, not its whole history. `query.window(tail: n)` keeps the last `n` rows of an ordered query in memory (the query has no limit, include or search). A relation that lists by a param column, such as `scope :thread, joins: :events` with `where :chatId` and an `order`, is a valid query: its members are the rows the relation loaded or received. A relation without an `order` has no local sort and is refused with `NotWindowable`,. A row outside the window is not decoded and not held, and a change outside the window delivers nothing. `LiveWindow` is to `Window` what `LiveList` is to `Query`: it keeps the rows, `hasBefore`, `hasAfter` and `total`, and it has `takeEdits()` with the steps of [Edits](#in-your-ui) in window positions. `hasBefore` and `hasAfter` tell whether the database holds rows beyond each edge, and `total` is the number of rows of the query. The window follows the tail: a new last row enters and the oldest row of the window leaves. `expand(before:)` adds up to that many older rows from the database, `expand(after:)` adds newer rows, and neither adds a row that does not exist. The window ends with its owner. The library keeps the ids and sort keys of the whole query, not its rows, and the window reads the local database only: it loads no page from the server yet.

```swift
let chat = LiveWindow(Message.where(Message.chatId == chatId).order(Message.createdAt.asc).window(tail: 50))
if chat.hasBefore { chat.expand(before: 50) }                 // chat.rows, chat.total are observable
if let edits = chat.takeEdits() { mirror.replay(edits) } else { mirror = chat.rows }
```

```kotlin
val chat = LiveWindow(Message.where(Message.chatId eq chatId).order(Message.createdAt.asc).window(tail = 50), viewModelScope)
if (chat.edges.value.hasBefore) chat.expand(before = 50)      // chat.rows, chat.edges are StateFlow
val edits = chat.takeEdits()
if (edits != null) mirror.replay(edits) else mirror = chat.rows.value.toMutableList()
```

**UIKit and Android views.** Apply the diff to a diffable data source or a collection view. The diff already tells which rows were inserted, removed, updated, moved or replaced, so animations are correct and a refresh does not flicker:

```swift
let feed = Live(Moment.recent)
feed.onChange { change in dataSource.apply(change.snapshot()) }
```

```kotlin
Moment.recent.onChange { change -> change.dispatchTo(adapter) }   // RecyclerView ListAdapter
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

A live query that cannot start ends its stream without a value. `current()` then returns the initial value.

## Queries

```swift
Moment.find(id)
Moment.where(Moment.userId == id).order(Moment.createdAt.desc()).limit(20)
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

Query description is plain data (`model`, `where`, `order`, `limit`, `offset`); the core compiles it to SQL. Every scalar and enum field compares as its own SQLite column; an index makes the comparison fast.

Null handling follows one rule: a row with a null field matches `!(field == value)`, like a person would expect.

## Text search

Fields listed by `search :text, :tags` use SQLite FTS5. The index changes in the same transaction as the row and follows edits and deletes. The app never writes FTS syntax: the core quotes each word.

```swift
Query<Moment>().search("hel")                          // finds "hello": the last word is a prefix
Query<Moment>().search("hello wor", wholeWords: true)  // every word must match whole
Query<Moment>().search("cats").where(Moment.userId == id).limit(20)
```

```kotlin
Query("Moment", Moment).search("hel")                          // finds "hello": the last word is a prefix
Query("Moment", Moment).search("hello wor", wholeWords = true) // every word must match whole
Query("Moment", Moment).search("cats").where(Moment.userId eq id).limit(20)
```

The default match treats the last word as a prefix and the other words as whole words. A query without an order is ranked by relevance. A query has one search, and the model needs at least one searchable field. Search combines with filters, scopes and limits, and an observed search re-runs when a searchable field changes.

## Remote relations are live too

Connecting to a relation that has a `fetch` declaration loads it when its data is stale, keeps it fresh through its `updates` subscription while someone reads it, and lets go a while after the last reader leaves. User gestures stay explicit: `refresh()` and `loadMore()`.

Three calls read a remote relation, each for one need:

| Call | Request | Keeps a subscription | Use it for |
| ---- | ------- | -------------------- | ---------- |
| live (`live`, `LiveList`, `@Live`) | when stale | yes, while read | a screen that shows the relation |
| `load()` | when stale, none when fresh | no | rows that need their relation once: 45 rows of a list, each loads its own relation |
| `refresh()` | always | no | a user gesture (pull to refresh) |

`load()` makes the same decision as connecting a live read: no request when the window is fresh and not invalidated, exactly 1 request when it is stale or never loaded. N parallel loads of one relation make 1 request, shared with a live connect and with `refresh()`. A failed load throws like `refresh()`. Cancelling the caller stops only its own wait: the load goes on for the other readers.

To test the 24 h or 5 minute windows, open the database with the test clock and `advance` it past `fresh_for`: the next `load()` requests again, and 1 second before the limit it does not. See [testing](testing.md).

```swift
try await Moment.byUser(userId: 5).load()
```

```kotlin
Moment.byUser(userId = 5).load()
```

A row that does not fit the generated model is skipped and logged as `Row of <Model> failed to decode` (Swift: `os` subsystem `DBlayer`, category `decode`; Kotlin: the JVM logger named `DBlayer`). An empty list with this line in the log means the model and the server schema disagree.

See [server sync](server-sync.md) and [threading](threading.md).

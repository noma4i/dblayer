# Live data and queries

Every read of a model is live. A read gives you the current value and a stream of changes. There is no snapshot-only read and no separate observe call: connecting to data is reading it.

## What a change contains

| Field | Meaning |
| ----- | ------- |
| `revision` | Commit revision the state reflects. It only grows for one observer |
| `rows` | Full current result |
| `diff.inserted`, `removed`, `updated`, `moved` | Row identities that changed |
| `rows[i].sync` | Sync state of every row, see [sync state](sync-state.md) |
| `diff.replaced` | Pairs `(old id, new id)`, for example a temporary id replaced by the server id, so a UI can keep the same element |

The first delivery contains every row as `inserted`.

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
let thread = LiveList(Message.thread(chatId))   // thread.rows, thread.replaced
```

```kotlin
val thread = LiveList(Message.thread(chatId), viewModelScope)   // thread.rows, thread.replaced are StateFlow
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

Query description is plain data (`model`, `where`, `order`, `limit`, `offset`); the core compiles it to SQL. Indexed fields compare as real columns, other fields through JSON extraction.

Null handling follows one rule: a row with a null field matches `!(field == value)`, like a person would expect.

## Remote relations are live too

Connecting to a relation that has a `remote` block loads it when its data is stale, keeps it fresh through its `updates` subscription while someone reads it, and lets go a while after the last reader leaves. User gestures stay explicit: `refresh()` and `loadMore()`.

A row that does not fit the generated model is skipped and logged as `Row of <Model> failed to decode` (Swift: `os` subsystem `DBlayer`, category `decode`; Kotlin: the JVM logger named `DBlayer`). An empty list with this line in the log means the model and the server schema disagree.

See [server sync](server-sync.md) and [threading](threading.md).

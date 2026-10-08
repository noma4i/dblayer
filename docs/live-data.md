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

## Guarantees

- One commit is one visible transition: a parent with its cascade-deleted children, a replaced list and an id swap appear together.
- Deliveries for one observer are in revision order. Older state never arrives after newer state. Several quick commits may be merged into one delivery, and the last state always arrives.
- If nothing the query reads has changed, nothing is delivered.
- After a write call returns, any new read sees the write.
- Dropping the subscription stops delivery at once.
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

The projected value `$moments` gives the loading state (`idle`, `loading`, `failed`) and the last diff:

```swift
if $moments.state.isLoading { ProgressView() }
```

```kotlin
val state by Moment.recent.collectState()
if (state.isLoading) CircularProgressIndicator()
```

**View models.** `Live` also works in `@Observable` classes and any other code: `let moments = Live(Moment.recent)` exposes `rows`, `state` and `diff`.

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

See [server sync](server-sync.md) and [threading](threading.md).

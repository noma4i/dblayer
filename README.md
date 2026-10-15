# DBlayer

ActiveRecord for Swift and Kotlin. Describe models once, store normalized data in SQLite, synchronize through GraphQL and receive live updates.

One Rust core serves iOS and Android. Swift and Kotlin only get thin bindings and a typed, Rails-like API generated from your model files.

## Highlights

- **Always live.** Every read of a model returns a live result: the current value plus a stream of changes. There is no separate "observe" call to remember.
- **Refresh on entry.** `LiveList` can force its initial request while keeping cached rows visible. Await that request without starting another one.
- **Local first.** Data lives in SQLite. Reads never wait for the network, and writes are applied locally at once.
- **Partial child updates.** Parent fields copied through `sets` keep their values when the child omits those fields. Explicit null clears them.
- **Sync state on every record.** Each record knows if the server has seen it: `synced`, `pending`, `sending` or `failed`, plus `isLocal` for records that still have a temporary id. Build spinners, retry buttons and unsent banners on it, and query by it.
- **Offline writes.** Create, update and destroy are applied optimistically and queued. The queue survives restarts, is sent in order, rolled back on a final failure and replaces temporary ids with server ids.
- **GraphQL without glue code.** Register your operations and point models at them. Responses of queries, mutations and subscription events are normalized into rows by one set of rules. Refetching after mutations, reconnects and app resume is automatic.
- **No flicker.** One change is one commit. Observers see cascades, replacements and id swaps as a single transition, in order. Pull-to-refresh replaces data in one commit with no empty state in between.
- **Files built in.** A `file` field stores binary data. You never choose where bytes live: SQLite or disk.
- **Background delivery.** Observers receive updates from library threads. Disk reads use a bounded WAL connection pool beside the writer. See [threading](docs/threading.md).
- **One core, two platforms.** All logic is Rust. Swift and Kotlin contain no business logic.
- **Versioned cached rows.** Each row sent to Swift or Kotlin includes its stored revision, so readers can reject older values.


## Documentation

- [Getting started](docs/getting-started.md)
- [Describing models](docs/models.md)
- [Local and synced models](docs/storage-models.md)
- [Live data and queries](docs/live-data.md)
- [Entities: one live value per record](docs/entities.md)
- [Writing data](docs/writing-data.md)
- [Sync state of a record](docs/sync-state.md)
- [Server sync](docs/server-sync.md)
- [Files](docs/files.md)
- [Threading and performance](docs/threading.md)
- [Testing](docs/testing.md)
- [Architecture](docs/architecture.md)

## How a model looks

Models are described in `.dbl` files with a Rails-like syntax:

```ruby
model Moment do
  field :uuid, type: :string
  field :userId, type: :id, null: true
  field :text, type: :string, null: true
  field :media, type: :file, null: true
  field :createdAt, type: :date

  index :uuid
  index :userId, :createdAt

  belongs_to :author, model: User, key: :userId, touch: true
  has_many :vibes, dependent: :destroy

  scope :recent do
    order createdAt: :desc
  end

  scope :feed do
    fetch MomentsFeed
    updates MomentEvents, deleted_by: :destroyed
    fresh_for 5.minutes
  end

  operation :create do
    mutation CreateMoment
  end
  operation :destroy do
    mutation DeleteMoment
  end
end
```

The generator turns these files into typed Swift and Kotlin models and a model specification for the core. Nested `object` and `array` blocks generate their value types too, with reusable names, optional fields, defaults and JSON key aliases. Keep field declarations in `.dbl` and add app behavior in extensions. See [describing models](docs/models.md#nested-objects-and-lists).

Use `dblayer-gen explain models/*.dbl` to inspect generated tables, indexes and SQLite query plans before building the app.

## What using it looks like

The generated bindings give you a live property. Declare the query, use the rows, the screen updates itself. Writes are one line and need no reload code.

### Swift

```swift
struct FeedView: View {
  @Live(Moment.where(Moment.userId == me.id).order(Moment.createdAt.desc()).limit(20))
  var moments

  var body: some View {
    List(moments) { MomentRow(moment: $0) }
      .refreshable { try? await Moment.feed.refresh() }
  }
}

let moment = try await Moment.create(text: "hello")   // optimistic, queued, synced
```

### Kotlin

```kotlin
@Composable
fun FeedScreen(me: User) {
  val moments by Moment.where(Moment.userId eq me.id).order(Moment.createdAt.desc()).limit(20).collectAsLive()
  LazyColumn { items(moments) { MomentRow(it) } }
}

val moment = Moment.create(text = "hello")            // optimistic, queued, synced
```

See [docs/live-data.md](docs/live-data.md) for UIKit, view models, single records, loading state and diffs.

## Repository layout

| Path | Contents |
| ---- | -------- |
| `core/` | The Rust core: storage, writes, queries, sync, files |
| `gen/` | `dblayer-gen`: `.dbl` parser and code generator |
| `fixtures/` | Example models used by tests |
| `swift/` | Swift package with the bindings |
| `kotlin/` | Android library module with the bindings |
| `docs/` | Developer documentation |
| `scripts/` | Build and check scripts |

## Build and test

Requirements: Xcode, Android SDK with NDK, Homebrew `rustup` with the iOS and Android targets, `cargo-ndk`, JDK 17 and Gradle 9.

```sh
export PATH=/opt/homebrew/opt/rustup/bin:$PATH
bash scripts/check_dblayer.sh      # cheap checks, then the Rust, Swift, Kotlin and Android lanes one after another (--fast: touched directories only)
cargo test                         # Rust core and generator only
```

More in [docs/getting-started.md](docs/getting-started.md).

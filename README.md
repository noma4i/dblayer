# DBlayer

ActiveRecord for Swift and Kotlin. You describe your models once, DBlayer keeps them in a local SQLite database, syncs them with a GraphQL endpoint and hands your app live data that never blocks a thread.

One Rust core serves iOS and Android. Swift and Kotlin only get thin bindings and a typed, Rails-like API generated from your model files.

## Highlights

- **Always live.** Every read of a model returns a live result: the current value plus a stream of changes. There is no separate "observe" call to remember.
- **Local first.** Data lives in SQLite. Reads never wait for the network, and writes are applied locally at once.
- **Offline writes.** Create, update and destroy are applied optimistically and queued. The queue survives restarts, is sent in order, rolled back on a final failure and replaces temporary ids with server ids.
- **GraphQL without glue code.** Register your operations and point models at them. Responses of queries, mutations and subscription events are normalized into rows by one set of rules. Refetching after mutations, reconnects and app resume is automatic.
- **No flicker.** One change is one commit. Observers see cascades, replacements and id swaps as a single transition, in order. Pull-to-refresh replaces data in one commit with no empty state in between.
- **Files built in.** A `file` field stores binary data. You never choose where bytes live: SQLite or disk.
- **Does not block.** Public calls are asynchronous and cheap. Reads do not wait for writes, and observers are called from library threads, never from the main thread.
- **One core, two platforms.** All logic is Rust. Swift and Kotlin contain no business logic.

## How a model looks

Models are described in `.dbl` files with a Rails-like syntax:

```ruby
model Moment do
  schema do
    string :uuid,      index: true
    id     :userId,    null: true, index: true
    string :text,      null: true
    file   :media,     null: true
    date   :createdAt, index: true

    index :userId, :createdAt
  end

  belongs_to :author, model: User, key: :userId, touch: true
  has_many   :vibes,  model: Vibe, key: :momentId, dependent: :destroy

  scope :recent, order: { createdAt: :desc }

  remote :feed do
    query MomentsFeed
    updates MomentEvents, deleted_by: :destroyed
    fresh_for 5.minutes
  end

  create  with: CreateMoment
  destroy with: DeleteMoment
end
```

The generator turns these files into typed Swift and Kotlin models and a model specification for the core. See [docs/models.md](docs/models.md).

## What using it looks like

The generated bindings give you this API:

```swift
let recent = Moment.where(Moment.userId == me.id).order(Moment.createdAt.desc).limit(20)
for await change in recent {          // value now, then every change
  render(change.rows, diff: change.diff)
}

let moment = try await Moment.create(text: "hello")   // optimistic, queued, synced
try await Moment.feed.refresh()                       // pull to refresh
```

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
| `legacy/` | The previous Swift implementation, kept as a reference until the port is complete |

## Build and test

Requirements: Xcode, Android SDK with NDK, Homebrew `rustup` with the iOS and Android targets, `cargo-ndk`, JDK 17 and Gradle 9.

```sh
export PATH=/opt/homebrew/opt/rustup/bin:$PATH
bash scripts/check_dblayer.sh      # Rust tests, Swift tests, iOS simulator build, Kotlin tests, Android libraries
cargo test                         # Rust core and generator only
```

More in [docs/getting-started.md](docs/getting-started.md).

## Documentation

- [Getting started](docs/getting-started.md)
- [Describing models](docs/models.md)
- [Live data and queries](docs/live-data.md)
- [Writing data](docs/writing-data.md)
- [Server sync](docs/server-sync.md)
- [Files](docs/files.md)
- [Threading and performance](docs/threading.md)
- [Testing](docs/testing.md)
- [Architecture](docs/architecture.md)

# DBlayer

ActiveRecord for Swift and Kotlin. Describe models once, store normalized data in SQLite, synchronize through GraphQL and receive live updates.

One Rust core serves iOS and Android. Swift and Kotlin only get thin bindings and a typed, Rails-like API generated from your model files.

## Requirements

Android 13 (API 33) or newer.

## Highlights

- **Always live.** Every read of a model returns a live result: the current value plus a stream of changes. There is no separate "observe" call to remember.
- **Refresh on entry.** `LiveList` can force its initial request while keeping cached rows visible. Await that request without starting another one.
- **Local first.** Data lives in SQLite. Reads never wait for the network, and writes are applied locally at once.
- **Partial child updates.** Parent fields copied through `sets` keep their values when the child omits those fields. Explicit null clears them.
- **Sync state on every record.** Each record knows if the server has seen it: `synced`, `pending`, `sending`, `failed` or `deliveryUnknown`, plus `isLocal` for records that still have a temporary id. Build spinners, retry buttons and unsent banners on it, and query by it.
- **Offline writes.** Create, update and destroy are applied optimistically and queued. The queue survives restarts, is sent in order, rolled back on a final failure and replaces temporary ids with server ids.
- **GraphQL without glue code.** Register your operations and point models at them. Responses of queries, mutations and subscription events are normalized into rows by one set of rules. Refetching after mutations, reconnects and app resume is automatic.
- **No flicker.** One change is one commit. Observers see cascades, replacements and id swaps as a single transition, in order. Pull-to-refresh replaces data in one commit with no empty state in between.
- **Encrypted sessions.** The core builds and opens encrypted requests, numbers mutations and stores a pending mutation before it is sent, so a lost answer is repeated unchanged after a restart. Session recovery delivers the answer to its queued mutation or upload step in Swift and Kotlin. The app supplies only the HTTP post and the credential storage. See [server sync](docs/server-sync.md).
- **Cloud sync of local data.** Local models and `local: true` attributes follow the user across devices through CloudKit on iOS and Google Drive on Android. The core journals, merges per attribute and retries, ready adapters move the bytes and the app passes one object. See [cloud sync](docs/cloud.md).
- **Encrypted cable primitives.** Rust `CableLink` authenticates the server pin, creates the encrypted hello and encrypts subscription frames with directional keys. See [cable protocol](docs/cable-protocol.md).
- **Files built in.** A `file` field stores binary data. You never choose where bytes live: SQLite or disk.
- **Background delivery.** Observers receive updates from library threads. Disk reads use a bounded WAL connection pool beside the writer. Releasing synchronization stops its background work. See [threading](docs/threading.md).
- **One core, two platforms.** All logic is Rust. Swift and Kotlin contain no business logic.
- **Versioned cached rows.** Each row sent to Swift or Kotlin includes its stored revision, so readers can reject older values.
- **Immediate cached entities.** The first reader gets a row already in memory without waiting for a subscription delivery or reading disk. Native callbacks never keep entity sources alive. Deliveries from a replaced database cannot overwrite current entities.


## Documentation

- [Getting started](docs/getting-started.md)
- [Describing models](docs/models.md)
- [Local and synced models](docs/storage-models.md)
- [Cloud models](docs/models.md#cloud-models)
- [Live data and queries](docs/live-data.md)
- [Entities: one live value per record](docs/entities.md)
- [Writing data](docs/writing-data.md)
- [Sync state of a record](docs/sync-state.md)
- [Server sync](docs/server-sync.md)
- [Files](docs/files.md)
- [Cloud sync of local data](docs/cloud.md)
- [Threading and performance](docs/threading.md)
- [Testing](docs/testing.md)
- [Architecture](docs/architecture.md)

## How a model looks

Models are described in `.dbl` files with a Rails-like syntax:

```ruby
model Moment do
  attribute :uuid, :string
  attribute :userId, :id, null: true
  attribute :text, :string, null: true
  attribute :media, :file, null: true
  attribute :createdAt, :date
  index :uuid
  index :userId, :createdAt
  belongs_to :author, class_name: User, foreign_key: :userId, touch: true
  has_many :vibes, dependent: :destroy
  scope :recent do
    order createdAt: :desc
  end
  scope :feed do
    query "MomentsFeed"
    freshness expires_in: 5.minutes
    subscribe "MomentEvents", apply: { effect: :upsert, delete_if: "destroyed" }
  end
  operation :create do
    mutation "CreateMoment"
  end
  operation :destroy do
    mutation "DeleteMoment"
  end
end
```

The generator turns these files into typed Swift and Kotlin models and a model specification for the core. Nested `attribute` blocks generate their value types too, with reusable names, optional attributes, defaults and JSON key aliases. Keep attribute declarations in `.dbl` and add app behavior in extensions. See [describing models](docs/models.md#nested-objects-and-lists).

A scope declared with `node: :one` reads one entity with GraphQL `node(id:)`, and `Group.node(id)` returns the typed row or nil. See [node reads](docs/models.md#node-reads).

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

### Encrypted sign-in

Fetch config before opening the database. Both platforms use the same Rust code without credentials or local storage.

```swift
let config = try await DB.serverConfig(encrypted: encrypted, platform: .ios, version: appVersion)
if config.updateRequired { showUpdate(config.storeUrl) }
```

```kotlin
val config = DBlayer.serverConfig(context, encrypted, AppPlatform.ANDROID, appVersion)
if (config.updateRequired) showUpdate(config.storeUrl)
```


```swift
try await DB.open(
  encrypted: EncryptedConfig(origin: "https://api.example.com", serverKey: serverKey, pin: serverPin, endpoints: endpoints, protocolLabel: protocolLabel, cablePurpose: cablePurpose, cableChannel: cableChannel),
  credentials: KeychainCredentials(), modelsJSON: modelsJSON, path: path
)
_ = try await DB.database.sessionCall(operation: "otp", bodyJson: #"{"phone":"+61400000000"}"#)
```

```kotlin
DBlayer.open(
  context,
  encrypted = EncryptedConfig("https://api.example.com", serverKey, serverPin, endpoints, protocolLabel, cablePurpose, cableChannel),
  credentials = KeystoreCredentials(), modelsJson = modelsJSON, path = path,
)
DBlayer.database.sessionCall("otp", """{"phone":"+61400000000"}""")
```

Await `DB.open` in Swift or call suspend `DBlayer.open` from a Kotlin coroutine.
Opening files, WAL and migrations runs on the library's worker pool.
Identical repeated calls reuse the opened database. Different parameters throw an error. Failed opens allow retry.
Keep one instance of each credential store, transport and HTTP callback for the process. The core compares their identities.

Omit `encrypted` and `credentials` for ordinary GraphQL through `endpoint` or a custom `transport`.
Omit the endpoint and transport too for a local database. See [server sync](docs/server-sync.md) for arguments and errors.

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

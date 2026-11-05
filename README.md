# DBlayer

ActiveRecord for Swift and Kotlin. Describe models once, store normalized data in SQLite, synchronize through GraphQL and receive live updates.

One Rust core serves iOS and Android. Swift and Kotlin only get thin bindings and a typed, Rails-like API generated from your model files.

Generated APIs escape language keywords while preserving JSON keys. SQLite also accepts keyword fields in storage, indexes and queries.

## Requirements

iOS 17, macOS 14, or Android 13 (API 33) or newer.

## Highlights

- **Always live.** Every read of a model is a stream: a snapshot, then keyed deltas. There is no separate "observe" call to remember.
- **Refresh on entry.** A stream loads a stale relation on connect while keeping cached rows visible. `beginLoad()` gives the result of that load without starting another one.
- **Local first.** Data lives in SQLite. Reads never wait for the network, and writes are applied locally at once.
- Repeat `local` in a create operation to fill attributes from different singleton models.
- Filter subscription applications with value lists or `not`. Model declarations share one incoming stream.
- Use `membership list: { contains: :lists }` and `insert_on: [:event, :response, :node]` for event-driven node membership.
- **Partial child updates.** Parent fields copied through `sets` keep their values when the child omits those fields. Explicit null clears them.
- **Sync state on every record.** Each record knows if the server has seen it: `synced`, `pending`, `sending`, `failed` or `deliveryUnknown`. Build spinners, retry buttons and unsent banners on it, and query by it.
- **Offline writes.** Create, update and destroy are applied optimistically and queued. The queue survives restarts, is sent in order, rolled back on a final failure and replaces temporary ids with server ids.
- **GraphQL without glue code.** Register your operations and point models at them. Responses of queries, mutations and subscription events are normalized into rows by one set of rules. Refetching after mutations, reconnects and app resume is automatic.
- **Typed GraphQL inputs.** Generated Swift and Kotlin types support recursive inputs and one-of choices, preserving omitted fields and explicit nulls.
- **No flicker.** One change is one commit. Observers see cascades, replacements and id swaps as a single transition, in order. Pull-to-refresh replaces data in one commit with no empty state in between.
- **Encrypted sessions.** The core builds and opens encrypted requests, numbers mutations and stores a pending mutation before it is sent, so a lost answer is repeated unchanged after a restart. Session recovery delivers the answer to its queued mutation or upload step in Swift and Kotlin. The app supplies only the HTTP post and the credential storage. See [server sync](docs/server-sync.md).
- **Encrypted cable primitives.** Rust `CableLink` authenticates the server pin, creates the encrypted hello and encrypts subscription frames with directional keys. See [cable protocol](docs/cable-protocol.md).
- **Files built in.** A `file` field stores binary data. You never choose where bytes live: SQLite or disk.
- **Owned upload sources.** Upload admission copies files before committing. Files above 256 KiB live beside the database and survive removal of the original.
- **Background file reads.** Upload reads and checksums run off asynchronous executor threads. Cancellation does not wait for queued file work.
- **Bounded observation reads.** At most 2 reads run or await application. Pending observations retain their requests without blocking callers.
- **Background delivery.** Observers receive updates from library threads. Disk reads use a bounded WAL connection pool beside the writer. Releasing synchronization stops its background work. See [threading](docs/threading.md).
- **One core, two platforms.** All logic is Rust. Swift and Kotlin contain no business logic.
- **Versioned cached rows.** Each row sent to Swift or Kotlin includes its stored revision, so readers can reject older values.
- **Immediate cached entities.** The first reader gets a row already in memory without waiting for a subscription delivery or reading disk. Native callbacks never keep entity sources alive. Deliveries from a replaced database cannot overwrite current entities.


## Documentation

- [Releasing](docs/releasing.md)
- [Getting started](docs/getting-started.md)
- [Describing models](docs/models.md)
- [Local and synced models](docs/storage-models.md)
- [Live data and queries](docs/live-data.md)
- [Value bridge](docs/value-bridge.md)
- [Writing data](docs/writing-data.md)
- [Sync state of a record](docs/sync-state.md)
- [Server sync](docs/server-sync.md)
- [Files](docs/files.md)
- [Threading and performance](docs/threading.md)
- [Testing](docs/testing.md)
- [Architecture](docs/architecture.md)

Core tests check shared behavior. Swift and Kotlin tests check bindings and platform APIs. The full check also rejects missing network coverage reports.

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

See [release dependency examples](docs/getting-started.md#use-a-release) for Swift and Kotlin.

The generated bindings give you a live property. Declare the query, use the rows, the screen updates itself. Writes are one line and need no reload code.

### Swift

```swift
struct FeedView: View {
  let moments: Projected<Moment>

  init(scope: AccountScope, userId: String) throws {
    moments = Projected(try scope.stream(Moment.where(Moment.userId == userId).order(Moment.createdAt.desc).limit(20)))
  }

  var body: some View {
    List(moments.rows) { MomentRow(moment: $0) }
      .refreshable { try? await moments.refresh() }
  }
}

let moment = try await Moment.send(in: scope, text: "hello").row   // shown at once, queued, synced
try await moment.attach(.media, data: bytes, name: "a.png", mime: "image/png")   // typed file call
```

### Kotlin

```kotlin
class FeedModel(me: User, scope: AccountScope) : ViewModel() {
  private val query = Moment.where(Moment.userId eq me.id).order(Moment.createdAt.desc()).limit(20)
  val moments = Projected(scope.stream(query), query.decoder, viewModelScope)   // rows: moments.projection
}

val moment = Moment.send(scope, text = "hello").row            // shown at once, queued, synced
moment.attach(Moment.media, data = bytes, name = "a.png", mime = "image/png")   // typed file call
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
  credentials: AppCredentials(), modelsJSON: modelsJSON, path: path
)
try await DB.sessionCall("otp", body: PhoneBody(phone: "+61400000000"))
```

```kotlin
DBlayer.open(
  context,
  encrypted = EncryptedConfig("https://api.example.com", serverKey, serverPin, endpoints, protocolLabel, cablePurpose, cableChannel),
  credentials = AppCredentials(context), modelsJson = modelsJSON, path = path,
  uploadNotification = UploadNotification("Uploading", "Your files are on their way", R.drawable.ic_upload),
)
DBlayer.sessionCall("otp", PhoneBody("+61400000000"))
```

Await `DB.open` in Swift or call suspend `DBlayer.open` from a Kotlin coroutine.
Opening files, WAL and format checks runs on the library's worker pool.
Open the database once at app start. A second `open` while it is open throws, whatever its parameters. Failed opens allow retry.
`AppCredentials` is app code that implements `CredentialStore` over the Keychain or the Keystore, see [server sync](docs/server-sync.md).

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

The Rust toolchain is pinned in `rust-toolchain.toml` (channel and cross targets).

More in [docs/getting-started.md](docs/getting-started.md).

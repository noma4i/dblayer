# Changelog

## 5.2.0 - 2026-10-21

### Added
- `renumber` in a batch declaration rewrites position and size of the remaining rows after `discard` or `cancelUpload`.

## 5.1.0 - 2026-10-21

### Added
- `uploadCreate` with a list of `UploadItem` creates 2 to 10 rows with uploads.
- `batch` in an upload operation declares one content mutation with an items list.
- Batch rows are replaced by the answer through their `correlate` key without duplicates.
- One failed file fails only its row. Below the minimum, single mutations are sent.

## 5.0.3 - 2026-10-21

### Fixed
- A new row from an operation response with `lands` joins loaded `joins: :events` lists.

## 5.0.2 - 2026-10-21

### Fixed
- A repeated open with equal credentials, transport and HTTP values reuses the database.

## 5.0.1 - 2026-10-21

### Fixed
- A `loadMore` refused with `INVALID_CURSOR` replaces the list window with the first page.

## 5.0.0 - 2026-10-21

### Added
- `Query.changes` delivers inserted, updated, moved and removed rows of a live query in Swift and Kotlin.
- `RetryListener.retrying(attempt, delayMs, reason)` reports encrypted HTTP retries.
- `Database.setRetryListener` installs or removes the listener. `serverConfig` takes `retryListener`.
- Generated Kotlin models carry Compose `@Immutable`, so applications need no stability config.
- `build_ios.sh --device` builds the iPhone slice only. `build_kotlin.sh --release` builds the two ARM libraries only.

### Changed
- `DB.open` is `async throws` in Swift and `DBlayer.open` is `suspend` in Kotlin.
- Opening runs on the library worker pool. Repeated identical opens do nothing, changed parameters throw.
- Encrypted requests wait through network errors, timeouts, HTTP 408, 429 and 5xx instead of failing.
- `TIMEOUT`, `SERVICE_UNAVAILABLE`, `INTERNAL_SERVER_ERROR` and `DATABASE_ERROR` retry without an attempt limit.
- Delays double from 1 to 30 seconds with 20% jitter. Applications need no startup retry.
- A confirmed temporary refusal repeats with a new requestId and the next sequence.
- Cancelling an encrypted call stops the HTTP request and backoff and keeps Pending for resume.
- Swift `Task.cancel` reaches the core for bootstrap, `sessionCall` and `sessionResume` only.
- Kotlin encrypted HTTP uses 30-second call and read timeouts and a 10-second connect timeout.
- Swift encrypted HTTP uses 30-second request and resource timeouts without a separate connect timeout.
- Native release libraries use one codegen unit and symbol stripping. Android libraries shrink by about 39%.
- Android consumer rules keep JNA classes and native methods instead of every DBlayer class.
- Build scripts read the core count with `nproc` and fall back to `sysctl`.

### Removed
- The x86_64 simulator slice is no longer in the default iOS build.
- The default Android build no longer includes x86 and x86_64. `--abis` builds all four ABIs.

## 4.1.0 - 2026-10-21

### Added
- `typed: [afterSequence: :int, pinned: :bool]` declares scope params that are sent as a JSON integer or boolean. Each value has its own window.
- Swift and Kotlin take the typed params as `Int` and `Bool` (`Long` and `Boolean` in Kotlin).

## 4.0.0 - 2026-10-20

### Added
- `EncryptedSessionOperation` marks a session operation as sessionless in `sessionOperations`.

### Changed
- `sessionOperations` maps each name to an `EncryptedSessionOperation` with a path and `sessionless`.
- A sessionless call is sealed without a credential and a sequence.
- A sessionless call stores no pending request and takes no mutation lock.
- A pending request refused with HTTP 400, 404, 405, 413, 415 or 422 is dropped.
- A dropped request keeps the sequence and fails its queued operation.
- `sessionResume()` returns `nil` or `null` for a dropped request instead of throwing.

### Fixed
- A stored pending request of a sessionless operation no longer blocks every app start.

## 3.0.0 - 2026-10-20

### Added
- `node` and `nodes` read entities by id, with freshness windows and `force`.
- Interface and union lists, input lists and `oneOf` inputs in operation arguments.
- Enum lists and enum scope params with defaults.
- Connection edge values and totals are stored per scope and read beside the list.
- Stable order for scopes with `order_stable: true`.
- Encrypted cable subscriptions with Swift and Kotlin sockets.
- Declared change subscriptions: `changes` and `changed_by` reread or remove rows.
- `apply` on `listen` and `updates` selects `ephemeral`, `reread` or `ignore`.
- `EncryptedConfig` takes `endpoints`, `protocolLabel`, `cablePurpose` and `cableChannel`.
- `GraphqlExtensions.fields` carries any other extensions key as JSON text.
- Persisted query retry and versioned user agents.
- Database opens without a transport.
- Uploads are stored as operation items and resume through their saved requests.
- Upload retries are kept per step and `UploadInfo.attempts` sums unfinished steps.
- Mutation idempotency keys survive restarts.

### Changed
- Cable retries back off with jitter and reset only after a server frame.
- Any server error code of 1 to 64 uppercase characters is accepted.
- A refused subscription stops until an explicit resume.
- Singleton subscriptions stay stopped after logout until `resume`.
- Queued mutations are kept after a lost session.
- Server paths and protocol labels come from the application, not the library.
- Bootstrap config is fetched before the database opens.

### Fixed
- A cable that closed after `hello` no longer reconnects every second.
- Upload status checks and offline cancellation no longer block.
- An explicit null resets a `belongs_to` key.

### Removed
- Window membership snapshots.
- Built-in server paths, channel names and change subscription.

## 2.0.0 - 2026-10-17

### Added
- Typed enums in Swift and Kotlin, with an `unknown` case that keeps the server text.
- `scalar :Name, as: :kind` maps a custom scalar to a generated type.
- Union fields declared with `on` blocks become a Swift enum and a Kotlin sealed class.
- A union with an unknown `__typename` is read as `unknown` and keeps the type name.
- A union is stored in columns, so `where` by variant kind works.

### Changed
- Enum fields, inputs, arguments and results are typed enums instead of strings.
- A model enum column keeps a value outside its declared list.
- A custom scalar without a mapping fails generation.

### Removed
- The string form of enum values in generated code.

## 1.2.0 - 2026-10-17

### Added
- `Query.window(tail:)` and `LiveWindow` keep only the last rows of an ordered query live.
- Window snapshots carry `hasBefore`, `hasAfter` and `total`; `expand(before:)` grows the window.
- Windows work over sorted relations with a parameter column and joins.
- `Database.openEncrypted` sends GraphQL V3 requests as JWE with a pinned server key.
- `sessionCall`, `sessionResume`, `sessionReset` and `serverConfig` run the V3 sign-in session.
- A V3 mutation is stored before sending and resent unchanged after a restart.
- Typed session errors in Swift and Kotlin, with `CredentialStore` and `EncryptedHttp` hooks.

### Changed
- One change to an ordered list reads the touched rows, not the whole list.
- A landing page looks up its members by id instead of loading the list.

## 1.1.0 - 2026-10-16

### Added
- `Snapshot.edits` and `LiveList.takeEdits()` give ordered row steps, joined across missed deliveries.
- `replay` applies the steps to a mirror of the rows.

### Changed
- The list feed keeps decoded rows between deliveries and undoes a refused delivery without copying.

## 1.0.1 - 2026-10-16

### Fixed
- The `dblayer-gen` crate builds against the core version.

## 1.0.0 - 2026-10-16

### Added
- Normalized SQLite storage: typed columns, child tables for lists, objects and arrays, node rows for free-shaped values.
- Redesigned model declarations in `.dbl`.
- Nested value types generated from model declarations.
- Check of model declarations and `args` against the GraphQL schema.
- Typed `args` on create and update.
- Operation blocks for calls without a model.
- FTS5 text search and searchable fields in query builders.
- Local storage mode: writes never enter the outbox.
- Local fields that sync never overwrites.
- `peek` and warm reads from memory.
- Typed transport errors in Swift and Kotlin.
- Rows carried as cells over the bridge.

### Changed
- Outbox, sync state, window ids, quarantine and reset intent are stored in tables.
- A database file that does not fit the build is recreated on open.
- File calls run off the caller thread.
- A commit batches the old-row read and the node and list inserts.
- An operation whose root field is a list returns a list.

### Fixed
- Parent fields omitted by a child update are preserved.
- Entities are seeded from cached rows, which carry stored revisions.
- Background synchronization stops when its owners leave.
- Entity sources are released when their readers leave.
- Entity deliveries from replaced databases are ignored.
- Live lists await forced initial loads.

### Removed
- Multi-row inserts, which made upsert slower.

## 0.3.0 - 2026-10-13

### Added
- Entity registry shared by Swift and Kotlin, with a singleton entity.
- List deliveries carry changed rows with positions and apply to the array.
- `Query.load` loads a stale relation without a subscription.
- Test clock for core time windows.
- Replaced ids and load kind exposed in Swift and Kotlin.
- Merge rule declared on a field.
- Patch of one array element by key in an optimistic action.
- Parent pointer ordered by a tuple of child fields.
- Cable payloads delivered as database responses.
- Multipart init sends the checksum.
- R8 keep rules for JNA and UniFFI bindings.

### Changed
- Dates are stored as UTC text with milliseconds.
- An event row joins the loaded lists that declare it without a reload.
- Cable reconnect backs off by failed opens.
- Outbox is retired on logout.
- Kotlin module compiles against SDK 37.
- Unused write policy, admission, pacing and sync code removed.

### Fixed
- Confirmed list members survive stale responses.
- A sent temp row is replaced by the server row that lands first.
- A param-less remote list takes the rows this device creates.
- A thread that cannot start is reported as an open error.
- Poisoned locks are recovered.
- Failed entity reads stay dirty and are re-read at the next batch.

## 0.2.0 - 2026-10-11

### Added
- One multiplexed cable channel for all subscriptions of a connection.
- `resume` on the DB entry.
- Format version derived from a hash of the core table DDL.
- Parent field sets and a newer guard on `belongs_to`.
- Create of a record together with its upload.
- Declared result of an action.
- Server total in the load state of a paged list.
- Rows move between lists on param change; lists order by relation.
- Responses merge by the fields they select.
- Rows outside the response root land from an action.
- Refetch of a relation of another model after an action.
- Debounced model listen merges events per row.
- Auth token is read per request.

### Changed
- A reload after a commit waits for in-flight loads.
- Transport response bodies are read off the caller thread.
- A wipe of an incompatible database keeps `row_sync`; missing core tables are created at open.

### Fixed
- A failed subscription ends its live stream.
- A frame without data fails as internal.
- A refusal is kept until settled reads it.
- A send of an unregistered mutation fails.
- The default is stored when the server answers null for a non-null field.
- Integer ids are sent as numbers.
- The nested parent of an `id_from` field is kept.

## 0.1.0 - 2026-10-09

### Added
- Rust core with SQLite storage.
- Swift package and Kotlin module over a UniFFI bridge.
- `.dbl` parser and code generation for Swift, Kotlin and the core spec.
- GraphQL queries, mutations, actions and subscriptions.
- Live data with diffs, merge-based replace and automatic refetch.
- Relations with params, scopes and enum grouping.
- Ephemeral values.
- File fields and uploads.
- iOS and Android build scripts and `check_dblayer.sh`.

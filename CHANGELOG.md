# Changelog

## 1.1.0 - 2026-10-16

### Added
- `Snapshot.edits` and `LiveList.takeEdits()`: the ordered steps from the previous rows to the new rows, joined across deliveries a slow reader missed, in Swift and Kotlin.
- `replay` applies the steps to a mirror of the rows.

### Changed
- The list feed keeps the decoded rows between deliveries and undoes a refused delivery without copying the slots.

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

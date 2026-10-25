# Changelog

## Unreleased

### Added
- `DB.sessionCall` and `DBlayer.sessionCall` send a declared session operation with an `Encodable` / `@Serializable` body and decode the answer. `sessionResume`, `sessionReset` and `retryAll` move to `DB` and `DBlayer`. `Model.retry(id:action:)` (Kotlin `ModelDecoder.retry(id, action)`) retries a record by id.
- `preload` in a `.dbl` scope loads the scope after open, sign-in, reset and network return.
- `RetryProgress` follows encrypted HTTP calls that wait for a retry: `changes` on Swift and `flow` on Kotlin deliver `RetrySnapshot(activeCalls, lastAttempt)`. Pass one object as `retries` to `open` and `serverConfig`.
- `Model.events` streams declared change events with the row before and after on Swift and Kotlin; a full queue reports `missed` with the dropped count. `changed_by ..., via: :observed` rereads every observed read of the model.
- Uploads continue in the background: iOS sends files through a background `URLSession` and the app adds one `DB.handleBackgroundSession` line; Android runs a user-initiated job on API 34+ and a sticky `dataSync` foreground task on API 33, with the notification text and icon passed to `DBlayer.open`.
- The core tells the platform which accepted upload needs a system round, and a refusal by the system is written to the `error` of that upload only.
- Upload requests carry an `upload` mark, so Android sends them over the network of the job.
- `Query.related(Association)` reads typed related rows on Swift and Kotlin; `Model.Relations` lists links.
- Stale observed windows reload when the app returns to the foreground or the network returns.
- `UploadOperations` declares upload operations once in `DB.open` on Swift and `DBlayer.open` on Kotlin.
- `Query.loadAll(force:)` reads every page of a related list to its confirmed end, on Swift and Kotlin.
- Generated GraphQL inputs support recursive input objects and `@oneOf` choices.
- Kotlin `JsonElement.jsonText()` serializes JSON with the library settings.
- Kotlin `offCaller` runs a block off the caller thread, for generated code.
- Swift `Entity.changes()` streams the value of an entity, current value then updates, and holds the entity while it is read.

### Changed
- Swift `Model.retryAll()` and Kotlin top-level `retryAll()` are removed; use `DB.retryAll()` and `DBlayer.retryAll()`.
- The core decides what happens after a list delivery does not fit the rows of a reader: one whole list on the same subscription, a failure after 2 refusals in a row. Swift and Kotlin only report the refusal; the subscription is no longer opened again.
- `RetryListener` and `Database.setRetryListener` are removed; `serverConfig(retryListener:)` becomes `serverConfig(retries:)`. A call leaves the retry state on success, failure and cancel by itself.
- Preserve typed errors in `Snapshot.error` and `LiveWindow.failure` on Swift and Kotlin.
- A live query delivers again when only the GraphQL `extensions` of the same error change.
- The core decides every `Entity` transition (seed, revision order, stale, failure, reset, close) with one state machine on both platforms. A late failure of a replaced singleton feed no longer changes the new state.
- Stored operation, upload and sync-state errors keep GraphQL `extensions` as node rows instead of JSON text. An existing database file is recreated on open.
- Cable reopens right after the network returns, without waiting for a pending retry delay.
- Kotlin `Entity.value` keeps the entity reachable while it is collected.
- Swift query conditions, `JSONValue.encode`, `JSONValue.text` and `JSONValue.tagged` throw on serialization failure.
- Generated Swift parameterized scopes and upload variable builders throw on serialization failure.
- Kotlin `parseJsonObject` throws when the body is not a JSON object.
- Cancelling a single upload after its content mutation starts returns false.
- Upload phase and stage change only through typed events of one state machine, and operation status changes follow a second machine; stored data and public API are unchanged.
- Uploads reject local and memory models without creating records or files.
- Upload sources are copied and hashed before the operation is written.
- Upload file reads and checksums run on blocking tasks and stop on cancellation.
- Upload transfers reserve their file slot before starting and hold it through retries.
- Observation reads queued, running or awaiting application are limited to two.
- Database calls prepare JSON and decode answers off the caller thread on both platforms.
- Live lists and windows build their ids off the main thread.
- `takeEdits()` is replaced by `takeUpdate()`: rows, edits and revision from one delivery.
- Kotlin live lists and windows publish their state from a background thread.
- Generated writes, actions, node reads and operations run off the caller thread.
- `Query.cached()` and `cachedRow()` are now `async` in Swift and `suspend` in Kotlin.
- Kotlin entity readers no longer wait while a delivery applies.
- Every commit on a disk database plans on a read connection outside the writer. The writer only checks the revision, and a commit that landed first makes the plan run again off the writer.
- Upload, run, load, refresh, load-more and warm calls parse their JSON arguments and convert results on the library executor, not on the caller thread.
- `Database.peekQuery` and `Database.edgeValues` are `async`. `Query.edgeValues` and the generated `<scope>Edge` reads are `async` in Swift and `suspend` in Kotlin and do their work on the library executor.
- Loading rows builds them and sizes them outside the row cache lock, so cached reads are not held back by a large load.
- Cancelling a subscription and expanding a window never wait for the observer registry: a busy registry takes the request later.
- A listener call never starts after the cancel of its subscription returns, also when the delivery thread had already passed the cancel check.
- A refresh deferred by the read limit or a running read keeps its touched rows and reads only those, instead of re-reading the whole list.
- Connecting a live query to a remote relation no longer waits for the storage writer: the stored window state is read on a blocking task and the query delivers its first snapshot, merged from everything that happened meanwhile, once the state is known.
- A commit that touches none of a query's rows no longer notifies every live query: only queries whose relation state is ahead of their rows ask to be told.
- Upload files are written to the cache and removed on the blocking pool, never on an async executor.
- Removing files after a confirmed destroy or a server destroy event runs on the blocking pool.
- Uploads are identified by their declared upload action (`model:upload:action`): `upload`, `uploadCreate` and `cancelUpload` take the action, a record can hold one upload per action, and an undeclared action admits nothing.
- Swift and Kotlin pass upload requests as typed records; `UploadRequest` no longer carries the content mutation, it comes from the declared upload action, and an upload action without a mutation is refused.
- Swift `upload`, `create(uploading:)` and `cancelUpload` and Kotlin `upload`, `uploadCreate` and `cancelUpload` take the declared upload action and run off the caller thread.
- `retry`, `discard` and `settled` on Swift and Kotlin take a required `action` that is `nil`/`null` for the whole record.
- A model exposes `uploads[action]` instead of `upload`.
- A record reports its uploads in `SyncInfo.uploads`, one entry per declared action, stored in the `sync_upload` table; `SyncInfo.actions` lists ordinary operations only. The single `upload` property of a record is removed.
- `settled` of one action reports the failure of that action's upload or operation only, and an unknown action is refused as such.
- `retry`, `discard` and `settled` take an optional declared action: none selects the whole record, an action selects only its operations or uploads, and an unknown action is refused.
- Failed uploads older than 7 days are removed and aborted at the first flush after start and when connectivity returns. A failed batch item is removed on its own and its siblings stay.

### Fixed
- A session failure is no longer hidden by another subscription's failure.
- Explicit null stays distinct from an absent field after reopening the database.
- Non-finite numbers fail serialization instead of becoming null.
- Fields named like SQL keywords work in storage, indexes, filters and updates.
- Generated APIs and GraphQL inputs escape Swift and Kotlin keywords and keep serialized keys.
- List changes reserve order keys together, so removals release keys before additions.
- Rows that join one list in a single commit, and appends planned from a stale snapshot, receive distinct order keys instead of failing the commit.
- Kotlin GraphQL subscriptions end when the server closes the WebSocket.
- Recovery and temporary-id replacement keep records of different models with equal ids separate.
- Interrupted database resets resume from the complete recorded model list.
- Cloud replies from before a database reset are rejected before writing rows.
- `UseLocal` keeps edits made while remote clocks are fetched.
- Cloud values received before their server row are kept until the row arrives.
- Subscriptions connect without waiting for storage.
- Unused load state is released after its last observer and request finish.
- File cleanup failures are reported, and committed row changes stay committed.
- A failed multipart `complete` keeps confirmed parts and the upload id.
- A database reset rejects upload admission queued before the reset.
- Bundled SQLite respects the macOS 14 deployment target.
- Dropping `Sync` no longer cancels queued upload file cleanup.
- Discarding or rolling back an upload keeps its multipart abort until the server answers, with or without confirmed parts.
- A finished or committed upload never records a multipart abort.
- Discarding one item of a rejected batch operation removes that item instead of failing as in flight, and a rejected batch operation older than 7 days expires like a failed item.
- A panic inside an upload task fails that upload and frees its file slot, so queued uploads keep starting.
- One unreadable upload no longer stops the other waiting uploads from starting.
- Waiting for a record or for one queued action reads the in-memory queue and no longer loads every queued operation on each event.
- Two uploads of one file no longer fail when they write the cache copy at the same time.
- A failure while removing expired uploads or sending a recorded abort is reported by `flush` and retried at the next start or online event.

### Removed
- The `liveHasMany` bridge call; instance links use `Query.related`.
- `readEdgeValues` and the generated `read<Scope>Edge` functions: `edgeValues` and `<scope>Edge` are the only reads and are `async` in Swift and `suspend` in Kotlin.
- `UploadRequest` and `UploadBatchRequest` lose `createBlob` and `multipart`.
- Swift `JSONValue.parse`.

## 6.7.0 - 2026-10-24

### Added
- Entities expose opening, present, absent, failed and closed phases.
- `CableLease` and `SubscriptionSink` receive the transport token through `opened`.
- `loadMore` reports loaded pages, exhausted pagination and concurrent loading.

### Changed
- Custom transports report tokens through leases instead of returning strings.
- `DB.open` and `DBlayer.open` take a headers closure read on every request.
- Kotlin `update` and `set` write null through `GraphQLNullable`.
- Kotlin maps the DSL type `:int` to `Long` in every generated output.
- Swift `JSONValue` keeps integers exact as `Int64`.
- Swift cached reads throw when reading or decoding fails.
- `Snapshot.total` and `WindowEdges.total` use one integer type on each platform.
- Android uses JNA 5.19.1.

### Fixed
- A Kotlin process no longer aborts on a corrupted foreign future result.
- A stored operation with an invalid id fails instead of losing the id.
- Waiting for a record matches its model as well as its id.
- An answer kept for one model stays available when another model asks first.
- A connection with an invalid `hasNextPage` fails instead of ending the list.
- Cancelled transport calls release tokens reported before or after cancellation.
- Interrupted cable closing keeps token ownership until cleanup completes.
- Logout waits until subscribers receive the reset revision.
- Cache resets discard unobserved loading windows.
- Subscription failures survive notification queue overflow.
- Read and decoding errors reach callers or live snapshots.
- Invalid subscription requests leave no open socket.
- Cancelling live reads does not show a failure.
- Concurrent initialization with another UniFFI library avoids a JNA lock inversion.

### Removed
- Separate query loading-state streams.
- Swift `UrlSessionTransport` initializer that takes a headers dictionary.

## 6.6.0 - 2026-10-23

### Added
- `DatabaseNotOpen` error for calls on a closed or unopened database.
- `generate_kotlin_bindings.sh` generates Kotlin bindings from explicit paths.
- `MockTransport` mode `networkError` answers an unqueued call with a network error.

### Changed
- A subscription delivers rows, phase, error and revision in one value.
- The first value of a subscription waits for the first read of its rows.
- A background load error with rows keeps the phase loaded and carries the error.
- `DB.database` throws `DatabaseNotOpen` instead of crashing when the database is closed.
- `subscriptionCensus`, `entityCensus` and `openReport` throw on a closed database.
- `stream()` of live sources and query changes is `async throws`.
- Kotlin `LiveSource.flow` throws the subscription error when collected.
- A singleton entity of a closed database becomes closed.
- A rejected query fails only its caller, the relation window stays intact.
- An entity delivery carries `failure` when its read failed.
- `MockTransport` fails the test that makes a call without a queued reply.
- `MockTransport` records an unknown unsubscribe token on both platforms without throwing.

### Fixed
- `logout` returns after held entities received their removal.
- Switching lists no longer opens a second viewer changes subscription.
- A Kotlin process no longer aborts when an async transport callback completes early.

## 6.5.0 - 2026-10-23

### Changed
- A subscription returns at once, its first value arrives after a reader thread reads it.
- `retain` returns without a database error, a failed read keeps the last rows.
- An invalid relation query fails the window of that relation with `invalid`.
- On Kotlin a second class loader in one process throws `IllegalStateException`.
- Mock transports record unsubscribe tokens they never issued, the Kotlin mock throws.

### Fixed
- A panic while handling one observed group no longer stops other groups.
- Sync, uploads and cloud share one network state per database.

## 6.4.0 - 2026-10-23

### Added
- `DBlayer.close()` and `DB.close()` close the database for tests and process end.
- `Entity.isClosed` marks an entity whose database closed.
- `entityCensus()` and `entityLeaseFailures()` count entity reads, skips and failed leases.

### Changed
- A second `open` while the database is open throws. Open it once at app start.
- `DBlayer.database` and `DB.database` cannot be assigned from outside.
- Signing out keeps held entities: they show empty, then the next account's rows.
- A returning network reloads observed windows whose load failed offline, once.

### Removed
- Public `Database.open` and `Database.openWithClock`.

### Fixed
- An entity receives its row when the first read for it failed.
- A load after reopening the database is no longer discarded.

## 6.3.0 - 2026-10-22

### Added
- Local models and `local: true` attributes sync across devices of one account.
- `cloud` in a model lists the attributes that sync.
- `connectCloudKit(container:)` syncs through the CloudKit private database on iOS.
- `connectDrive(activity)` syncs through Google Drive app data on Android.
- `CloudKitLink.receive` passes a CloudKit push to the library.
- The open report names the changed models and the reset models.

### Changed
- Android requires API 33 or newer.
- A changed spec of any server model clears the whole server cache.
- Local models and `local: true` values survive a server cache reset.
- Associations and storage join the model fingerprint, which rebuilds the cache once.

### Fixed
- Cancelling one caller of a shared load no longer fails the other callers.
- A cancelled load no longer leaves its relation in the loading state.

## 6.2.0 - 2026-10-22

### Added
- A scope with parameters opens one live subscription per parameter set.
- Generation with `--schema` checks subscription arguments against the server schema.

### Fixed
- Scope subscriptions send the read parameters as subscription variables.

## 6.1.0 - 2026-10-22

### Added
- `.dbl` declares fields with `attribute` and scope arguments with `parameter`.
- `query`, `response from:`, `paginate size:`, `freshness` and `subscribe` describe the server side of a scope.
- `upsert`, `match_by`, `insert_on` and `stable_order` name response, correlation and list rules.
- `validates :items, length:` sets the size limits of a batch upload.
- Generation fails on an unknown, incomplete, repeated or misplaced declaration.

### Changed
- One change to a scoped list reads only the changed rows.
- A second window over the same query shares its order index.
- `max_pages` counts fetched pages and resets on refresh.
- A confirmed create no longer reloads lists that insert rows from events.
- The cache format changed. The cache is rebuilt on the first open.

### Removed
- The previous `.dbl` words: `field`, `fetch`, `root`, `lands`, `correlate`, `joins`, `listen`, `updates` and others.
- `local_effect` and its `reason`.
- Untyped `:object` and `:array` fields. Use `:json` or a named type.
- `default` on non-enum scope parameters.

## 6.0.0 - 2026-10-22

### Added
- `parse: :json` on a `raw` field stores a JSON text string as a JSON value.
- `Query.changes` collapses an overflowing queue into one `replaced` change with a full snapshot.
- `retry()` and `retryAll()` return the number of retried operations and throw errors.
- Kotlin `DBlayer.open` and `DBlayer.serverConfig` take a `Context` and watch the network.

### Changed
- A repeated `open` reuses the database only for the same callback instances.
- Encrypted requests retry without a limit online and make no attempt offline.
- `load()` fails at once with `Offline` when the device has no network.
- Bootstrap waits for the network before the first request.
- A mutation that is not delivered after 40 attempts stays `Failed` with its edit.
- A rollback happens only when the server refuses a mutation.
- A local write keeps fields of pending edits against loads, events and responses.
- A row from an event or `lands` joins a loaded list missing it.
- `lands` accepts a list root.
- `discard` of a batch member updates the saved operation input.
- The database format changed. The cache is rebuilt on the first open.

### Fixed
- Several rows written in one transaction no longer get the same list position.
- A batch rollback restores the parent row values.
- A dropped upload item removes all its saved state.
- Cancelling a native HTTP request stops the response body read.

### Removed
- The second platform transport: Swift `DatabaseTransport`, `GraphQLTransport`, payload signing and Kotlin `Crypto`.
- Equality of callback objects in `open`.

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

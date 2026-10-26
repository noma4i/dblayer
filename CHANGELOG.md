# Changelog

## 6.10.0 - 2026-10-27

### Added
- Kotlin `rememberLiveList(query)` and `rememberLiveWindow(window)` own the scope of a live list in Compose.
- `Model.entity(id, fetch: .force|.stale|.none)` reads the node when a reader appears, and `refreshError` shows a failed read.
- `merge: :stable_list` keeps the places of a stored id list, appends new ids and drops vanished ones.
- `refresh_on_foreground: true` on a scope reloads its read windows at every foreground event.
- A window over a scope loads it, and `expand(before:)` loads an older page.
- `window(tail: 60, max: 240)` shrinks the window to `tail` rows past `max`.
- `window(tail:).related(Relations.x)` reads the targets of window rows.
- `optimistic ids: { include: { of: Model, field: :id }, when: :flag }` adds or removes an id.
- `optimistic count: { at_least: 1, when: :flag }` raises a number while the flag is true.
- `subscribe id: ["chatId", "userId"]` keys a row by a pair, and `delete_unless:` deletes on false.
- `changed_by "AREA", via: :node, id_of: { of: Model, field: :attr }` rereads the node a row points to.
- `optimistic attr: { from: :param, map: { VALUE: "literal" } }` writes the literal that belongs to an enum parameter.
- `local :attr, from: { of: Model, field: :name }` fills a `create` attribute from a singleton row.
- `changed_by` may name one area as `via: :node` and as `via: :observed` on a model.
- A committed credential reloads stale windows with readers and restarts stopped subscriptions.
- `preload vibeId: { from: "Vibe.list" }` loads one scope for each row of another scope, and a new row adds its scope.
- `DB.refreshPreloads()` and `DBlayer.refreshPreloads()` reload every `preload` scope in one call, fresh or not, and name the scopes that failed.
- `add_to` and `remove_from` join a row to a scope or take it out.
- `create_row` adds a row of another model that the server answer replaces.
- `change_row` changes attributes of another row and shifts numbers with `minus:` or `plus:`.
- A refused operation takes back only its own membership, created row and changed attributes.

### Changed
- `LiveWindow` connects its scope like `LiveList` instead of reading the local database only.
- `preload` waits for the first snapshot of the app and runs behind loads the app asked for.
- A committed credential starts the model subscriptions, and `logout()` and `sessionReset()` stop them.

### Fixed
- A freed owner of entities returns `subscriptionCensus` to its base: the core drops the entity observer when its last id is released and creates it again on the next retain.

### Removed
- `DB.listen()`, `DB.unlisten()`, `DBlayer.listen()` and `DBlayer.unlisten()`.

## 6.9.2 - 2026-10-26

### Added
- An optimistic keyed element carries the nested object of the local row its declared `ref:` and `id:` name.

### Changed
- `expires_in` of a memory model counts from the last write, and a write of equal values renews it.

### Fixed
- `entity(id)` of a record whose reader was freed no longer calls the core again while its value or absence is retained.
- `confirmed` returns the server id to a waiter that the closing commit of a create wakes.

## 6.9.1 - 2026-10-26

### Fixed
- A live read opened while a server reply replaces a temporary row returns the rows before or after the swap, never an empty set.

## 6.9.0 - 2026-10-26

### Added
- `preload` parameter on `DB.openTesting` and `DBlayer.openTesting`, default false, keeps scope preloads quiet in tests.

### Changed
- `preload` scopes wait while the device saves power and never load on a low-RAM Android device.

### Fixed
- An action that declares a result reads its answer when the payload row is null.
- `changed_by` accepts one change area on several models, each model rereads its own read.
- Local writes check nested objects and lists against their declared type.
- `TestClock.advance` expires due memory rows and resets ephemeral values at once.

## 6.8.0 - 2026-10-26

### Added
- `DB.sessionCall` and `DBlayer.sessionCall` send a declared session operation and decode the answer.
- `sessionResume`, `sessionReset` and `retryAll` on `DB` and `DBlayer`.
- `Model.retry(id:action:)` and Kotlin `ModelDecoder.retry(id, action)` retry a record by id.
- `preload` in a `.dbl` scope loads the scope after open, sign-in, reset and network return.
- `RetryProgress` reports encrypted HTTP calls waiting for a retry through `retries` in `open` and `serverConfig`.
- `Model.events` streams declared change events with the row before and after.
- `missed` event item reports the count of events dropped by a full queue.
- `changed_by ..., via: :observed` rereads every observed read of the model.
- Background uploads on iOS through a background `URLSession` and `DB.handleBackgroundSession`.
- Background uploads on Android through a user-initiated job and a `dataSync` foreground task.
- `Query.related(Association)` reads typed related rows, and `Model.Relations` lists links.
- `Query.loadAll(force:)` reads every page of a related list to its confirmed end.
- `UploadOperations` declares upload operations once in `DB.open` and `DBlayer.open`.
- `uploads[action]` on a model and `SyncInfo.uploads` report one upload per declared action.
- Generated GraphQL inputs support recursive input objects and `@oneOf` choices.
- Kotlin `JsonElement.jsonText()` serializes JSON with the library settings.
- Swift `Entity.changes()` streams the current value of an entity, then its updates.
- Stale observed windows reload when the app returns to the foreground or the network returns.
- `attach`, `file`, `read`, `url` and `detach` on a record for each `:file` attribute.
- `FileInfo` describes a stored file on Swift and Kotlin.

### Changed
- `create` and `update` refuse an undeclared field or a wrongly typed value.
- A row with a temporary id sorts before permanent rows of equal order.
- The store hook runs after the writer lock is released.
- `serverConfig(retryListener:)` becomes `serverConfig(retries:)`.
- A `:file` attribute reads as `FileInfo?`, and `create` and `update` take no file parameter.
- `takeEdits()` becomes `takeUpdate()`: rows, edits and revision from one delivery.
- A delivery that does not fit the rows gives one whole list.
- The subscription stops with a failure after two refusals in a row.
- `Snapshot.error` and `LiveWindow.failure` keep typed errors.
- A live query delivers again when only the GraphQL `extensions` of the same error change.
- Stored operation, upload and sync-state errors keep GraphQL `extensions` as node rows.
- An existing database file is recreated on open.
- Cable reopens right after the network returns, without waiting for a retry delay.
- Swift query conditions and `JSONValue` encoding throw on serialization failure.
- Generated Swift scopes and upload variable builders throw on serialization failure.
- Kotlin `parseJsonObject` throws when the body is not a JSON object.
- Cancelling a single upload after its content mutation starts returns false.
- Uploads reject local and memory models without creating records or files.
- `upload`, `uploadCreate` and `cancelUpload` take the declared upload action.
- A record holds one upload per declared action, and an undeclared action admits nothing.
- Upload requests are typed records, and the content mutation comes from the declared upload action.
- `retry`, `discard` and `settled` take an `action`, `nil` or `null` for the whole record.
- `settled` of one action reports the failure of that action only.
- `SyncInfo.actions` lists ordinary operations only.
- Failed uploads older than 7 days are removed at the first flush after start.
- A failed batch item is removed on its own and its siblings stay.
- `Query.cached()` and `cachedRow()` are `async` in Swift and `suspend` in Kotlin.
- `Database.peekQuery`, `Database.edgeValues` and `<scope>Edge` reads are `async` or `suspend`.
- Database calls, writes, actions and operations run off the caller thread.
- Kotlin live lists and windows publish their state from a background thread.
- Observation reads queued, running or awaiting application are limited to two.
- A commit that touches no row of a query no longer notifies that live query.
- A refresh deferred by the read limit rereads only its touched rows.
- A live query on a remote relation delivers its first snapshot without waiting.

### Fixed
- An expiring memory row written again before its removal commits stays alive.
- A batch of server events keeps both rows that touch the same parent.
- The answer of an action is ready when its operation turns committed.
- A refused batch of server events reloads the observed lists of its model.
- A failed reload reports a gap to the readers of model events.
- A session failure is not hidden by another subscription's failure.
- An unreadable mutation answer keeps the operation in delivery unknown.
- A destroy answered with `NOT_FOUND` closes as deleted.
- Explicit null stays distinct from an absent field after reopening the database.
- Non-finite numbers fail serialization instead of becoming null.
- Fields named like SQL keywords work in storage, indexes, filters and updates.
- Generated APIs escape Swift and Kotlin keywords and keep serialized keys.
- Rows joining one list in a single commit receive distinct order keys.
- Kotlin GraphQL subscriptions end when the server closes the WebSocket.
- Recovery and temporary-id replacement keep equal ids of different models separate.
- Interrupted database resets resume from the complete recorded model list.
- Cloud replies from before a database reset are rejected.
- `UseLocal` keeps edits made while remote clocks are fetched.
- Cloud values received before their server row are kept until the row arrives.
- Subscriptions connect without waiting for storage.
- File cleanup failures are reported, and committed row changes stay committed.
- A failed multipart `complete` keeps confirmed parts and the upload id.
- A database reset rejects upload admission queued before the reset.
- Bundled SQLite respects the macOS 14 deployment target.
- Discarding an upload keeps its multipart abort until the server answers.
- A finished upload never records a multipart abort.
- Discarding one item of a rejected batch operation removes only that item.
- A rejected batch operation older than 7 days expires like a failed item.
- A panic inside an upload task fails that upload and frees its file slot.
- One unreadable upload does not stop the other waiting uploads.
- Two uploads of one file no longer fail when they write the cache copy together.
- `flush` reports a failure while removing expired uploads and retries at the next start.

### Removed
- The `liveHasMany` bridge call; use `Query.related`.
- `readEdgeValues` and generated `read<Scope>Edge`; use `edgeValues` and `<scope>Edge`.
- `createBlob` and `multipart` from `UploadRequest` and `UploadBatchRequest`.
- `RetryListener` and `Database.setRetryListener`.
- Swift `Model.retryAll()` and Kotlin top-level `retryAll()`; use `DB.retryAll()`.
- The single `upload` property of a record.
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

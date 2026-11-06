# App migration

## Lost session state

Replace the lost-state message comparison with `DbError.Session` code `.stateLost` on Swift or `DbException.Session` code `SessionCode.STATE_LOST` on Kotlin.

## Event payloads

Replace Chat's MESSAGE `id_of: { of: Message, field: :chatId }` declaration with `payload: "chat"` on its node scope.
Add `payload: "message"` to Message's MESSAGE node declaration and retain its terminal `removes_on` kinds.
Add `attribute :serverRevision, :int, from: "revision"` and `server_revision :serverRevision` to Chat.
Map the server's Int64 scalar with `scalar :Int64, as: :long`.
Select the full Chat projection, including revision and list membership, inside `EntityChange.chat` in the subscription document.
Select the full Message projection inside `EntityChange.message`. Terminal message events can provide null there.
Use the same Chat revision field in node and page documents.
Regenerate Swift and Kotlin models with the schema and operation documents.
Remove the manual Chat reread and list refetch after MESSAGE on both platforms.
Keep the generated list scope and open `scope.events(Chat)` for the events. The core applies payload rows, membership and event snapshots.
Without a payload declaration, the declared identifier-based read remains forced. There is no TTL skip or automatic fallback.
Rust `ChangeRead` literals add `payload: None` for identifier-based reads. `ModelSpec` adds the optional `server_revision` field.

## Blob uploads

Replace `DB.upload(.videoIntro, request:)`, `DB.blob(id)`, `Blob.waitUntilUploaded()` and `Blob.snapshot` on Swift, and `DBlayer.upload(BlobUpload.VideoIntro, request)`, `DBlayer.blob(id)`, `waitUntilUploaded()` and `snapshot` on Kotlin, with a model action that declares the upload. Send the record with `upload: UploadCommand(action:, request:)` and read `uploads[action]` on the record. Remove the blob upload declaration and `parameter :video, :blob` from the `.dbl` files.

## Media library marker

Add `marker "chatMediaState", from: "node.mediaLibrary", fields: [:revision, :visualCount, :audioCount, :giftCount], sequence: :latestSequence` to the media library scope.
Add `tail "chatMedia", after: :afterSequence` beside the existing `query`, `response`, `paginate` and `freshness`.
The `chatMediaState` query takes `chatId` and `upToSequence`, and selects every marker field and `latestSequence`.
The `chatMedia` query takes `afterSequence`, `first`, `after` and returns messages in ascending sequence order.
The source of `revision` is `ChatMediaLibrary.revision`, the server's `chats.media_revision`. `Chat.revision` does not replace this marker.
Regenerate Swift and Kotlin models with the GraphQL schema and query documents.
Open `Projected(try scope.stream(ChatMedia.media(chatId)))` on Swift or `Projected(scope.stream(ChatMedia.media(chatId)), ChatMedia, screenScope)` on Kotlin.
Remove the local Chat fields: `mediaSequence`, `mediaVisualCount`, `mediaAudioCount`, `mediaGiftCount`.
A typed SQLite marker owned by the core replaces them.
Remove counter comparisons and manual tail loading from ChatMediaModel on both platforms.
Existing snapshot and refresh calls carry rows, completeness and errors. The app makes no additional calls.
Rust: existing `WindowSave` literals add `marker: None`, `invalidated: false` unless they confirm a media library marker.

## Preloading and members

Derived preload: `preload vibeId: { from: "Compass.rows" }, limit: 40`.
`limit` is required for `from`, is positive, and applies to the parameter product. Parameters follow declaration order, and ids follow the source scope.
Remove `preload ... { from: ... }` declarations without `limit`. `limit` is now a reserved key of the `preload` line.
Full membership scope: `scope :chat_members, members_complete: true do`, existing `query`, `response`, `paginate size: 100, direction: :forward`.
`max_pages` is incompatible with `members_complete`. The server limit for a members page is 100.
Swift: `Projected(try scope.stream(User.chatMembers(chatId)))`.
Kotlin: `Projected(scope.stream(User.chatMembers(chatId)), User, screenScope)`.
Read `state.phase`, `state.error`, `state.complete` from the projection.
Completeness occurs only after `hasNextPage == false`. A short or empty intermediate page does not end the chain.
On error, partial rows remain: `Loaded` with `error`, `complete = false`. Without rows, the phase is `Failed`.
The last reader cancels the request. The next reader continues from the saved cursor. Network return continues a failed chain with readers.
Remove `CompassRowLoads`, manual `loadAll`, the completeness flag of `ChatGroupDetails`, the second list and app-side row slicing.
SQLite stores the full membership.
`rows` and `state.error` of the projection read the same value. Regenerate models for both platforms.

## Create sources

Replace singleton reads and copied create arguments with repeated `local :field, from: { of: Model, field: :source }` declarations.
Regenerate Swift and Kotlin models. Each sourced argument is optional on both platforms.
Keep explicit product values as arguments. Use each destination once.
Remove `from:` from update declarations. Update calls pass their local values explicitly.

## Subscription filters

Replace `matches: { kind: "ACTIVE" }` with `matches: { kind: ["ACTIVE"] }`.
Use `matches: { kind: { not: ["SCREENSHOT"] } }` for exclusion.
Declare ChatAction with the five accepted activity kinds. Declare ChatActivityNotice with `["SCREENSHOT"]` on the same subscription.
Remove app-side kind switches, receive/stop calls and notice copying. Regenerate both platforms.
Model-level subscription declarations are read with `scope.events(Model)` on Swift and Kotlin.
Regenerate both platforms and replace the `events` property with `scope.events(Model.self)` on Swift or `scope.events(Model).events(Model)` on Kotlin.
Start actions after this call returns: the first reader and server subscription are ready.
Remove offline retry loops around this call. The library waits for connectivity and owns cancellation and reader release.
A stream is one reader. Open a new one for each screen.
Handle opening errors with their original `DbError` kind. Later collection failures use `ModelEventItem.failed` or `ModelEventItem.Failed`.

## Node membership

Add `membership list: { contains: :lists }` to Chat.list and include `:node` in `insert_on: [:event, :response, :node]`.
Select `viewerState.lists` into Chat.lists in the node response. Chat.status alone does not describe all memberships.
Remove app-side insertion and list refetch after MESSAGE. Regenerate Swift and Kotlin models.
The existing generated list and node calls keep their signatures.
Missing members join the head. Existing members keep their places. Manual node reads do not insert rows into lists.

## One owner per stream

Replace `LiveList`, `Live`, `Entity`, `.live`, `.snapshots`, `.find(id)`, `.entity(id)` and `Model.events()` with `AccountScope` streams and `Projected`. See [live data](live-data.md).
Replace `Model.create` of a message or a media record with `Model.send(in: scope, ...)`. See [writing data](writing-data.md#sending-without-a-network).
Replace `add_to`, `remove_from`, `create_row` and `change_row` lines with `upsert` and `refetch`. An action that needs the server fails at once while the network is away.
Make a new `AccountScope` after each sign-in and build the screens again for it.

## Journal of a stream

Read the state of a projection instead of waiting on the raw stream. `loaded()` and `shown(revision)` are gone (see API v2).
Handle `ModelEventItem.ready` and `ModelEventItem.lost(first, last)` in an events loop. `missed(count)` is gone: the count is `last - first + 1`.
A test or a screen that must hear every event after a moment waits for `ready` first.
Give each caller of a shared load its own `beginLoad()` request and cancel it when the caller leaves. The request stops only after every caller cancelled.
A custom `ValueSink` implements `available()` and reads the stream with `readStream` until it returns nothing.

## API v2

The platform wrappers no longer expose these calls. Each one has an outcome in the core or needs no call.

| Removed call | What the app does instead |
| --- | --- |
| `DB.resume()` / `DBlayer.resume()` | Nothing. The core reloads stale relations on foreground, on network return and after sign-in. |
| `flush` | Nothing. The queue of writes is sent by the core. Read the outcome of each write from its receipt. |
| `Query.warm()` | Nothing. Reads and streams fill the memory themselves. |
| `ValueStream.reach(revision)` / `Projected.shown(revision)` | Read the outcome of the receipt of the write. |
| `ValueStream.resync()` | Nothing. A projection that meets a delta it cannot apply waits for the next snapshot. |
| `OperationReceipt.outcome()` (batch outcome) | Read the outcome of the receipt of each item. |
| `Projected.loaded()` | Read the result of `begin_load()` for the relation. |

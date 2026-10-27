# App migration

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
Keep `Chat.events()` and the generated list scope. The core applies payload rows, membership and event snapshots.
Without a payload declaration, the declared identifier-based read remains forced. There is no TTL skip or automatic fallback.
Rust `ChangeRead` literals add `payload: None` for identifier-based reads. `ModelSpec` adds the optional `server_revision` field.

## Media library marker

Add `marker "chatMediaState", from: "node.mediaLibrary", fields: [:revision, :visualCount, :audioCount, :giftCount], sequence: :latestSequence` to the media library scope.
Add `tail "chatMedia", after: :afterSequence` beside the existing `query`, `response`, `paginate` and `freshness`.
The `chatMediaState` query takes `chatId` and `upToSequence`, and selects every marker field and `latestSequence`.
The `chatMedia` query takes `afterSequence`, `first`, `after` and returns messages in ascending sequence order.
The source of `revision` is `ChatMediaLibrary.revision`, the server's `chats.media_revision`. `Chat.revision` does not replace this marker.
Regenerate Swift and Kotlin models with the GraphQL schema and query documents.
Open `LiveList(try ChatMedia.media(chatId))` on Swift or `LiveList(ChatMedia.media(chatId), scope)` on Kotlin.
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
Swift: `LiveWindow(try User.chatMembers(chatId).window(tail: 50, max: 200))`.
Kotlin: `LiveWindow(User.chatMembers(chatId).window(tail = 50, max = 200), scope)`.
Read `snapshot.phase`, `snapshot.error`, `snapshot.complete`, through `snapshot.value` in Kotlin.
Completeness occurs only after `hasNextPage == false`. A short or empty intermediate page does not end the chain.
On error, partial rows remain: `Loaded` with `error`, `complete = false`. Without rows, the phase is `Failed`.
The last reader cancels the request. The next reader continues from the saved cursor. Network return continues a failed chain with readers.
Remove `CompassRowLoads`, manual `loadAll`, the completeness flag of `ChatGroupDetails`, the second list and app-side row slicing.
Keep `expand(before: 40)` / `expand(before = 40)`. SQLite stores the full membership, and the UI receives at most 200 rows.
`rows`, `failure` and edges of `LiveWindow` read the same snapshot. Regenerate models for both platforms.

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
Model-level subscription declarations expose `Model.events()` on Swift and Kotlin.
Regenerate both platforms and replace the `events` property with `try await Model.events()` on Swift or `Model.events()` on Kotlin.
Start actions after this call returns: the first reader and server subscription are ready.
Remove offline retry loops around this call. The library waits for connectivity and owns cancellation and reader release.
Reusing the returned stream creates an independent reader for each later collection, including after completion or cancellation.
Handle opening errors with their original `DbError` kind. Later collection failures use `ModelEventItem.failed` or `ModelEventItem.Failed`.

## Node membership

Add `membership list: { contains: :lists }` to Chat.list and include `:node` in `insert_on: [:event, :response, :node]`.
Select `viewerState.lists` into Chat.lists in the node response. Chat.status alone does not describe all memberships.
Remove app-side insertion and list refetch after MESSAGE. Regenerate Swift and Kotlin models.
The existing generated list and node calls keep their signatures.
Missing members join the head. Existing members keep their places. Manual node reads do not insert rows into lists.

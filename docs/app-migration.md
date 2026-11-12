# App migration

Steps to move an app to the current version of the library. Do them in this order.

## 1. Update the build

1. Take the generator and the binding files of the same release (`dblayer-gen`, `DBlayerFFI.xcframework.zip`, `dblayer.swift`, `dblayer-android.zip`). See [getting started](getting-started.md#use-a-release).
2. Regenerate the Swift and Kotlin models. A generated `DBModel` has `rowRevision` (the revision of the store the row was read at) and `estimatedBytes` (the bytes the core estimates the row holds). Models from an older generator do not compile.
3. On Android, pass `uploadNotification = UploadNotification(title, text, smallIcon)` to `DBlayer.open`. On Android below API 34 the manifest needs the `FOREGROUND_SERVICE_DATA_SYNC` permission, else `open` fails with `Config`. See [files](files.md#android-background-transfer).

## 2. Open and close

- Open once at app start: `try await DB.open(...)` in Swift, suspend `DBlayer.open(context, ...)` in Kotlin. A second `open` while one is open fails. See [server sync](server-sync.md#connecting).
- `DB.close()` and `DBlayer.close()` are async. A call before the first `open` fails with `NotOpen`. After a `close`, every call fails with `Closed` and a second `close` succeeds.
- `DBlayer.database` and `DB.database` are not public. Use the generated calls and the `AccountScope`.

## 3. Session

- Call `sessionReset()` for every sign-out. It ends the work of the old session, clears the credential and the data of the account, and returns after that. See [server sync](server-sync.md#ending-a-session).
- A lost session state arrives as `DbError.Session` with code `.stateLost` in Swift and `DbException.Session` with `SessionCode.STATE_LOST` in Kotlin.
- `DB.interruptedSessionAnswer(as:)` and `DBlayer.interruptedSessionAnswer()` return the answer of a session call that a killed process left running, once.
- Make a new `AccountScope` after each sign-in and build the screens again for it.

## 4. Reads

Every read is a stream of an `AccountScope` shown through `Projected`. See [live data](live-data.md) and [value bridge](value-bridge.md).

- A stream has one reader. A second reader ends at once with `ended(.invalid)`. Open one stream for each screen.
- A snapshot carries the `edits` from the rows the reader holds to its rows. `Projected.edits` and `ListProjection.lastEdits` hold the last delivery.
- Read `phase`, `error`, `settled`, `hasMore` and `total` from the state of the projection instead of waiting on a raw stream.
- An events loop handles `ModelEventItem.ready`, `.event`, `.lost(first, last)`, `.gap` and `.ended`. A test or a screen that must hear every event after a moment waits for `ready` first.
- Give each caller of a shared load its own `beginLoad()` request and cancel it when the caller leaves.
- `Query.read()` and `Query.readRow(id)` read storage once. `Query.cachedRow(id)` reads what the core already holds.
- The state of an upload is `uploads[action]` on the record and `scope.uploadStatus(row, action)`. See [files](files.md#state).
- `progress` of an upload is a share from 0 to 1, not a percent.

## 5. Writes

- `Model.create(..., in: scope)` (`account = scope` in Kotlin) returns a receipt with the optimistic value, the revision and `confirmed()`. Every write waits in the queue while the network is away, so the app handles no offline error of a write. See [writing data](writing-data.md#writing-without-a-network).
- `OperationFailure` takes `kind`, `sessionCode`, `requestSize`, `requestLimit`, `message`, `extensions` and `responseLimit`, in this order.
- `FileInfo.size` is `UInt64` in Swift and `ULong` in Kotlin.
- `RetryProgress` is read by pull: `RetryProgress.changes` in Swift and `RetryProgress.flow` in Kotlin. See [server sync](server-sync.md#retries).
- `upload` on a row that does not exist is accepted, and the answer of the final mutation writes the row. See [files](files.md#upload-to-a-row-that-does-not-exist).
- Send a file with `upload: UploadCommand(action:, request:)` on a send, or call `scope.upload(row, action:, request)` for an existing record. The `.dbl` files declare the upload steps once per project in `uploads do ... end`.
- Start a file with `scope.stage(Model, action, request)` when the user picks it and the row does not exist yet, then call `staged.commit(values, variables)` or `staged.discard()`. Remove the own upload of the app that waits for the form. See [files](files.md#upload-before-the-final-mutation).
- Pass `prepare` settings in the source to recompress a video or an image, and remove the recompression of the app before an upload. See [files](files.md#source-preparation).

## 6. Models in `.dbl`

| Need | Declaration | Where |
| --- | --- | --- |
| An event carries the row | `payload: "chat"` on the node scope of a model, `server_revision :serverRevision` on the model | [models](models.md#node-reads) |
| A media library with a marker | `marker "chatMediaState", from: "node.mediaLibrary", fields: [...], sequence: :latestSequence` and `tail "chatMedia", after: :afterSequence` in the scope | [models](models.md#media-library-marker-and-tail) |
| Preload by another scope | `preload vibeId: { from: "Compass.rows" }, limit: 40` | [models](models.md) |
| A complete member list | `scope :chat_members, members_complete: true` | [models](models.md#scopes) |
| A create takes values from other models | repeated `local :field, from: { of: Model, field: :source }` | [models](models.md) |
| A subscription filter | `matches: { kind: ["ACTIVE"] }` or `matches: { kind: { not: ["SCREENSHOT"] } }` | [server sync](server-sync.md#subscriptions) |
| Membership from the node response | `membership list: { contains: :lists }` and `insert_on: [:event, :response, :node]` | [models](models.md#scopes) |
| An action that changes other rows | `removes_receiver`, `copies_receiver_to`, `joins_scope`, `leaves_scope` | [models](models.md#effects-of-an-operation-before-the-answer) |
| A server `Int64` | `scalar :Int64, as: :long` | [models](models.md#custom-scalars) |

## 7. Tests

- A test opens its database and closes it. There is no reset between tests. See [testing](testing.md).
- `MockTransport` needs one scripted reply per PUT (`enqueuePut`). A PUT without one fails like a call without a reply.
- `DB.openTesting` and `DBlayer.openTesting` take `observers`. The test hooks live in `DBlayerTesting` (Swift) and in the `testFixtures` of the Maven artifact (Kotlin).

## 8. After 6.30.0

- The network of the phone is a `NetworkPath`. `NetworkStatus.changes` (Swift) and `NetworkStatus.flow` (Kotlin) deliver it instead of a `Bool`: `online` is the old flag, `kind` is `wifi`, `cellular`, `other` or `none`, and `network` grows when the default network changes. Remove the own network monitor of the app. See [sync state](sync-state.md#network-status).

```swift
// before: for await online in network.changes { ... }
for await path in network.changes { print(path.online, path.kind, path.network) }
```

```kotlin
// before: network.flow.collect { online -> ... }
network.flow.collect { path -> println("${path.online} ${path.kind} ${path.network}") }
```

- A test builds a failed row with `operationFailure`, `syncInfo` and `uploadInfo` from the test products, and reads no JSON for it. Remove the JSON fixtures of failures. See [testing](testing.md#failure-values).

```swift
let failure = operationFailure(kind: "Invalid", reason: "the cell is empty", message: "refused")
let sync = syncInfo(state: .failed, error: failure)
```

```kotlin
val failure = operationFailure("Invalid", "the cell is empty", "refused")
val sync = syncInfo(SyncState.Failed, failure)
```

- The library reads the source file only during the call that brought its path: `send`, `upload`, the attaching of a deferred source and `attach`. Delete the file when the call returns, and remove the directory of kept sources and its cleanup. A process that dies before the call returns leaves no upload. See [files](files.md).

```swift
let receipt = try await DB.account().send(Moment.self, values: values, upload: UploadCommand(action: "upload_clip", request: request))
try FileManager.default.removeItem(at: url)
```

```kotlin
val receipt = AccountScope.current().send(Moment, values, upload = UploadCommand("upload_clip", request))
file.delete()
```

- `registerOperations`, and an `open` that passes a read or a mutation, fail with `Invalid` when the definitions leave out an operation the library sends by itself. The error names every missing operation. Pass the generated `GraphQLOperations.all`. See [server sync](server-sync.md#connecting).
- The parents of a changed row are touched once per batch, and the queue order of restored operations is kept. The app changes nothing for these two fixes.

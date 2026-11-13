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
- `OperationFailure` takes `kind`, `sessionCode`, `requestSize`, `requestLimit`, `message`, `extensions`, `responseLimit` and `reason`, in this order.
- `FileInfo.size` is `UInt64` in Swift and `ULong` in Kotlin.
- `RetryProgress` is read by pull: `RetryProgress.changes` in Swift and `RetryProgress.flow` in Kotlin. See [server sync](server-sync.md#retries).
- `upload` on a row that does not exist is accepted, and the answer of the final mutation writes the row. See [files](files.md#upload-to-a-row-that-does-not-exist).
- Send a file with `upload: UploadCommand(action:, request:)` on a send, or call `scope.upload(row, action:, request)` for an existing record. The `.dbl` files declare the upload steps once per project in `uploads do ... end`.
- Pass `prepare` settings in the source to recompress a video or an image, and remove the recompression of the app before an upload. See [files](files.md#source-preparation).

## 6. Models in `.dbl`

| Need                                    | Declaration                                                                                                                                               | Where                                                         |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| An event carries the row                | `payload: "chat"` on the node scope of a model, `server_revision :serverRevision` on the model                                                            | [models](models.md#node-reads)                                |
| A media library with a marker           | `marker "chatMediaState", from: "node.mediaLibrary", fields: [...], sequence: :latestSequence` and `tail "chatMedia", after: :afterSequence` in the scope | [models](models.md#media-library-marker-and-tail)             |
| Preload by another scope                | `preload vibeId: { from: "Compass.rows" }, limit: 40`                                                                                                     | [models](models.md)                                           |
| A complete member list                  | `scope :chat_members, members_complete: true`                                                                                                             | [models](models.md#scopes)                                    |
| A create takes values from other models | repeated `local :field, from: { of: Model, field: :source }`                                                                                              | [models](models.md)                                           |
| A subscription filter                   | `matches: { kind: ["ACTIVE"] }` or `matches: { kind: { not: ["SCREENSHOT"] } }`                                                                           | [server sync](server-sync.md#subscriptions)                   |
| Membership from the node response       | `membership list: { contains: :lists }` and `insert_on: [:event, :response, :node]`                                                                       | [models](models.md#scopes)                                    |
| An action that changes other rows       | `removes_receiver`, `copies_receiver_to`, `joins_scope`, `leaves_scope`                                                                                   | [models](models.md#effects-of-an-operation-before-the-answer) |
| A server `Int64`                        | `scalar :Int64, as: :long`                                                                                                                                | [models](models.md#custom-scalars)                            |

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

- A file the user picks before the row exists goes up with `stage`. Call `scope.stage(Model, action, request)` when the file is chosen, `scope.uploadStatus(staged)` for its status, `staged.commit(values, variables)` when the form is published, and `staged.discard()` when it closes without publishing. Remove the own upload of the app that waits for the form, and the own staging files. See [files](files.md#upload-before-the-final-mutation).

```swift
let staged = try await scope.stage(Moment.self, action: "upload_clip", UploadRequest(path: path, prepare: prepare))
let receipt = try await staged.commit(values: ["text": .string("hello")], variables: [:])
let removed = try await staged.discard()
```

```kotlin
val staged = scope.stage(Moment, "upload_clip", UploadRequest(path, prepare = prepare))
val receipt = staged.commit(buildJsonObject { put("text", "hello") })
val removed = staged.discard()
```

- An `open` that passes a read or a mutation fails with `Invalid` when the definitions leave out an operation the library sends by itself. The error names every missing operation. Pass the generated `GraphQLOperations.all`. See [server sync](server-sync.md#connecting).
- The parents of a changed row are touched once per batch, the queue order of restored operations is kept, and a failed operation with a lost answer stops no other write. The app changes nothing for these fixes.

## 9. After 6.31.0

- `localUrl` of an upload is set from the first frame of its row. During the call that brought the file it is the file of the caller, after the copy is durable it is the copy of the library, and it is `nil` once the transfer is removed. Show the preview only from `localUrl`, and remove the own copy of the picked file that the app keeps for the preview. See [files](files.md#upload-before-the-final-mutation).

```swift
for await status in scope.uploadStatus(staged) { preview.show(status.localUrl) }
```

```kotlin
scope.uploadStatus(staged).uploadStatus.collect { status -> preview.show(status.localUrl) }
```

- A saved write or upload whose operation the generated set no longer declares ends `failed` with a `refused` error: "the operation <model>.<action> is not declared". Handle it as any refused write: `discard` it, or `retry` it after the update that declares the operation. See [sync state](sync-state.md).
- A call or a write that meets an unreadable secret waits and goes when the secret can be read again. Remove the retry of the app on a `Storage` error that only waited for the keystore.
- A stored upload step fails with the state of the session when the session cannot send it (`StateLost`, or the reason of a blocked session) instead of staying queued. A `failed` row of this kind is handled by `retry` or `discard`.
- A refused write of the session record to the disk, or of the erase of the account data at the end of a session, ends at the next online or foreground event: calls get `Storage` until then, and the end of the session goes on after the write went through. The app adds no timer.
- The file probe of a custom `Preparer` reports the turn of a video as 0, 90, 180 or 270, an EXIF orientation as 1 to 8 (any other value is 1), and the location of a video as the key `location`. Remove the own conversion of negative turns and of the platform spelling of the location key.

## 10. After 6.31.1

- The upload status `cancelled` and the wait reason `offline` are removed: the core never set them. Remove their branch from the `switch` or `when`, and read "no network" from `NetworkStatus`.

```swift
switch upload.status {
case .waiting, .preparing, .queued, .uploading, .sending, .done, .failed: break
}
```

```kotlin
when (upload.status) {
    UploadStatus.Waiting, UploadStatus.Preparing, UploadStatus.Queued, UploadStatus.Uploading, UploadStatus.Sending, UploadStatus.Done, UploadStatus.Failed -> Unit
}
```

- `discard()` of a staged upload, `discard(action)` and `cancelUpload(action)` work in any stage of the transfer. Remove a repeat of the call after `InFlight`: a second call gets the same outcome as the first.

```swift
let removed = try await staged.discard()
```

```kotlin
val removed = staged.discard()
```

## 11. After 6.32.0

- An event `changed_by ..., via: :node` rereads the row that a list, a counter or a local read shows. Remove a poll, a separate reader of the row and a `via: :observed` that you added only to refresh the tiles of a list.

```swift
// no change: the list shows the new value of the row after the event
```

```kotlin
// no change: the list shows the new value of the row after the event
```

- `commit` of a staged upload during a step of the library waits for the step instead of throwing `InFlight`, and `commit` of an upload that is being removed throws `NotFound`. Remove a repeat of `commit` after `InFlight`, and handle `NotFound` as the end of the upload.

```swift
let receipt = try await staged.commit(values, variables)
```

```kotlin
val receipt = staged.commit(values, variables)
```

- `discard` right after `commit` returns `true` when the upload is removed with its row and `false` when the final mutation already left and the row stays. It does not throw `InFlight`: remove a repeat after it.
- A batch upload with one key twice in the call fails with `Invalid` that names the key, not with `InFlight`. Fix the call.
- A video with `mirror` set goes up mirrored on iOS also when the source needs no re-encode. The app changes nothing.
- A custom `Preparer` that reports a video rotation outside 0, 90, 180 or 270 gets `prepare_failed` with the reason `failed`, and the copy of the source stays. Report the turn as one of the four values.
- The first open after the update rebuilds the cache of the server models because the cache format changed. The app sees the server data load again once. Local models, queued operations, the session and uploads stay. The app changes nothing.
- A call or a load made before the first network value of the system waits for it instead of failing with `Offline`. A call that waits 30 seconds for that value fails with a network error, not with `Offline`. Remove a retry on `Offline` at start. The app changes nothing else.
- `discard` and `cancelUpload` of a failed upload that is being replaced no longer throw `InFlight`: remove a repeat after it. The command applies to the upload the key keeps: the new one when the replacement is confirmed, the failed one when it is refused.

## 12. After 6.32.1

- The PUT in a background session on iOS is removed with its entry point of the library. The PUT of a file runs in the app, in parts. Remove the call of the library that you added to `application(_:handleEventsForBackgroundURLSession:completionHandler:)`: the build fails while it stays. Nothing replaces it, and the app adds no `beginBackgroundTask` of its own around uploads. Android changes nothing. See [upload with the app in the background](files.md#upload-with-the-app-in-the-background).

```swift
// iOS: remove the call of the library in the background session callback, nothing replaces it.
```

```kotlin
// Android: no change.
```

- An event `changed_by ..., via: :node` rereads a row that a read of another model shows through `ref:` or a relation, and the child of a shown parent with a `sets:` field. The parent recomputes the field. A model with several `node: :one` scopes rereads the row with the scope that `via` names. Remove a poll or a reader of the row that you added for it.

```swift
// no change: the read shows the new value of the row after the event
```

```kotlin
// no change: the read shows the new value of the row after the event
```

- The first open after the update rebuilds the server cache. Queued operations, the session and uploads stay.

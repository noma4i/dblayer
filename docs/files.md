# Files

A `:file` attribute stores binary data such as images, audio, video and documents.

```ruby
attribute :media, :file, null: true
```

```swift
let info = try await moment.attach(.media, data: bytes, name: "a.png", mime: "image/png")   // FileInfo
let data = try await moment.read(.media)
let url  = try await moment.url(.media)     // local file URL for a player or viewer
try await moment.detach(.media)
```

```kotlin
val info = moment.attach(Moment.media, data = bytes, name = "a.png", mime = "image/png")   // FileInfo
val data = moment.read(Moment.media)
val uri  = moment.url(Moment.media)         // java.net.URI of a local file for a player or viewer
moment.detach(Moment.media)
```

| Call | Result |
| ---- | ------ |
| `attach` | stores the file and replaces the previous one for that row and field |
| `file` | `FileInfo`: id, name, mime type, size, sha256 |
| `read` | the bytes |
| `url` | a local file `URL` (Swift) or `java.net.URI` (Kotlin), created on demand |
| `detach` | removes the file |

You do not decide where bytes live. Small files are kept inside the database, large ones next to it, and the library may move them. The API has no paths to storage and no size thresholds.

## Rules

- A `:file` attribute reads as `FileInfo?` on the record. `create` and `update` take no file parameter, `attach` is the only way to write one.
- Every file call is asynchronous (`async` in Swift, `suspend` in Kotlin) and does its disk work off the caller thread, so it is safe to call from the main thread.
- The row holds a description of the file; observers see a new row revision when the file changes.
- `attach` replaces the old file and updates the row in one commit. If the commit fails, no file remains.
- Destroying a row removes its files. Maintenance removes files left by crashes.
- File operations report cleanup errors. A committed row change remains committed when subsequent disk cleanup fails.
- A temporary row keeps its file when the row gets its server id.
- A `:file` attribute cannot be indexed.

## Uploads

### Upload before choosing an action

Declare the upload once and use a Blob parameter in its consumer operation:

```ruby
blob_upload :video_intro, create: "uploadCreate"

operation :set_video do
  mutation "ProfileSetVideo"
  parameter :video, :blob
  optimistic false
  response from: "profileSetVideo.profile"
end
```

The generator checks the create mutation, its file variables, and the consumer's signedId type and binding.
The generated BlobUpload name and typed operation are available on both platforms.
Existing UploadOperations multipart declarations apply to these transfers too.

```swift
let blob = try await DB.upload(.videoIntro, request: UploadRequest(path: url.path))
let restored = try await DB.blob(blob.id)
let signedId = try await blob.waitUntilUploaded()
try await Profile.setVideo(video: blob)
```

```kotlin
val blob = DBlayer.upload(BlobUpload.VideoIntro, UploadRequest(file.path))
val restored = DBlayer.blob(blob.id)
val signedId = blob.waitUntilUploaded()
Profile.setVideo(video = blob)
```

Upload returns a durable reference after the source is accepted, before PUT finishes.
The snapshot contains state, progress, error and nullable signedId.
Retry a failed transfer with `try await blob.retry()` on Swift or `blob.retry()` on Kotlin.
Discard an unused Blob with `try await blob.discard()` on Swift or `blob.discard()` on Kotlin.
Swift reads `blob.snapshot`. Kotlin reads `blob.snapshot.value`.
An operation can accept an uploading Blob. The core waits and supplies signedId itself.
The snapshot states are uploading, uploaded, failed, expired, discarded and consumed.
Only one operation can pin a Blob. A second pin fails explicitly.
An unknown mutation outcome keeps the pin. Discard rejects a pinned Blob.
Confirmed success removes local bytes and the transfer operation.
A final refusal releases the pin and preserves the uploaded Blob for another choice until expiry.
The upload and confirmed multipart parts survive reopening under the same id.
Leaving the editor does not cancel the transfer.

An unused uploaded Blob expires seven days after createBlob, not after PUT or multipart completion.
Multipart creation is recorded at init. Failed transfers retain UploadConfig.failed_ttl, measured from failure.
Expiry is checked at startup, network return and use acceptance. No expiry timer runs.
Expired Blobs fail before the consumer mutation is sent. Local bytes are removed without deleting the server blob.
Before every multipart part is confirmed, failure can abort the multipart.
After confirmation, retry repeats complete and startup cleanup preserves the confirmed parts.
Explicit discard and failed expiry still abort an unfinished multipart, including confirmed parts awaiting complete.
On Android the foreground task and the user-initiated job carry the transfer. iOS uploads run while the app runs.
An unused uploaded Blob without readers sends no periodic requests and schedules no wakeups.

An upload sends a local file to storage and then runs one content mutation with the stored blob. It is an operation of the library: it lives in the same queue as other pending operations, survives restarts, and shows its state on the record.

After the upload call returns, the library owns a copy of the source. The application can delete its temporary file.
The upload reports the copied file size as `totalBytes` before transfer starts, including while its record awaits create confirmation.
Source copying and SHA-256 calculation run before the SQLite write, on a blocking task, in 1 MiB chunks.
For file databases, sources larger than 256 KiB stay beside the database. Smaller sources and in-memory databases use SQLite bytes.
Resumed transfers read the owned disk file directly. A rejected admission removes its prepared files.
Uploads require server models. Local and memory models reject admission without creating records or files.
A database reset rejects upload admission queued before that reset.
Transfer metadata reads, MD5 calculation and byte-range reads run in the blocking pool. Cancellation returns without waiting for queued file work.

Waiting files are ordered by operation sequence and item position. A transfer reserves its file slot before its task starts. The slot covers preparation and transfer retries, and is released before the final content mutation. Abort and content mutation tasks do not consume file slots.

Synchronization reads outgoing operation metadata once at startup. Commits update affected entries, and sending reads inputs for the selected operation. Upload progress does not reload the outgoing queue.
Inline bytes are written to the cache and cleanup after a finished or cancelled run is removed in the blocking pool. `wait_uploads` waits for both. A run keeps the executor handle it started on, so dropping it on any thread still queues the cleanup there. File removal after a confirmed destroy or a server destroy event also runs in the blocking pool. Queued file work is never cancelled when the `Sync` is dropped.
Discarding an upload (or any removal of its operation) that holds a multipart `uploadId` records the abort in `upload_orphan` in the same commit, with or without confirmed parts, and never sends `complete`. The abort is sent when the app is online: at the first flush after start and when connectivity returns, and the row is removed only after the server answers. A failed upload older than 7 days (`UploadConfig.failed_ttl`, measured from the failure) is removed by the same rollback path at those two events. No timer runs for this.
Running checksum and range reads check cancellation between 1 MiB chunks.
Each file has an `op_item` row. Multipart parts and saved requests include the item position.
The database format changes with these tables. Opening an incompatible database clears it through the existing recreation policy.

An upload belongs to a declared upload action of the model (`operation :upload_clip, upload: true`), which names the content mutation. A record holds one upload per action and several uploads of different actions at the same time. Starting the same action again while it is open is refused. The transfer steps are operations you registered by name. Declare them once in `DB.open` (Swift) or `DBlayer.open` (Kotlin) with a transport or endpoint, and register the content mutation next to them; every upload and batch uses that declaration. A file takes `createBlob` below the multipart threshold and the multipart steps above it. `rootPath` names the response field a step answers under, and nil keeps the first field of its document. Without a declaration an upload is refused before any file is read or request sent, and `open` fails when a declared name is not registered.

```swift
try await DB.open(endpoint: endpoint, modelsJSON: models,
  operations: [MomentCreateFile.self, CreateBlob.self, InitMultipart.self, PresignParts.self, CompleteMultipart.self, AbortMultipart.self],
  uploads: UploadOperations(
    createBlob: UploadOperation("CreateBlob", rootPath: "uploadCreate.directUpload"),
    multipart: .init(initiate: UploadOperation("InitMultipart"), presign: UploadOperation("PresignParts"), complete: UploadOperation("CompleteMultipart"), abort: UploadOperation("AbortMultipart"))))
let request = UploadRequest(path: url.path, variables: ["text": .string("hello")])
try await DB.account().upload(moment, action: "upload_clip", request)
moment.uploads["upload_clip"]?.status       // .uploading
moment.uploads["upload_clip"]?.percent      // 42.0
try await moment.cancelUpload(action: "upload_clip")
```

```kotlin
DBlayer.open(context, endpoint = endpoint, modelsJson = models,
    operations = listOf(MomentCreateFile, CreateBlob, InitMultipart, PresignParts, CompleteMultipart, AbortMultipart),
    uploads = UploadOperations(
        createBlob = UploadOperation("CreateBlob", rootPath = "uploadCreate.directUpload"),
        multipart = UploadOperations.Multipart(UploadOperation("InitMultipart"), UploadOperation("PresignParts"), UploadOperation("CompleteMultipart"), UploadOperation("AbortMultipart"))),
    uploadNotification = notification)
val request = UploadRequest(path = file.path, variables = variables)
AccountScope.current().upload(moment, "upload_clip", request, UploadClipAnswer.serializer())
moment.uploads["upload_clip"]?.status       // Uploading
moment.uploads["upload_clip"]?.percent      // 42.0
moment.cancelUpload("upload_clip")
```

### Android background transfer

On iOS uploads go through the outbox of the core while the app runs.

On Android the app gives the text and the small icon of the notification to `DBlayer.open` as `uploadNotification = UploadNotification(title, text, icon)`.

API 34 and later run one user-initiated job with a network requirement. API 33 runs a sticky `dataSync` foreground task that starts its notification before `open` finishes. The library does not use WorkManager. A job or foreground task never repeats a transfer: retries follow the upload `Backoff` of the core. When the system refuses the start, the reason appears in the `error` of the accepted upload and nothing else changes. Upload requests (the file `PUT`, the steps and the content mutation) use the network of the job, all other requests use the default route. A restart or a force stop continues at the next `open`.

`retry` and `discard` take an `action`. Pass `null` for the whole record, or a declared action. `retry` returns a new receipt. Preparing the request and calling the library run off the caller thread.

### Create with an upload

A record that does not exist yet can be created together with its upload. The record appears at once with a temporary id and its `uploads[action]` state. No create operation is queued: after the file is stored, the content mutation creates the record on the server and its id replaces the temporary one. A failed upload leaves the record failed, `retry(action)` sends again and `discard(action)` removes it.

Declare a named operation for each content mutation, with `upload MutationName` and its record `root`. Uploads use the same response normalization as other actions. Create and update uploads can have different nested roots on the same model.

```swift
let request = UploadRequest(path: url.path, variables: ["text": .string("hello")])
let receipt = try await DB.account().send(Moment.self, values: ["text": .string("hello")], upload: UploadCommand(action: "upload_clip", request: request))
receipt.optimisticValue?.uploads["upload_clip"]?.percent
let outcome = try await receipt.outcome()
```

```kotlin
val request = UploadRequest(path = file.path, variables = variables)
val receipt = AccountScope.current().send(Moment, values, upload = UploadCommand("upload_clip", request))
receipt.optimisticValue?.uploads?.get("upload_clip")?.percent
val outcome = receipt.outcome()
```

Each generated upload method returns a receipt. Its outcome includes the declared response fields as JSON after the transfer and final mutation.

### Create several records with uploads

A batch creates all temporary records and owns their source files in one commit.
Declare its size limits with `validates :items, length:`, the item variable mapping, the `match_by` attribute, and the single-record upload operation.
The application supplies a nonempty, unique correlation key in each record's values.
The library preserves these keys and adds each confirmed `signedId` to the ordered mutation items.
The response replaces temporary records by correlation key, including responses received before the mutation answer.

An individual file failure leaves other transfers running and delays the final mutation until all remaining files finish.
Retrying a failed file preserves completed transfers.
Discarding a record removes it from the operation and recalculates the remaining count.
Below the declared minimum, the remaining records use the declared single-record mutation.
An optional `renumber:` option of `batch` rewrites the remaining records' position and size in that same commit after `discard(action)` or `cancelUpload(action)`.
The same commit updates saved mutation variables declared by `batch input:`, including values used after reopening the cache.
Positions follow the original item order, starting at the required `start: 0` or `start: 1`.
Below the declared minimum, these fields stay unchanged and the application decides how to represent a single record.
Without `renumber:`, the library preserves these attributes.

During the final mutation, changing membership returns `InFlight`.
After `deliveryUnknown`, retrying any member repeats the complete saved request with the same operation key.
Discarding any member then rolls back the entire uncertain operation.

```ruby
operation :create_file, upload: true do
  mutation "CreateAsset"
  match_by "clientKey"
  optimistic false
  response from: "createAsset.asset"
end
operation :create_files, upload: true do
  mutation "CreateAssets"
  match_by "clientKey"
  optimistic false
  batch items: "items", single: :create_file, input: { clientKey: :clientKey }, renumber: { position: "position", size: "size", start: 0 }
  validates :items, length: { minimum: 2, maximum: 10 }
  response from: "createAssets.assets"
end
```

`batch input:` maps input object fields to model attributes.
`renumber:` names distinct integer attributes and accepts dotted paths inside declared objects, such as `position: "object.position", size: "object.size"`.
Nested writes preserve other object values and use canonical stored attribute names, including names declared with `from`.
The rewrite changes local row values without queuing another mutation, and the correlated server response replaces them as usual.
Only an unsent final mutation permits membership changes, and `deliveryUnknown` never renumbers records.
The limits of `validates :items, length:` must fit within 2 through 10, and `minimum` may not exceed `maximum`. Only an upload operation declares `batch`.
The GraphQL mutation accepts an input object list containing the correlation key and `signedId`.
Its response includes every created record with the same correlation key, in any order.

```swift
let records = try await DB.account().uploadBatch(
  Asset.self,
  items: files.map { UploadItem(values: $0.values, path: $0.path) },
  request: UploadBatchRequest(operation: "create_files")
)
try await records[1].discard(action: "create_files")
```

```kotlin
val records = AccountScope.current().uploadBatch(
    Asset,
    items = files.map { UploadItem(values = it.values, path = it.path) },
    request = UploadBatchRequest(operation = "create_files"),
)
records[1].discard("create_files")
```

Each returned record has a temporary id, local file preview, `Sending` state, and independent upload progress.
Use item `variables` for additional input object fields and request `variables` for shared mutation arguments.
`cancelUpload(action)` removes only the selected transfer and keeps its local record.
Offline cancellation and restart use the same stored file and multipart state as a single upload.

### Typed variables of the content mutation

The library sets `signedId` itself. Other mutation variables go in `variables`. Declare schema input objects with the named operation's `parameter` lines. The generator checks mutation variables and writes a typed builder named after the operation, such as `createFileVariables`. Do not declare `signedId`.

```ruby
operation :create_file, upload: true do
  mutation "MomentCreateFile"
  optimistic false
  parameter :widgets, [MomentWidgetInput], null: true
  response from: "momentCreate.moment"
end
```

```swift
let widgets = [MomentWidgetInput(kind: .emoji, props: .emoji(MomentEmojiInput(code: "fire")))]
let request = UploadRequest(path: url.path, variables: try Moment.createFileVariables(widgets: widgets))
let receipt = try await DB.account().send(Moment.self, values: ["text": .string("hello")], upload: UploadCommand(action: "create_file", request: request))
```

```kotlin
val widgets = listOf(MomentWidgetInput(kind = WidgetKind.Emoji, props = MomentWidgetProps.Emoji(MomentEmojiInput(code = "fire"))))
val request = UploadRequest(path = file.path, variables = Moment.createFileVariables(widgets = widgets))
val receipt = AccountScope.current().send(Moment, values, upload = UploadCommand("create_file", request))
```

### Scenarios

Absent headers are valid. Malformed header JSON or non-string header names and values reject the answer before PUT.

| Scenario | Steps |
| -------- | ----- |
| single (smaller than the threshold) | `create` returns url, headers, `signedId`, `blobId`, then one PUT of the file. The answer may sit at a dotted path (`uploadCreate.directUpload`) and `headers` may be an object, a list or a JSON string |
| multipart (threshold or larger) | `init` receives filename, contentType, byteSize and checksum, and returns `key` and `uploadId`. `presign` returns part URLs. PUT confirms parts. `complete` exchanges their ETags for `signedId` and `blobId` |
| content mutation | runs once with the original variables plus `signedId` (and `blobId` when the steps name a variable for it, the typed `UploadRequest` sends only `signedId`) |

The file is read in ranges: a single upload in one range up to the 10 MB threshold, a multipart upload in parts of 5 MB. The checksum is base64 MD5, the MIME type comes from the file extension unless you pass one.

| Setting | Default |
| ------- | ------- |
| multipart threshold | 10 MB |
| part size | 5 MB |
| parallel parts | 3 (1 means sequential) |
| files at a time | 1, the others wait as `queued` |
| retry backoff | [Upload retry policy](server-sync.md#retries), with a saved budget for each step and part |
| PUT timeout / GraphQL step timeout | 120 s / 45 s |
| presigned url lifetime | 50 minutes |

The file limit counts transfers across operations and their items. A limit of 2 allows 2 files to transfer together.
The final content mutation does not hold a file permit.

### State

`record.uploads` is empty when nothing is running, otherwise it holds one entry per declared upload action. It changes through the same live reads as `record.sync`, so observers get a new row for every state or progress change.

| Field | Meaning |
| ----- | ------- |
| `status` | `waiting`, `preparing`, `queued`, `uploading`, `sending`, `failed`, `deliveryUnknown` |
| `waitReason` | why a `waiting` upload does not run yet: `createUnconfirmed` (the record is still local, its `create` has no answer) or `offline` (the step waits for the network and sends no request, `record.sync.state` stays `pending`). The upload leaves `offline` when the network returns. `nil` in every other status |
| `stage` | `prepare`, `create`, `put`, `init`, `presign`, `part`, `complete`, `abort`, `mutation` |
| `sentBytes`, `totalBytes`, `fraction` | for multipart the sum over all parts divided by the file size |
| `attempts` | sum of retries for unfinished steps, preserved across restarts |
| `error` | kind and message, `isRetryable` is false for a rejection |

An upload of a local record starts as `waiting` with `totalBytes` set from the file size and `sentBytes` and `fraction` at zero. When the `create` is confirmed the real id replaces the local id in the upload variables and the status moves on through `preparing` or `queued` to `uploading`, `sending` and the end. If the `create` is refused the upload is discarded with the record. Every change of status, stage, bytes or reason reaches observers as a new row.

A finished upload closes its operation and removes its entry from `uploads`. The record becomes `synced` when no other operation remains open. Progress ticks are limited to one per 100 ms and never go down inside a phase. A restarted phase starts again from the bytes of its finished parts.

### Rules

- Transport errors inside the upload steps retry with backoff. Offline waits for `setOnline(true)` and spends no attempts.
- Each step and multipart part stores its retry count and next retry time. Restarting preserves both. A successful step removes its retry record.
- An explicit `retry(action)` after failure clears saved retry counts and times, giving unfinished steps a fresh budget.
- A PUT succeeds only with an HTTP status from 200 through 299. Other statuses follow the transfer error rules before the content mutation can run.
- A `403` renews the single-upload url or affected part url after backoff. The renewal uses that PUT step's retry budget. Urls older than the presign lifetime are requested again for unfinished parts.
- The final content mutation uses the [outbox retry policy](server-sync.md#retries), including its saved budget and encrypted request identity.
- Retrying the content mutation preserves confirmed file transfers.
- The grant, including `signedId`, `blobId`, URL and headers, is stored before PUT. A separate flag records confirmed transfer.
- A restarted unfinished PUT uses its saved grant. `retry(action)` after a failed or unknown content mutation skips confirmed transfers.
- A multipart failure calls `abort` only while parts remain unconfirmed. After all parts are confirmed, a failed `complete` preserves their ETags and upload id.
- `retry(action)` and `discard(action)` on the record work as for any pending operation. `cancelUpload` stops the transfer, aborts a multipart and removes the operation.
- Offline cancellation returns without a network call and keeps the multipart upload id. After `setOnline(true)`, the saved cancellation aborts the multipart and removes the operation.
- After the content mutation starts, cancelling a single upload returns `false` immediately. Cancelling a batch member returns `InFlight`.
- One record has one open upload per declared action.

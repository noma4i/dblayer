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

### Deferred source

Accept a request before its source exists. The row is visible with `waiting` and `source_pending`. No file is read or sent before `supplySource`. Supply `values` for declared row attributes and `variables` for the final mutation. If preparation fails, call `failSource`. Retry a failed source with a new `UploadSource`.

```swift
let request = UploadRequest.pending()
let receipt = try await DB.account().send(Document.self, values: values, upload: UploadCommand(action: "upload", request: request))
if let record = receipt.optimisticValue {
  try await record.supplySource(action: "upload", source: UploadSource(path: path, values: values))
}
```

```kotlin
val request = UploadRequest.pending()
val receipt = AccountScope.current().send(Document, values, upload = UploadCommand("upload", request))
receipt.optimisticValue?.supplySource("upload", UploadSource(path, values = values))
```

A restart before supply marks the transfer `missing_file`. Cancellation or account reset removes its row. A pending batch waits for all remaining sources before its final mutation.

### Uploads of a record

An upload belongs to a declared upload action of the model. A standalone blob upload without a record does not exist: the upload creates or updates a record, and `uploads[action]` on that record shows its state, progress and error.

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

Upload progress does not reload the outgoing queue. Cleanup of inline bytes after a finished or cancelled run, and file removal after a confirmed destroy or a server destroy event, run off the caller thread.
Discarding an upload (or any removal of its operation) that holds a multipart `uploadId` records the abort in the same commit, with or without confirmed parts, and never sends `complete`. The abort is sent when the app is online: at the first send after start and when the network returns, and the row is removed only after the server answers. A failed upload older than 7 days, counted from its failure, is removed by the same rollback at those two events, and one of exactly 7 days stays. No timer runs for this.
Running checksum and range reads check cancellation between 1 MiB chunks.
Each file has a `sync_upload` row with a stable transfer id, not a position in the batch.
The database format changes with these tables. Opening a database of another format keeps queued uploads when the layout of their tables is unchanged and clears them otherwise.

An upload belongs to a declared upload action of the model (`operation :upload_clip, upload: true`), which names the content mutation. A record holds one upload per action and several uploads of different actions at the same time. Starting the same action again while it is open is refused. The transfer steps are operations you registered by name. Declare them once in `DB.open` (Swift) or `DBlayer.open` (Kotlin) with a transport or endpoint, and register the content mutation next to them; every upload and batch uses that declaration. A file takes `createBlob` below the multipart threshold and the multipart steps above it. `rootPath` names the response field a step answers under, and nil keeps the first field of its document. Without a declaration an upload is refused before any file is read or request sent, and `open` fails when a declared name is not registered.

The steps of the upload are declared once in the `.dbl` files and travel with the models:

```ruby
uploads do
  create_blob :CreateBlob, root: "uploadCreate.directUpload"
  multipart initiate: :InitMultipart, presign: :PresignParts, complete: :CompleteMultipart, abort: :AbortMultipart
end
```

A project with an `upload: true` operation declares the block exactly once. A second block, an empty block, a block without an upload operation and an upload operation without the block fail the generator with the file and line. `open` registers the operations of the steps from `operations`.

```swift
try await DB.open(endpoint: endpoint, modelsJSON: models,
  operations: [MomentCreateFile.self, CreateBlob.self, InitMultipart.self, PresignParts.self, CompleteMultipart.self, AbortMultipart.self])
let request = UploadRequest(path: url.path, variables: ["text": .string("hello")])
try await DB.account().upload(moment, action: "upload_clip", request)
moment.uploads["upload_clip"]?.status       // .uploading
moment.uploads["upload_clip"]?.progress      // 42.0
try await moment.cancelUpload(action: "upload_clip")
```

```kotlin
DBlayer.open(context, endpoint = endpoint, modelsJson = models,
    operations = listOf(MomentCreateFile, CreateBlob, InitMultipart, PresignParts, CompleteMultipart, AbortMultipart),
    uploadNotification = notification)
val request = UploadRequest(path = file.path, variables = variables)
AccountScope.current().upload(moment, "upload_clip", request, Moment)
moment.uploads["upload_clip"]?.status       // Uploading
moment.uploads["upload_clip"]?.progress      // 42.0
moment.cancelUpload("upload_clip")
```

### Android background transfer

On iOS a transfer is a plain `URLSession` PUT while the app runs. The outbox keeps an upload that the app stopped and sends it after the next start.

On Android the app gives the text and the small icon of the notification to `DBlayer.open` as `uploadNotification = UploadNotification(title, text, icon)`.

API 34 and later run one user-initiated job with a network requirement. Below API 34 the library runs a sticky `dataSync` foreground task that starts its notification before `open` finishes: `open` fails with `Config` when the manifest of the app lacks `android.permission.FOREGROUND_SERVICE_DATA_SYNC`. The library does not use WorkManager. A job or foreground task never repeats a transfer: retries follow the upload `Backoff` of the core. When the system refuses the start, the reason appears in the `error` (kind `refused`) of the accepted upload until the next job is asked, and nothing else changes. Upload requests (the file `PUT`, the steps and the content mutation) use the network of the job, all other requests use the default route. A restart or a force stop continues at the next `open`.

The multipart abort uses the transfer network too. A custom `Transport.put` receives a `TransferIdentity` with the operation, the item and the step. The identity of an item stays the same when the batch changes. The item key is an opaque value, not a file name.
The background job keeps the epoch of the accepted upload. A late refusal of the system applies only to that epoch.

`retry` and `discard` take an `action`. Pass `null` for the whole record, or a declared action. `retry` returns a new receipt. Preparing the request and calling the library run off the caller thread.

### Create with an upload

A record that does not exist yet can be created together with its upload. The record appears at once with a temporary id and its `uploads[action]` state. No create operation is queued: after the file is stored, the content mutation creates the record on the server and its id replaces the temporary one. A failed upload leaves the record failed, `retry(action)` sends again and `discard(action)` removes it.

Declare a named operation for each content mutation, with `upload MutationName` and its record `root`. Uploads use the same response normalization as other actions. Create and update uploads can have different nested roots on the same model.

```swift
let request = UploadRequest(path: url.path, variables: ["text": .string("hello")])
let receipt = try await DB.account().send(Moment.self, values: ["text": .string("hello")], upload: UploadCommand(action: "upload_clip", request: request))
receipt.optimisticValue?.uploads["upload_clip"]?.progress
let outcome = try await receipt.outcome()
```

```kotlin
val request = UploadRequest(path = file.path, variables = variables)
val receipt = AccountScope.current().send(Moment, values, upload = UploadCommand("upload_clip", request))
receipt.optimisticValue?.uploads?.get("upload_clip")?.progress
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
Below the declared minimum, the remaining records use the declared single-record mutation. They go one at a time as steps of the same operation, and the batch receipt reports each record.
An optional `renumber:` option of `batch` rewrites the remaining records' position and size in that same commit after `discard(action)` or `cancelUpload(action)`.
The same commit updates saved mutation variables declared by `batch input:`, including values used after reopening the cache.
Positions follow the original item order, starting at the required `start: 0` or `start: 1`.
Below the declared minimum, these fields stay unchanged and the application decides how to represent a single record.
Without `renumber:`, the library preserves these attributes.

During the final mutation, changing membership returns `InFlight`.
A send without an answer repeats the complete saved request with the same operation key, and so does retrying any member.
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
Only an unsent final mutation permits membership changes, and a repeated send never renumbers records.
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
| `status` | `waiting`, `preparing`, `queued`, `uploading`, `sending`, `done`, `failed`, `cancelled` |
| `waitReason` | why a `waiting` upload does not run yet: `create_unconfirmed` (the record is still local, its `create` has no answer) or `offline`. A step that waits for the network or for power saving to end is `queued` |
| `stage` | `prepare`, `create`, `put`, `init`, `presign`, `part`, `complete`, `abort`, `mutation` |
| `sentBytes`, `totalBytes` | for multipart the sum over all parts |
| `progress` | the sent share of the bytes in percent from 0 to 100, 0 while the total is unknown |
| `attempts` | saved attempts for the current unfinished step, preserved across restarts |
| `error` | an `OperationFailure`: `kind`, `sessionCode`, `requestSize`, `requestLimit`, `message` and `extensions` |
| `localUrl` | the file URL of the copy of the library while the transfer holds it, `nil` otherwise |
| `processing` | `processing.progress`: the progress the server reports while it works on the file after the transfer, `nil` before |

An upload of a local record starts as `waiting` with `totalBytes` set from the file size and `sentBytes` and `progress` at zero. When the `create` is confirmed the real id replaces the local id in the upload variables and the status moves on through `preparing` or `queued` to `uploading`, `sending` and the end. If the `create` is refused the upload is discarded with the record. Every change of status, stage, bytes or reason reaches observers as a new row.

A finished upload closes its operation and removes its entry from `uploads`, except that an action with a declared `processing` keeps an entry with status `done` and the `processing` progress while the server works on the file. The record becomes `synced` when no other operation remains open. Progress advances when a part is confirmed and never decreases within a phase. A restarted phase begins with its confirmed parts.

### Rules

- Transport errors retry with backoff. Offline waits for the network to return without starting another attempt.
- Each step saves its attempt count before starting a network request. A restart or network change does not refund that attempt.
- A successful part starts the next step with a fresh budget. Each step permits up to 50 attempts.
- An explicit `retry(action)` after failure saves a fresh attempt budget before restarting unfinished work.
- When an upgraded database has lost the item of a saved transfer, the error kind is `missing_file`. A retry without a file is refused. `retry(id, action, source)` gives the item a new file: `id` is the record id. The new transfer keeps the operation and the item key.
- A PUT succeeds only with an HTTP status from 200 through 299. Other statuses follow the transfer error rules before the content mutation can run.
- A `403` renews the single-upload url or affected part url after backoff. The renewal uses that PUT step's retry budget. Urls older than the presign lifetime are requested again for unfinished parts.
- The final content mutation uses the [outbox retry policy](server-sync.md#retries), including its saved budget and encrypted request identity.
- Retrying the content mutation preserves confirmed file transfers.
- The grant, including `signedId`, `blobId`, URL and headers, is stored before PUT. A separate flag records confirmed transfer.
- A restarted unfinished PUT saves another attempt before sending. `retry(action)` after a failed content mutation skips confirmed transfers.
- A multipart failure calls `abort` only while parts remain unconfirmed. After all parts are confirmed, a failed `complete` preserves their ETags and upload id.
- `retry(action)` and `discard(action)` on the record work as for any pending operation. `cancelUpload` stops the transfer, aborts a multipart and removes the operation.
- Offline cancellation returns without a network call and keeps the multipart upload id. When the network returns, the saved cancellation aborts the multipart and removes the operation.
- After the content mutation starts, cancelling a single upload returns `false` immediately. Cancelling a batch member returns `InFlight`.
- One record has one open upload per declared action.
- `upload` on a row that does not exist fails with `NotFound` for every model. A call checks its arguments in this order: `Closed` or `NoSession` first, then `Invalid` (an empty or unreadable file, an undeclared action), then `NotFound` for the row.
- `scope.uploadStatus(row, action)` streams the status of one transfer (`TransferProgress`: `state`, `progress`, `error`, `localUrl`, `processing`). Its last item is `ended` with the cause.
- `cancelUpload(action)` returns `true` when it stopped the transfer. `retry(action)` sends a failed transfer again and `discard(action)` removes it with its optimistic row.

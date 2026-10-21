# Files

A `file` field stores binary data such as images, audio, video and documents.

```ruby
field :media, type: :file, null: true
```

```swift
let info = try await moment.attach(.media, data: bytes, name: "a.png", mime: "image/png")
let data = try await moment.read(.media)
let url  = try await moment.url(.media)     // local file URL for a player or viewer
try await moment.detach(.media)
```

```kotlin
val info = moment.attach(Moment.media, data = bytes, name = "a.png", mime = "image/png")
val data = moment.read(Moment.media)
val uri  = moment.uri(Moment.media)         // local file Uri for a player or viewer
moment.detach(Moment.media)
```

| Call | Result |
| ---- | ------ |
| `attach` | stores the file and replaces the previous one for that row and field |
| `file` | description: id, name, mime type, size, sha256 |
| `read` | the bytes |
| `url` (Swift) / `uri` (Kotlin) | a local file URL, created on demand |
| `detach` | removes the file |

You do not decide where bytes live. Small files are kept inside the database, large ones next to it, and the library may move them. The API has no paths to storage and no size thresholds.

## Rules

- Every file call is asynchronous (`async` in Swift, `suspend` in Kotlin) and does its disk work off the caller thread, so it is safe to call from the main thread.
- The row holds a description of the file; observers see a new row revision when the file changes.
- `attach` replaces the old file and updates the row in one commit. If the commit fails, no file remains.
- Destroying a row removes its files. Orphans left by crashes are cleaned by maintenance.
- A temporary row keeps its file when the row gets its server id.
- A file field cannot be indexed.

## Uploads

An upload sends a local file to storage and then runs one content mutation with the stored blob. It is an operation of the library: it lives in the same queue as other pending operations, survives restarts, and shows its state on the record.

After the upload call returns, the library owns a copy of the source. The application can delete its temporary file.
Each file has an `op_item` row. Multipart parts and saved requests include the item position.
The database format changes with these tables. Opening an incompatible database clears it through the existing recreation policy.

The steps are operations you registered by name. Each upload names the ones it uses.

```swift
let steps = UploadSteps(mutation: "CreateMoment")
    .single(create: "CreateBlob")
    .multipart(init: "InitMultipart", presign: "PresignParts", complete: "CompleteMultipart", abort: "AbortMultipart")
try await sync.upload(moment, path: url.path, steps: steps, variables: ["text": "hello"])
moment.upload?.status       // .uploading
moment.upload?.fraction     // 0.42
await sync.cancelUpload(moment)
```

```kotlin
val steps = UploadSteps(mutation = "CreateMoment")
    .single(create = "CreateBlob")
    .multipart(init = "InitMultipart", presign = "PresignParts", complete = "CompleteMultipart", abort = "AbortMultipart")
sync.upload(moment, path = file.path, steps = steps, variables = mapOf("text" to "hello"))
moment.upload?.status       // Uploading
moment.upload?.fraction     // 0.42
sync.cancelUpload(moment)
```

### Create with an upload

A record that does not exist yet can be created together with its upload. The record appears at once with a temporary id and its `upload` state. No create operation is queued: after the file is stored, the content mutation creates the record on the server and its id replaces the temporary one. A failed upload leaves the record failed, `retry()` sends again and `discard()` removes it.

Declare a named operation for each content mutation, with `upload MutationName` and its record `root`. Uploads use the same response normalization as other actions. Create and update uploads can have different nested roots on the same model.

```swift
let request = UploadRequest(path: url.path, mutation: "CreateMoment", createBlob: "CreateBlob", variables: ["text": .string("hello")])
let moment = try await Moment.create(values: ["text": .string("hello")], uploading: request)
moment.upload?.fraction     // 0.42
try await moment.confirmedId()
```

```kotlin
val request = UploadRequest(path = file.path, mutation = "CreateMoment", createBlob = "CreateBlob", variables = variables)
val moment = uploadCreate("Moment", values, request, Moment)
moment.upload?.percent      // 42.0
moment.confirmedId()
```

### Create several records with uploads

A batch creates all temporary records and owns their source files in one commit.
Declare its minimum and maximum size, item variable mapping, correlation field, and single-record upload operation.
The application supplies a nonempty, unique correlation key in each record's values.
The library preserves these keys and adds each confirmed `signedId` to the ordered mutation items.
The response replaces temporary records by correlation key, including responses received before the mutation answer.

An individual file failure leaves other transfers running and delays the final mutation until all remaining files finish.
Retrying a failed file preserves completed transfers.
Discarding a record removes it from the operation and recalculates the remaining count.
Below the declared minimum, the remaining records use the declared single-record mutation.
An optional `renumber` declaration rewrites the remaining records' position and size in that same commit after `discard()` or `cancelUpload()`.
Positions follow the original item order, starting at the required `position_from: 0` or `position_from: 1`.
Below the declared minimum, these fields stay unchanged and the application decides how to represent a single record.
Without `renumber`, the library preserves these fields.

During the final mutation, changing membership returns `InFlight`.
After `deliveryUnknown`, retrying any member repeats the complete saved request with the same operation key.
Discarding any member then rolls back the entire uncertain operation.

```ruby
operation :create_file do
  upload CreateAsset
  correlate :clientKey
  root "createAsset.asset"
  local_effect :none, reason: "created with its file"
end
operation :create_files do
  upload CreateAssets
  batch min: 2, max: 10, items: :items, single: :create_file
  renumber position: :position, size: :size, position_from: 0
  item clientKey: :clientKey
  correlate :clientKey
  root "createAssets.assets"
  local_effect :none, reason: "created with their files"
end
```

`item` maps input object fields to model fields.
`renumber` names distinct integer fields and accepts dotted paths inside declared objects, such as `position: "object.position", size: "object.size"`.
Nested writes preserve other object values and use canonical stored field names, including names declared with `from`.
The rewrite changes local row values without queuing another mutation, and the correlated server response replaces them as usual.
Only an unsent final mutation permits membership changes, and `deliveryUnknown` never renumbers records.
The declared limits must fit within 2 through 10.
The GraphQL mutation accepts an input object list containing the correlation key and `signedId`.
Its response includes every created record with the same correlation key, in any order.

```swift
let records = try await Asset.uploadCreate(
  items: files.map { UploadItem(values: $0.values, path: $0.path) },
  request: UploadBatchRequest(operation: "create_files", createBlob: "CreateBlob")
)
try await records[1].discard()
```

```kotlin
val records = Asset.uploadCreate(
    items = files.map { UploadItem(values = it.values, path = it.path) },
    request = UploadBatchRequest(operation = "create_files", createBlob = "CreateBlob"),
)
records[1].discard()
```

Each returned record has a temporary id, local file preview, `Sending` state, and independent upload progress.
Use item `variables` for additional input object fields and request `variables` for shared mutation arguments.
`cancelUpload()` removes only the selected transfer and keeps its local record.
Offline cancellation and restart use the same stored file and multipart state as a single upload.

### Typed variables of the content mutation

The library sets `signedId` itself. Other mutation variables go in `variables`. Declare schema input objects in the named operation's `args`. The generator checks mutation variables and writes a typed builder named after the operation, such as `createFileVariables`. Do not declare `signedId`.

```ruby
operation :create_file do
  upload       MomentCreateFile
  local_effect :none, reason: "the row exists locally from the create with an upload"
  args         widgets: [:MomentWidgetInput]?
  root         "momentCreate.moment"
end
```

```swift
let widgets = [MomentWidgetInput(kind: .emoji, props: .emoji(MomentEmojiInput(code: "fire")))]
let request = UploadRequest(path: url.path, mutation: "MomentCreateFile", createBlob: "CreateBlob", variables: Moment.createFileVariables(widgets: widgets))
let moment = try await Moment.create(values: ["text": .string("hello")], uploading: request)
```

```kotlin
val widgets = listOf(MomentWidgetInput(kind = WidgetKind.Emoji, props = MomentWidgetProps.Emoji(MomentEmojiInput(code = "fire"))))
val request = UploadRequest(path = file.path, mutation = "MomentCreateFile", createBlob = "CreateBlob", variables = Moment.createFileVariables(widgets = widgets))
val moment = uploadCreate("Moment", values, request, Moment)
```

### Scenarios

| Scenario | Steps |
| -------- | ----- |
| single (smaller than the threshold) | `create` returns url, headers, `signedId`, `blobId`, then one PUT of the file. The answer may sit at a dotted path (`uploadCreate.directUpload`) and `headers` may be an object, a list or a JSON string |
| multipart (threshold or larger) | `init` receives `filename`, `contentType`, `byteSize` and the base64 MD5 `checksum` and returns `key` and `uploadId`, `presign` returns one url per part, parts go out by PUT, `complete` receives the part numbers with their ETags and returns `signedId` and `blobId`, `abort` runs when the upload fails |
| content mutation | runs once with the original variables plus `signedId` (and `blobId` when the steps name a variable for it, the typed `UploadRequest` sends only `signedId`) |

The file is read in ranges: a single upload in one range up to the 10 MB threshold, a multipart upload in parts of 5 MB. The checksum is base64 MD5, the MIME type comes from the file extension unless you pass one.

| Setting | Default |
| ------- | ------- |
| multipart threshold | 10 MB |
| part size | 5 MB |
| parallel parts | 3 (1 means sequential) |
| files at a time | 1, the others wait as `queued` |
| retry backoff | 1 s doubling to 15 s with jitter, independent seeds per client, 50 attempts per step |
| PUT timeout / GraphQL step timeout | 120 s / 45 s |
| presigned url lifetime | 50 minutes |

The file limit counts transfers across operations and their items. A limit of 2 allows 2 files to transfer together.
The final content mutation does not hold a file permit.

### State

`record.upload` is `nil` when nothing is running. It changes through the same live reads as `record.sync`, so observers get a new row for every state or progress change.

| Field | Meaning |
| ----- | ------- |
| `status` | `waiting`, `preparing`, `queued`, `uploading`, `sending`, `failed`, `deliveryUnknown` |
| `waitReason` | why a `waiting` upload does not run yet: `createUnconfirmed` (the record is still local, its `create` has no answer). `nil` in every other status |
| `stage` | `prepare`, `create`, `put`, `init`, `presign`, `part`, `complete`, `abort`, `mutation` |
| `sentBytes`, `totalBytes`, `fraction` | for multipart the sum over all parts divided by the file size |
| `attempts` | sum of retries for unfinished steps, preserved across restarts |
| `error` | kind and message, `isRetryable` is false for a rejection |

An upload of a local record starts as `waiting` with `totalBytes` set from the file size and `sentBytes` and `fraction` at zero. When the `create` is confirmed the real id replaces the local id in the upload variables and the status moves on through `preparing` or `queued` to `uploading`, `sending` and the end. If the `create` is refused the upload is discarded with the record. Every change of status, stage, bytes or reason reaches observers as a new row.

A finished upload closes its operation, so `upload` goes back to `nil` and the record is `synced`. Progress ticks are limited to one per 100 ms and never go down inside a phase. A restarted phase starts again from the bytes of its finished parts.

### Rules

- Transport errors inside the upload steps retry with backoff. Offline waits for `setOnline(true)` and spends no attempts.
- Each step and multipart part stores its retry count and next retry time. Restarting preserves both. A successful step removes its retry record.
- An explicit `retry()` after failure clears saved retry counts and times, giving unfinished steps a fresh budget.
- A PUT succeeds only with an HTTP status from 200 through 299. Other statuses follow the transfer error rules before the content mutation can run.
- A `403` renews the single-upload url or affected part url after backoff. The renewal uses that PUT step's retry budget. Urls older than the presign lifetime are requested again for unfinished parts.
- The content mutation follows the same rules as other mutations ([writing data](writing-data.md)). Offline it waits for `setOnline(true)`. If it could not reach the server, the upload stays pending and the mutation is sent again with a growing pause, up to 40 attempts, without a new transfer. If the transfer started and no answer is observed, the upload becomes `deliveryUnknown` and is never sent again by itself. A rejected mutation, or 40 unreached attempts, becomes `failed`.
- The grant, including `signedId`, `blobId`, URL and headers, is stored before PUT. A separate flag records confirmed transfer.
- A restarted unfinished PUT uses its saved grant. `retry()` after a failed or unknown content mutation skips confirmed transfers.
- A failed multipart upload calls `abort` and clears its parts. After a restart a stored multipart continues from its finished parts, and an upload that failed with an unfinished `abort` is aborted again.
- `retry()` and `discard()` on the record work as for any pending operation. `cancelUpload` stops the transfer, aborts a multipart and removes the operation.
- Offline cancellation returns without a network call and keeps the multipart upload id. After `setOnline(true)`, the saved cancellation aborts the multipart and removes the operation.
- One record has one upload at a time.

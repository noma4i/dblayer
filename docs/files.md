# Files

A `file` field stores binary data such as images, audio, video and documents.

```ruby
schema do
  file :media, null: true
end
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

- The row holds a description of the file; observers see a new row revision when the file changes.
- `attach` replaces the old file and updates the row in one commit. If the commit fails, no file remains.
- Destroying a row removes its files. Orphans left by crashes are cleaned by maintenance.
- A temporary row keeps its file when the row gets its server id.
- A file field cannot be indexed.

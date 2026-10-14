# Local and synced models

Choose storage in the model declaration. Both platforms use the same rules.

| Declaration | Persistence | Writes | Server declarations |
| --- | --- | --- | --- |
| `storage: :local` | SQLite file | Immediate, no outbox | Rejected |
| Default or `storage: :disk` | SQLite file | Optimistic, queued | Allowed |
| `storage: :memory` | Memory only | No durable outbox | Remote reads allowed |

Local and synced models can share a database. A local model needs no separate database file.

`storage: :local` rejects `remote`, `listen`, `action`, `create with:`, `update with:` and `destroy with:`.
Declare fields, nested values, scopes and associations as usual. Generated create, update and destroy calls still work.

```ruby
model Session, storage: :local do
  schema do
    string :token
  end
end
```

```swift
let session = try await Session.create(token: "saved")
let edited = try await session.update(token: "changed")
let cached = await Session.find(edited.id).current()
try await edited.destroy()
```

```kotlin
val session = Session.create(token = "saved")
val edited = session.update(token = "changed")
val cached = Session.find(edited.id).current()
edited.destroy()
```

Local rows notify readers and survive reopening. Create, update and destroy generate no network request and write no outbox entry.
Their sync state is `synced`, meaning that no server operation is pending.

A disk model without remote or mutation declarations is valid. Its writes queue until matching mutation declarations bind them.
Use local storage when a model must never send its values.

`local: true` on a field has a different purpose. It keeps one field local inside a synced model.
See [local fields](models.md#local-fields) and [writing data](writing-data.md).

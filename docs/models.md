# Describing models

A model is described once in a `.dbl` file. The syntax follows Ruby on Rails: a word, a symbol name, then named options after colons and commas. Order of options does not matter.

```ruby
model Moment do
  schema do
    string :uuid,      index: true
    id     :userId,    null: true, index: true
    string :text,      null: true
    object :media,     default: {}
    date   :createdAt, index: true
    index :userId, :createdAt
  end
  belongs_to :author, model: User, key: :userId, touch: true
  has_many   :vibes,  model: Vibe, key: :momentId, dependent: :destroy
  scope :recent,  order: { createdAt: :desc }
  scope :by_user, params: [:userId], where: { userId: :userId }, order: { createdAt: :desc }
  remote :feed do
    query MomentsFeed
    paginate 20
    fresh_for 5.minutes
  end
end
```

## The `schema` block

Fields and indexes live only inside `schema do ... end`. A field outside it is an error.

| Word | Type |
| ---- | ---- |
| `string`, `num`, `int`, `bool`, `date`, `id` | scalars (`id` accepts a string or a number and stores a string, `date` accepts an ISO 8601 text or milliseconds and stores UTC text with milliseconds, `2026-01-03T00:00:05Z` becomes `2026-01-03T00:00:05.000Z`, so text order is time order) |
| `enum :name, values: [:a, :b]` | one of the listed values |
| `object`, `array`, `raw` | JSON values |
| `list :name, of: :string` | array of scalars |
| `file :name` | binary attachment, see [files](files.md) |

Field options:

| Option | Meaning |
| ------ | ------- |
| `null: true` | value may be null |
| `optional: true` | key may be absent in server responses |
| `default: value` | default value, also stored when the server answers null for a field without `null: true` |
| `index: true` | SQLite index on the field column (not allowed for `object`, `array`, `raw`, `list`, `file`) |
| `id_from: :parent` | take the id from a nested parent object, also when the same object is a `belongs_to` association |
| `as: MomentMedia` | for `object` and `array`: name a generated value type, declared by a field block in the input files |
| `from: "location.lat"` | read the value from a nested path of the response when the field name itself is absent |
| `merge: :non_empty` | what a row from the server may replace in a stored value, see below |
| `local: true` | the server never sends the field and a refresh never replaces or clears it, see [local fields](#local-fields) (needs `optional: true` or `null: true`) |
| `searchable: true` | the text of the field is indexed for full-text search, see [text search](live-data.md#text-search) (`string` and `list` of strings only) |

`index :a, :b` inside `schema` declares a composite index over fields that are already indexed.

Every scalar and enum field is its own typed SQLite column, indexed or not. An update writes only the columns of the fields that changed. A null value and an absent value are stored the same way (SQL `NULL`), so a field read back is either present or missing, never `null`.

### Nested objects and lists

Declare JSON fields with `object do ... end` or `array do ... end`. An object generates a Swift `Codable`, `Sendable`, `Equatable` struct and a Kotlin serializable data class. An array generates a list of that type. Both platforms use the same type and property names. Inside the `Message` model:

```ruby
  schema do
    object :replyTo, as: MessageReplyNode, null: true do
      id :id, null: true
      string :body, null: true
      string :kind, null: true
      id :userId, null: true
      object :user, as: MessageUserRef, null: true do
        id :id, null: true
        string :fullName, null: true
        string :name, null: true
        string :username, null: true
        string :avatarUrl, null: true
      end
      object :media, as: MomentMedia, null: true do
        id :id, default: ""
        string :fileUrl, null: true, from: "file_url"
      end
    end
    array :replies, as: MessageReplyNode, default: []
  end
```

Use generated values when creating or updating a model, or decode a response with the platform serializer. Do not write another struct, data class or decoder with the same fields.

```swift
let reply = MessageReplyNode(body: "Hello", userId: "u1", user: MessageUserRef(id: "u1", fullName: "Ada"))
let message = try await Message.create(replyTo: reply, replies: [reply])
let name = message.replyTo?.user?.fullName
```

```kotlin
val reply = MessageReplyNode(body = "Hello", userId = "u1", user = MessageUserRef(id = "u1", fullName = "Ada"))
val message = Message.create(replyTo = reply, replies = listOf(reply))
val name = message.replyTo?.user?.fullName
```

Nested blocks accept `string`, `num`, `int`, `bool`, `date`, `id`, `enum`, `list`, `object`, `array` and `raw`. Options are `null`, `optional`, `default` and `from`, with `values` for enums, `of` for scalar lists and `as` for objects and arrays. Blocks can contain more blocks. Without `as`, the type name joins the owning type and the capitalized field name. All generated value types have top-level names.

For reusable names, recursive lists, unrestricted JSON and typed defaults, see [nested value types](nested-types.md).

| Input | Decoding rule on both platforms |
| ----- | ------------------------------ |
| Unknown key | ignored by generated types, retained inside unrestricted JSON |
| Absent nullable or optional field without a default | `nil` / `null` |
| Absent field with a default | declared default |
| Null nullable or optional field | `nil` / `null`, even when a default is declared |
| Null required field with a default | declared default |
| Absent or null required field without a default | decoding error |
| Wrong type for a present field | decoding error |
| ID string or signed 64-bit integer | string, for scalar IDs and scalar ID lists |
| Boolean, fractional number, object or out-of-range integer as an ID | decoding error |
| Nested date or enum | string, as in the generated model properties |

An enum default must be one of its declared values. Nested enums decode as strings. Unknown server values are preserved. `from` inside a nested block names a literal JSON key, including dots. At the model level, `from` reads a response path.

Declare each named type once. Pass all files that declare or use it to `dblayer-gen`. `as` accepts a type name such as `GiftItem`, never a Swift list expression. A missing declaration, duplicate type or invalid default reports its input file and line. `raw` has no `as` option.

Nested values belong to their containing row. They have no independent table, index, relation, file attachment or sync operation. Put those features on a model. The same declarations work in local, memory and synchronized models.

### Merge rules

A row from the server (page, event, mutation answer, replace) replaces the fields it carries. A `merge:` rule on a field decides what an arriving value may replace, so the order in which answers arrive does not matter.

| Rule | An arriving value is ignored when |
| ---- | --------------------------------- |
| `:present` | it is null and a value is stored |
| `:non_empty` | it is null, `""`, `[]` or `{}` and a non-empty value is stored |
| `:positive` | it is not a number above 0 and a positive number is stored (`int`, `num`, `date`) |
| `:monotonic` | it is null or lower than the stored number or date (`int`, `num`, `date`) |
| `[[:PENDING, :PROGRESSING], [:COMPLETED, :FAILED]]` | its tier is below the tier of the stored value, values inside a tier replace each other (`enum` or `string`) |
| `{ key: rule, ... }` | `object` and `raw`: the object is merged key by key, a key without a rule ignores null, a null object keeps the stored one |

```ruby
object :media, as: MomentMedia, null: true, merge: { fileUrl: :non_empty, width: :positive, transcodeStep: [[:PENDING, :SUBMITTED, :PROGRESSING], [:COMPLETED, :FAILED]], transcodeProgress: :monotonic }
string :avatarUrl, null: true, merge: :non_empty
```

A local edit that is not confirmed yet is never replaced, with or without a rule. A rule that does not fit the field type rejects the model.

## Local fields

A local field is written by the app and stored with the row. The server never sends it: it is not in the sync diff, no mutation carries it, and no answer, page, event or refresh replaces or clears it. It reads `nil` until it is written, survives a restart, and goes with the row when the row is destroyed. Every subscription of the row delivers it together with the server fields.

```ruby
string :tone, optional: true, local: true
object :media, as: MomentMedia, null: true, merge: { fileUrl: :non_empty, tone: :local }
```

`local: true` declares a top-level field. A key of an `object` or `raw` field is local with `tone: :local` inside its `merge:` hash. The value of a local key is any JSON the app type decodes. A local field is part of the generated model type and is left out of `create` and `update`.

`setLocal` writes a local field by path (`tone`, or `media.tone` for a key). One write is one delivery to the observers of the row, and a write that does not change the value is silent. Passing null clears the field. A write to a row that is not stored yet waits and is applied when the row arrives, so the writer can be faster than the server.

```swift
try await moment.setLocal("media.tone", .object(["hue": .number(30)]))
```

```kotlin
moment.setLocal("media.tone", buildJsonObject { put("hue", 30) })
```

A path that is not declared local is refused.

## Ephemeral fields

An `ephemeral do ... end` block inside `schema` declares values that live only in memory. They are never written to SQLite and do not survive a restart.

```ruby
model User do
  schema do
    string :name

    ephemeral do
      bool   :typing, default: false, reset_after: 5.seconds
      string :mood,   null: true
    end
  end

  remote :profile do
    query UserProfile
    updates UserTyping, set: { typing: true }
  end
end
```

| Option | Meaning |
| ------ | ------- |
| `default: value` | value before the first write and after a reset. Required unless `null: true` |
| `null: true` | the default is null |
| `reset_after: 5.seconds` | after this time without a write the value returns to `default`. Needs `default:` |

- Reads see the values: `user.typing` is part of the row, and `select` returns it.
- A write restarts the timer. When the time is up the value returns to `default`.
- A write and a reset reach observers as a normal change with the row in `diff.updated`. Neither creates a database revision.
- Deleting a row removes its values. Writing the value a field already holds delivers nothing.
- A write for a row that is not stored yet is kept and shows when the row arrives. The timer starts at the write, so a value for a row that never arrives lapses by `reset_after`.
- `where`, `order`, `index` and `scope` cannot use an ephemeral field, because those run in SQL. `dblayer-gen` reports it with file and line.
- A field listed in `set:` must declare `reset_after`, so a value for a row that never arrives always lapses. `dblayer-gen` reports it with file and line.
- `updates Subscription, set: { typing: true }` writes the listed values to the row named by each subscription event, after the event is applied. A deleting event writes nothing.

Write from code:

```swift
try await user.set(typing: true)
```

```kotlin
user.set(typing = true)
```

## Memory models

`model Session, storage: :local` persists rows without an outbox. It rejects server declarations. Local and synced models share a database. See [storage rules](storage-models.md).

`model Presence, storage: :memory` keeps the whole model in memory. Rows are read, observed and loaded through `remote` like any other model. They are not written to the database file and are empty after a restart.

```ruby
model Presence, storage: :memory, expire_after: 1.minute do
  schema do
    id     :userId, index: true
    string :room
  end

  remote :online do
    query PresenceOnline
    list
    fresh_for 30.seconds
  end
end
```

- `expire_after:` is optional. A row that has not changed for that time is deleted, and observers receive it in `diff.removed`.
- A memory model has no `file` fields and no `create`, `update`, `destroy` or `action` lines, so it has no outbox.
- Use it for search results, prices and other data that is fetched, shown and dropped.

```swift
@Live var online: [Presence]
_online = Live(Presence.where(Presence.room == "lobby"))
```

```kotlin
val online by Presence.where(Presence.room eq "lobby").collectAsLive()
```

## Associations

| Word | Options |
| ---- | ------- |
| `belongs_to :name` | `model:`, `key:` (required), `touch: true`, `sets:`, `newer:`, `from:` |
| `has_many :name` | `model:`, `key:`, `dependent: :destroy`, `from:` |
| `has_one :name` | `model:`, `key:`, `dependent: :destroy`, `from:` |
| `references :name` | `model:`, `key:` (array of ids), `from:` |

- `from: "visitorsList.nodes"` is the dotted path in a response where the association's objects sit. It defaults to the association name. A list on the way is walked element by element, and a connection (`nodes` or `edges { node }`) gives its nodes.
- Objects found there become rows of the target model. A `belongs_to` object that holds only its `id` (`owner { id }`) cannot become a row when the target needs more fields, and it still sets `key:` to that id. `has_many` and `has_one` rows get the parent id in `key:`. A `references` parent keeps the ids of its rows in `key:`; declare that field in `schema` (`list :similarIds, of: :id`), the model is rejected without it.
- `dependent: :destroy` removes children in the same commit as the parent.
- `touch: true` bumps the parent's revision when a child changes, so observers of the parent refresh.
- `sets: { lastMessageId: :id, lastMessageAt: :createdAt }` on a `belongs_to` writes the child fields to the parent fields in the same commit as every write of the child: a local `create`, an event, a page landing, the answer of a mutation, a replace, `land`. Destroying the child that the parent points at (the first parent field set from the child `id`), locally or from the server, moves the parent fields to the newest remaining child by the `newer` child fields, or to null when none remains. `newer: { sequenceNumber: :lastSequenceNumber, createdAt: :lastMessageAt }` writes only when the child is newer than what the parent holds. The pairs are a tuple in the order written: the first pair where both sides have a value and differ decides, a child with the id the parent already points at always refreshes, a temporary id outranks a confirmed one (client and server clocks never order them), and an empty parent value counts as older. The same tuple guards the parent row itself: a row of the parent that arrives from a list, an event or a mutation answer never moves these parent fields backward. An event for a child whose parent is not stored (a chat outside the loaded pages) reloads the watched lists of the parent model. A discard or a refusal gives the parent values back, the swap of a temporary child id moves a parent field that points at it. Both options name fields that exist on their side, otherwise the model is rejected.
- The foreign key of a `has_many` or `has_one` must be an indexed field of the target model.
- There are no counter caches. Count with a query: `moment.vibes.count`.

## Scopes and remote relations

`scope :name` declares a named query: `params:`, `where: { field: :param }`, `order: { field: :asc | :desc | :asc_nulls_last | :desc_nulls_first }`. Rows without a value come first in `:asc` and last in `:desc`; the two `nulls` directions turn that around, for example a message that has no sequence number yet (not sent) sorts after the numbered ones with `order: { sequenceNumber: :asc_nulls_last, createdAt: :asc }`.

When a response row has no value for a column the relation groups by, the param fills it. A response without an id becomes a row keyed by the param.

`remote :name do ... end` connects a relation to the server. A relation that depends on columns names them: `remote :by_user, params: [:userId] do ... end` loads and lists the rows whose `userId` equals the param. `order: { createdAt: :desc }` sorts the rows locally, otherwise the server order is kept. A row whose param column changes (a message moving to another `chatId`) leaves the list of the old value and joins the head of the list of the new value in the same commit, for relations that list every row of their params. `joins: :created` on a relation without params makes the list take the rows this device creates, at its head; `joins: :events` makes it take rows from its subscription events.

`params:` accepts scalar and enum columns. A param passed as `nil` is a value of its own: `Moment.by_user(nil)` is the unfiltered list and has its own window. A param column gives the row one owner list: a row that belongs to several lists at once (a moment in the default feed and in a vibe feed) needs one relation per list, and the vibe id goes in `text: [:vibeId]`, not in a column param. `text: [:query]` declares params that are not columns, such as a search string. They are sent as variables, always strings, and each value gets its own window: `remote :search, text: [:query] do ... end` generates `Model.search("cats")`.

| Line | Meaning | Default |
| ---- | ------- | ------- |
| `query Operation` | GraphQL operation name (required) | none |
| `query Operation, root: "a.b"` | path of the rows inside the response | first field of the operation |
| `paginate 20` | Relay-style pages of 20, forward | whole response |
| `paginate 20, backward: true` | pages backward | forward |
| `paginate 20, cursor: :afterSequence, from: "lastSequenceNumber", cursor_type: :int` | the server pages by its own variable: the next page sends `afterSequence`, taken from `lastSequenceNumber` of the connection. No page size is sent | Relay `first`/`after` |
| `list` | response is a plain list | single record |
| `fresh_for 5.minutes` | how long a response counts as fresh | `5.minutes` |
| `fresh_for 5.minutes, empty: 30.seconds` | shorter freshness for an empty response | same |
| `max_pages 5` | page limit per load | none |
| `variables statusFilter: :status, includeGifts: false` | name the GraphQL variables: a symbol takes the value of a relation param, any other value is sent as written | every param under its own name |
| `updates Subscription, deleted_by: :field` | keep the relation current with a subscription. Write the line again for each subscription. `removes: true` makes every event delete its row, `debounce: 300.milliseconds` merges bursts | none |

An operation name is a constant like `MomentsFeed`, or a string like `"feed"` when the GraphQL name starts with a lowercase letter.

Durations are written `300.milliseconds`, `30.seconds`, `5.minutes`, `2.hours`, `1.day`. Mutations and the `updates` line are described in [server sync](server-sync.md).

## Validation

`dblayer-gen` reports mistakes with file and line (`user.dbl:12: unknown key touch for has_many`). The same checks run in the core when a database is opened: unknown models, missing indexes on foreign keys, duplicate names and invalid combinations.

```sh
cargo run -p dblayer-gen -- spec fixtures/*.dbl     # print the model specification as JSON
```

Example models live in `fixtures/`.

## Check against the server schema

`dblayer-gen --schema schema.graphql models/*.dbl` checks the declarations against the GraphQL schema of the server. It prints nothing but notes when every check passes, and exits with code 1 and a file and line for each mismatch.

For each `remote`, the check finds the operation named by `query`, takes its first field (the alias when there is one) and follows `root:` from there, through connections (`nodes` or `edges { node }`) to the type that holds the rows. Operations are read from the `queries` and `mutations` directories next to the schema file, or from every `--operations <file or directory>` you pass. Without operation documents the operation name is read as the schema field.

Every field of the model, every `from:` path, every `id_from:` and every field of a nested type must exist on that type. The check reports:

- a field or path the schema does not have;
- a list declared as a single value, or the reverse;
- a nullable schema field declared without `null: true`, `optional: true` or `default:`;
- a scalar, enum or object that does not fit the schema type, and an `enum` value the schema enum lacks;
- a `remote` that paginates, lists or returns one row against a schema field of another shape;
- an association `from:` path of the wrong shape.

Each arg declared with `args:` on `create`, `update` or an action is checked against the mutation that sends it. With `wrap:`, the check reads the input type of the wrapped variable. Without it, the check reads the variables of the operation. The check needs the mutation in the operation documents and reports:

- a variable that `input:` binds to an arg but the input type or the operation does not have;
- an arg whose kind does not fit the type: `:string` fits `String`, `ID`, an enum or a custom scalar, `:id` fits `ID`, `Int`, `String` or a custom scalar, `:int` fits `Int`, `:num` fits `Float` or `Int`, `:bool` fits `Boolean`, `:ids` fits a list of `ID`, `Int` or `String`;
- an arg with `?` that the schema requires (`String!` without a default).

A required arg may fill an optional input. An input with a default counts as optional.

Fields marked `local: true`, `file` fields and models with `storage: :local` are skipped. Schema fields that the model leaves out are listed as notes, because a model may select a subset.

## Access level

`dblayer-gen --internal swift ...` emits models and their generated value types without `public`, for an app that generates them into its own module.

## Input objects of the schema

An `operation` block can take an input object of the server schema as an arg (`args input: :StickerInput`, see [server sync](server-sync.md)). `dblayer-gen swift|kotlin` needs the schema to write it:

`dblayer-gen --schema schema.graphql swift models/*.dbl` checks the models as above and also writes the input objects. The same flag does both, so the schema file must satisfy the check.

The generator writes every input object that an operation names, and every input object those fields use, once each, after the models. A name that the schema does not have, a name that is not an input object, or an operation that names one without a schema file stops the run with code 1 and a message.

| Schema field | Swift | Kotlin |
| ------------ | ----- | ------ |
| `String`, `ID`, an enum, a custom scalar | `String` | `String` |
| the scalar named `JSON` | `JSONValue` | `JsonElement` |
| `Int`, `Float`, `Boolean` | `Int`, `Double`, `Bool` | `Int`, `Double`, `Boolean` |
| `[T!]!`, `[T]` | `[T]`, `[T?]?` | `List<T>`, `List<T?>?` |
| another input object | its struct | its data class |
| a field without `!` | optional, not sent when nil | nullable with default `null`, not sent when null |

```swift
let input = StickerInput(name: "wave", tags: ["a"], pinned: true, owner: OwnerInput(id: "7"))
```

```kotlin
val input = StickerInput(name = "wave", tags = listOf("a"), pinned = true, owner = OwnerInput(id = "7"))
```

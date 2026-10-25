# Describing models

A model is described once in a `.dbl` file. The syntax follows Ruby on Rails: a word, a symbol name, then named options after colons and commas. Order of options does not matter.

## Attributes

Generated Kotlin models and their nested value types carry Compose `@Immutable` from its lightweight annotation dependency.
Applications need no stability config for these models or their read-only `List`, `Set`, and `Map` fields.
Treat collection contents as immutable and replace model instances when values change.
Swift generation is unchanged.

Declare attributes and indexes directly inside `model`. Use `attribute :name, :string` for an explicit storage type.

Generated fields, parameters and methods escape Swift and Kotlin keywords. JSON keys and model declarations retain their original names.
SQLite column identifiers are quoted, so keyword fields also support storage, indexes, filters and updates.

With `--schema`, `attribute :name` without a type infers its type and nullability from the matching GraphQL type. Local attributes require an explicit type.
The DSL is parsed by Rust. It does not execute Ruby expressions or load Ruby gems.

| Word | Type |
| ---- | ---- |
| `:string`, `:num`, `:int`, `:bool`, `:date`, `:id` | scalars (`id` accepts a string or a number and stores a string, `date` accepts an ISO 8601 text or milliseconds and stores UTC text with milliseconds, `2026-01-03T00:00:05Z` becomes `2026-01-03T00:00:05.000Z`, so text order is time order) |
| `attribute :name, :enum, values: ["a", "b"]` | one of the listed values, generated as a typed enum (see [Enums](#enums)). `as: Name` names the type |
| `attribute :name, [:enum], values: ["a", "b"], as: Name` | list of enum values, `[Name]` in Swift and `List<Name>` in Kotlin, stored like `[:string]` (see [Enums](#enums)) |
| `:json` | any JSON value, an object, an array or a scalar |
| `attribute :name, [:string]` | array of scalars |
| `attribute :name, :file` | binary attachment, see [files](files.md) |

Attribute options:

| Option | Meaning |
| ------ | ------- |
| `null: true` | value may be null |
| `optional: true` | key may be absent in server responses |
| `default: value` | default value, also stored when the server answers null for a field without `null: true` |
| `parse: :json` | `:json` attribute input must be JSON text, core stores the parsed value, and its default must be an object or null |
| `id_from: :parent` | take the id from a nested parent object, also when the same object is a `belongs_to` association |
| `MomentMedia` or `[MomentMedia]` as the type | named nested value or list, declared once by an attribute block |
| `from: "location.lat"` | read the value from a nested path of the response when the attribute name itself is absent |
| `merge: :non_empty` | what a row from the server may replace in a stored value, see below |
| `local: true` | the server never sends the field and a refresh never replaces or clears it, see [local fields](#local-fields) (needs `optional: true` or `null: true`) |

For a top-level `from` field, an explicit null at any segment clears a nullable field or applies its non-null default.
An absent path key leaves the stored field unchanged.

`index :a` declares a single-column SQLite index. `index :a, :b` declares a composite index without requiring individual indexes. Columns must be scalar or enum fields.

`search :text, :tags` selects string fields or lists of strings for [full-text search](live-data.md#text-search).

Declared queries generate indexes with equality filters first and sort columns next, preserving each sort direction. Duplicate indexes and indexes covered by a longer prefix are removed.

Every scalar and enum field is its own typed SQLite column, indexed or not. An update writes only changed fields.
Nullable fields with defaults retain an explicit null marker, so reopening preserves the difference between an absent value and null.

### GraphQL errors

GraphQL failures expose `extensions` on Swift `DbError.Graphql` and Kotlin `DbException.Graphql`, including failures from `settled(action)`.
`GraphqlExtensions.code` is the optional server string, such as `REFUSED`.
`transactionDisposition` is an optional `TransactionDisposition`: `finish` / `Finish`, `keep` / `Keep`, or `unknown(value:)` / `Unknown(value)`.
Unknown values retain their original text, just like model enums.
Only `errors[0].extensions` supplies these fields. Missing, null and non-string typed fields remain absent, even when later errors have values.
`fields` contains every other entry as JSON text: `[String: String]` in Swift and `Map<String, String>` in Kotlin.
Strings keep their JSON quotes, numbers keep their JSON representation, and objects, arrays, booleans and null retain their values.
An error containing only these entries still has `extensions`. Stored operation errors retain the same entries after reopening the database, as node rows, not JSON text.
Notes remain available separately. Transaction disposition describes the server decision, and does not change queue retry or rollback rules.

```swift
do {
  _ = try await DB.database.run(operation: "PurchaseCreate", variablesJson: inputJSON)
} catch let DbError.Graphql(messages, extensions) {
  let code = extensions?.code
  let detail = try extensions?.fields["detail"].map { try JSONValue.decode(Data($0.utf8)) }
  switch extensions?.transactionDisposition {
  case .finish: finishTransaction()
  case .keep: keepTransaction()
  case .unknown(let value): reportUnknown(value)
  case nil: reportFailure(code, messages)
  }
}
```

```kotlin
try {
    DBlayer.database.run("PurchaseCreate", inputJSON)
} catch (error: DbException.Graphql) {
    val code = error.extensions?.code
    val detail = error.extensions?.fields?.get("detail")?.let { Json.parseToJsonElement(it) }
    when (val disposition = error.extensions?.transactionDisposition) {
        TransactionDisposition.Finish -> finishTransaction()
        TransactionDisposition.Keep -> keepTransaction()
        is TransactionDisposition.Unknown -> reportUnknown(disposition.value)
        null -> reportFailure(code, error.messages)
    }
}
```

### Enums

Every `:enum` attribute becomes an enum named after its owner and field (`VibeKind` for `Vibe.kind`, `NoteReplyNodeKind` for a nested `kind`). `as: Kind` sets the name. A schema enum used by an input object, an operation arg or an operation result keeps the schema name (`WidgetKind`). Each name is declared once, and the same name with different values stops the run, wherever the two declarations sit: a field, a nested type, a union variant, a shared field or an `edge`. The message names the enum and points at both declarations.

Case names come from the values: lowerCamel in Swift (`like`, `fooBar`, `` `default` `` for a keyword) and PascalCase in Kotlin (`Like`, `FooBar`). `HAPPY` and `FOO_BAR` read as words. A value that starts with a digit, becomes an empty name, becomes `unknown` (Swift) or `Unknown` and `Companion` (Kotlin), or gives the same name as another value stops the run with the value in the message.

Every enum has an `unknown` case that keeps the text. A value the declaration lacks is not an error on any path: a server row, a nested JSON value, an input, an argument and a result decode it into `unknown`, and writing it back sends the same text. The SQLite column stays text and is stored as it arrived, so a filter compares the text of the typed value.

```swift
let loved = try Vibe.where(Vibe.kind == .love)
let vibe = try await Vibe.create(momentId: "m", userId: "u", kind: .love)
switch vibe.kind {
case .like, .love, .laugh: break
case .unknown(let text): print(text)
}
```

```kotlin
val loved = Vibe.where(Vibe.kind eq VibeKind.Love)
val vibe = Vibe.create(momentId = "m", userId = "u", kind = VibeKind.Love)
when (val kind = vibe.kind) {
    VibeKind.Like, VibeKind.Love, VibeKind.Laugh -> Unit
    is VibeKind.Unknown -> println(kind.value)
}
```

The check against the server schema compares the declared values with the schema enum. A field declared `:string` over a schema enum does not fit: declare `:enum`.

A list of enum values is `[:enum]` with the same `values:` and `as:`. It is stored as a list of strings and reads back as a list of the enum, each value outside the declaration as `unknown`. The schema check needs a list type (`[GroupList!]!`), and `null: true` for a nullable one. A `default:` lists values from `values`. The block form of a nested type takes the same line.

```ruby
model Group do
  attribute :lists, [:enum], values: ["PRIMARY", "SECONDARY"], as: GroupList
end
```

```swift
let group = try await Group.create(lists: [.primary, .secondary])
let secondary = group.lists.contains(.secondary)
```

```kotlin
val group = Group.create(lists = listOf(GroupList.Primary, GroupList.Secondary))
val secondary = GroupList.Secondary in group.lists
```

A default outside the values stops the run with the line of the field.

### Custom scalars

A schema scalar other than the built-in ones needs one line in a `.dbl` file, next to the models:

```ruby
scalar :Cursor, as: :string

scalar :ISO8601Date, as: :date

scalar :ISO8601DateTime, as: :date

scalar :Int64, as: :long
```

`as:` is `:string`, `:date` (the ISO 8601 text, `String` like a `date` field), `:int`, `:long` (`Int` in Swift, `Long` in Kotlin), `:num`, `:bool` or `:json`. A scalar declared twice or with another kind stops the run. A custom scalar without a line stops the run when an input object, an arg or a model field uses it, with the line to add. `JSON` needs no line.

### Nested objects and lists

Declare nested values with `attribute :name, NamedType do ... end`. Use `[NamedType]` for a list.
An object generates a Swift `Codable`, `Sendable`, `Equatable` struct and a Kotlin serializable data class.
Models decode from row cells. Both platforms use the same type and property names. Use generated values when creating or updating a model. Their serializers read and write canonical nested JSON. Server response paths are resolved in the core before storing rows. Do not write another struct, data class or decoder with the same fields.

Serialization errors propagate to the caller. Non-finite numbers never become null or reach a request.
Swift value encoding, typed comparisons, parameterized scopes and upload variable builders use `try`.
Live sources serialize their query when opened and report errors through that opening operation.
Kotlin JSON text uses `jsonText()`. `parseJsonObject` rejects non-object values.

```swift
let reply = NoteReplyNode(body: "Hello", userId: "u1", user: NoteUserRef(id: "u1", fullName: "Ada"))
let message = try await Note.create(replyTo: reply, replies: [reply])
let name = message.replyTo?.user?.fullName
```

```kotlin
val reply = NoteReplyNode(body = "Hello", userId = "u1", user = NoteUserRef(id = "u1", fullName = "Ada"))
val message = Note.create(replyTo = reply, replies = listOf(reply))
val name = message.replyTo?.user?.fullName
```

Nested blocks contain `attribute` declarations. Their options include `null`, `optional`, `default`, `from`, and `values` for enums. Blocks can contain more blocks. An unnamed object joins the owning type and capitalized field name. All generated value types have top-level names.

For reusable names, recursive lists, unrestricted JSON, typed defaults, GraphQL unions and interfaces, see [nested value types](nested-types.md).

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
| Nested date | string, as in the generated model properties |
| Nested enum | its generated enum, a value the declaration lacks becomes the `unknown` case with the text |

An enum default must be one of its declared values. `from` reads a server response path at every nesting level. A path such as `user.id` traverses the `user` object, never a literal server key containing a dot. The core reads the path once and stores its value under the canonical key `user.id`. Swift `Codable` and Kotlin serializers use that canonical key for local values. SQLite, row cells and optimistic changes keep that representation and never interpret it as a server path again.

Declare each named type once. Pass all files that declare or use it to `dblayer-gen`. The type accepts a named value such as `RewardItem` or a list such as `[RewardItem]`. A missing declaration, duplicate type or invalid default reports its input file and line. Use `:json` for unrestricted values.

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
| `{ key: rule, ... }` | `:json`: the object is merged key by key, a key without a rule ignores null, a null object keeps the stored one |

```ruby
attribute :media, MomentMedia, null: true, merge: { fileUrl: :non_empty, width: :positive, transcodeStep: [[:PENDING, :SUBMITTED, :PROGRESSING], [:COMPLETED, :FAILED]], transcodeProgress: :monotonic }
attribute :avatarUrl, :string, null: true, merge: :non_empty
```

A local edit that is not confirmed yet is never replaced, with or without a rule. A rule that does not fit the field type rejects the model.

## Local fields

A local field is written by the app and stored with the row. The server never sends it: it is not in the sync diff, no mutation carries it, and no answer, page, event or refresh replaces or clears it. It reads `nil` until it is written, survives a restart, and goes with the row when the row is destroyed. Every subscription of the row delivers it together with the server fields.

```ruby
attribute :tone, :string, optional: true, local: true
attribute :media, MomentMedia, null: true, merge: { fileUrl: :non_empty, tone: :local }
```

`local: true` declares a top-level attribute. A key of a `:json` attribute is local with `tone: :local` inside its `merge:` hash. The value of a local key is any JSON the app type decodes. A local field is part of the generated model type and is left out of `create` and `update`.

`setLocal` writes a local field by path (`tone`, or `media.tone` for a key). One write is one delivery to the observers of the row, and a write that does not change the value is silent. Passing null clears the field. A write to a row that is not stored yet waits and is applied when the row arrives, so the writer can be faster than the server.

```swift
try await moment.setLocal("media.tone", .object(["hue": .number(30)]))
```

```kotlin
moment.setLocal("media.tone", buildJsonObject { put("hue", 30) })
```

A path that is not declared local is refused.

## Ephemeral fields

An `ephemeral do ... end` block inside `model` declares values that live only in memory. They are never written to SQLite and do not survive a restart.

```ruby
model User do
  attribute :name, :string
  ephemeral do
    attribute :typing, :bool, default: false, reset_after: 5.seconds
    attribute :mood, :string, null: true
  end
  scope :profile do
    query "UserProfile"
    subscribe "UserTyping", apply: { effect: :upsert, values: { typing: true } }
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
- An attribute written by `values:` must declare `reset_after`, so a value for a row that never arrives always lapses. `dblayer-gen` reports it with file and line.
- `subscribe "Name", apply: { effect: :upsert, values: { typing: true } }` writes the listed values to the row named by each subscription event, after the event is applied. A deleting event writes nothing.

Write from code:

```swift
try await user.set(typing: true)
```

```kotlin
user.set(typing = true)
```

## Cloud models

`cloud` marks the attributes a model sends to the user's own cloud copy. The generator expands the word into a list of attribute names and writes it to the model spec as `"cloud"`. A model without the word has no `"cloud"` key.

```ruby
model Draft, storage: :local do
  attribute :title, :string
  attribute :pinned, :bool, default: false
  cloud
end

model ReadCursor, storage: :local do
  attribute :chatId, :id
  attribute :readUntil, :int, null: true
  attribute :scroll, :int, default: 0
  cloud :chatId, :readUntil
end

model Note do
  attribute :title, :string
  attribute :tone, :string, optional: true, local: true
  cloud :tone
end
```

- `cloud` with no names selects every attribute of a `storage: :local` model.
- `cloud :a, :b` selects the named attributes.
- On a `storage: :disk` model the names are required, and each attribute needs `local: true`. A server attribute is an error.
- `storage: :memory`, `ephemeral` attributes, unknown names, repeated names and a second `cloud` are errors.
- A `storage: :local` model with a partial selection needs every other attribute to have `null: true`, `optional: true` or a `default`. Otherwise another device cannot create the row.

## Memory models

`model Session, storage: :local` persists rows without an outbox. It rejects server declarations. Local and synced models share a database. See [storage rules](storage-models.md).

`model Presence, storage: :memory` keeps the whole model in memory. Rows are read, observed and loaded through scopes with `query` like any other model. They are not written to the database file and are empty after a restart.

```ruby
model Presence, storage: :memory, expires_in: 1.minute do
  attribute :userId, :id
  index :userId
  attribute :room, :string
  scope :online do
    query "PresenceOnline"
    response shape: :list
    freshness expires_in: 30.seconds
  end
end
```

- `expires_in:` is optional. A row that has not changed for that time is deleted, and observers receive it in `diff.removed`.
- A memory model has no `:file` attributes and no model `operation` blocks, so it has no outbox.
- Use it for search results, prices and other data that is fetched, shown and dropped.

```swift
@Live var online: [Presence]
_online = Live(try Presence.where(Presence.room == "lobby"))
```

```kotlin
val online by Presence.where(Presence.room eq "lobby").collectAsLive()
```

### Notices in memory

A local scope of a memory model accepts `subscribe` without `query`. A disk model must not declare it.
The subscription starts at the first reader of the scope and is shared between readers. It closes after the last reader leaves, after `live_linger`.
Events are lost while no reader exists. The server does not replay them on connect.
An event without an id gets a new UUID. `expires_in` deletes the row, and the data never reaches the SQLite file or the outbox.
A model must not repeat the same subscription.

```ruby
model Notice, storage: :memory, expires_in: 1.minute do
  attribute :body, :string
  attribute :occurredAt, :date
  scope :recent do
    order occurredAt: :desc
    subscribe "BulletinAdded"
  end
end

model InteractionNotice, storage: :memory, expires_in: 1.minute do
  attribute :messageId, :id
  attribute :actorUserId, :id
  attribute :occurredAt, :date
  scope :recent do
    order occurredAt: :desc
    subscribe "RecordSignal"
  end
end

model User do
  attribute :name, :string
  ephemeral do
    attribute :typing, :bool, default: false, reset_after: 5.seconds
  end
  scope :profile do
    query "UserProfile"
    subscribe "PresenceSignal", from: "presenceSignal", id: "userId", apply: { effect: :ephemeral, fields: { typing: "active" }, matches: { kind: "ACTIVE" } }
  end
end
```

```swift
@Live var notices: [Notice]
@Live var interactions: [InteractionNotice]
_notices = Live(Notice.recent)
_interactions = Live(InteractionNotice.recent)
```

```kotlin
val notices by Notice.recent.collectAsLive()
val interactions by InteractionNotice.recent.collectAsLive()
```

## Associations

| Word | Options |
| ---- | ------- |
| `belongs_to :name` | `class_name:`, `foreign_key:`, `touch: true`, `sets:`, `newer:`, `from:` |
| `has_many :name` | `class_name:`, `foreign_key:`, `dependent: :destroy`, `from:` |
| `has_one :name` | `class_name:`, `foreign_key:`, `dependent: :destroy`, `from:` |
| `references :name` | `class_name:`, `foreign_key:` (array of ids), `from:` |

Singular associations capitalize their name, preserving a final `s`. Plural associations remove one final `s` before capitalization. Use `class_name:` for irregular names.
`belongs_to` defaults to `<name>Id`. `has_many` and `has_one` use the owner's lower-camel name plus `Id`. `references` uses its association name.

- `from: "visitorsList.nodes"` is the dotted path in a response where the association's objects sit. It defaults to the association name. A list on the way is walked element by element, and a connection (`nodes` or `edges { node }`) gives its nodes.
- Objects found there become rows of the target model. A `belongs_to` object that holds only its `id` (`owner { id }`) cannot become a row when the target needs more fields, and it still sets `foreign_key:` to that id. `has_many` and `has_one` rows get the parent id in `foreign_key:`. A `references` parent keeps the ids of its rows in `foreign_key:`; declare that attribute directly (`attribute :similarIds, [:id]`), the model is rejected without it.
- An explicit null for a `belongs_to` object or its ID clears a nullable `foreign_key:` in every server answer. The target row stays.
- An absent association path preserves the key. A required key uses its declared default, or stays unchanged when no default exists.
- Association paths use the same null reader as `from` attributes. A null `references` path empties its ID list.
- `has_one` and `has_many` write no key on the parent, so a null changes nothing.

```swift
// group { id opponent { id } }       -> Group.opponent is the user
// group { id opponent { id } }       -> opponent: null, Group.opponent is nil, the user row stays
// group { id title }                 -> opponent not selected, Group.opponent keeps its last user
let group = try await Group.find(groupId).current()
let opponent = try await group?.opponent.current().first
```

```kotlin
// group { id opponent { id } }       -> Group.opponent is the user
// group { id opponent { id } }       -> opponent: null, Group.opponent is null, the user row stays
// group { id title }                 -> opponent not selected, Group.opponent keeps its last user
val group = Group.find(groupId).current()
val opponent = group?.opponent?.current()?.firstOrNull()
```

- `dependent: :destroy` removes children in the same commit as the parent.
- `touch: true` bumps the parent's revision when a child changes, so observers of the parent refresh. See [partial child updates](parent-fields.md) for `sets` field presence.
- `sets: { lastNoteId: :id, lastNoteAt: :createdAt }` on a `belongs_to` writes the child fields to the parent fields in the same commit as every write of the child: a local `create`, an event, a page landing, the answer of a mutation, a replace, `land`. Destroying the child that the parent points at (the first parent field set from the child `id`), locally or from the server, moves the parent fields to the newest remaining child by the `newer` child fields, or to null when none remains. `newer: { sequenceNumber: :lastSequenceNumber, createdAt: :lastNoteAt }` writes only when the child is newer than what the parent holds. The pairs are a tuple in the order written: the first pair where both sides have a value and differ decides, a child with the id the parent already points at always refreshes, a temporary id outranks a confirmed one (client and server clocks never order them), and an empty parent value counts as older. The same tuple guards the parent row itself: a row of the parent that arrives from a list, an event or a mutation answer never moves these parent fields backward. An event for a child whose parent is not stored (a group outside the loaded pages) reloads the watched lists of the parent model. A discard or a refusal gives the parent values back, the swap of a temporary child id moves a parent field that points at it. Both options name fields that exist on their side, otherwise the model is rejected.
- Foreign-key indexes are inferred from associations. Declare the foreign-key attribute, without adding its index again.
- There are no counter caches. Count with a query: `moment.vibes.count`.

## Scopes

`scope :name do ... end` declares a named selection. `parameter :ownerId, :id` declares a typed parameter and `where ownerId: :ownerId` binds the column to it. A parameter bound by `where` has the name of its column. Each column takes its own `where` pair. `order field: :asc` sorts rows. Null values come first in `:asc` and last in `:desc`. Use `:asc_nulls_last` or `:desc_nulls_first` to reverse their placement.

When a response row has no value for a column the relation groups by, the param fills it. A response without an id becomes a row keyed by the param.

A `query "Operation"` line connects a scope to the server. A scope that depends on columns binds them: `where userId: :userId` loads and lists the rows whose `userId` equals the parameter. `order createdAt: :desc` sorts the rows locally, otherwise the server order is kept. A row whose param column changes (a message moving to another `groupId`) leaves the list of the old value and joins the head of the list of the new value in the same commit, for relations that list every row of their params. `insert_on: [:create]` on a scope without parameters makes the list take the rows this device creates, at its head; `insert_on: [:event, :response]` makes it take rows from its subscription events and from operation responses.

`where` accepts scalar and enum columns. A param passed as `nil` is a value of its own: `Moment.by_user(nil)` is the unfiltered list and has its own window. A parameter bound to a column gives the row one owner list: a row that belongs to several lists at once (a moment in the default feed and in a vibe feed) needs one scope per list, and the vibe id is a `parameter` without `where`, not a column parameter. A parameter without `where`, such as a search string, is sent as a variable and each value gets its own window: `parameter :query, :string` in `scope :search` generates `Model.search("cats")`.

`parameter :afterSequence, :int` and `parameter :includeRewards, :bool` declare parameters that are sent as a JSON integer or boolean. The call takes `Int` and `Bool` (`Long` and `Boolean` in Kotlin: `:int` is `Long` in every Kotlin output, like the attribute), and the core refuses a text value. Each value has its own window and its own members. The document variable of the same name (or the name on the left of `variables name: :parameter`) must be `Int` or `Boolean`. A scope that pages in ascending order and one that pages in descending order are two scopes: a window key holds the scope, its params and the direction, so their cursors never meet.

```ruby
scope :since do
  parameter :groupId, :id
  where groupId: :groupId
  parameter :afterSequence, :int
  query "NotesSince"
  response from: "group.messages"
  paginate size: 50, direction: :forward
end
```

```swift
try Note.since("c1", afterSequence: 120)
```

```kotlin
Note.since("c1", afterSequence = 120L)
```

A parameter takes the name of a schema enum as its type: `parameter :list, GroupList`. The call takes the generated enum and sends its text, and each value has its own window and its own members, the rows of the server answer. The schema check needs the enum in the schema and a document variable with the same name (or the name on the left of `variables name: :parameter`) typed with that enum, nullable or not. An enum parameter is a labeled argument in Swift (`list: .primary`) and a named one in Kotlin.

`default: :PRIMARY` on an enum parameter gives it a default value in the call: `Group.list()` sends `PRIMARY`. The value must be one of the schema enum, or the generator stops. Only enum parameters take a default. The param is always sent, so a call with and without the default share one window.

```ruby
model Group do
  scope :list do
    parameter :list, GroupList
    query "groups"
    response from: "viewer.groups"
    paginate size: 25, direction: :forward
  end
end
```

```swift
let primary = try Group.list(list: .primary)
let secondary = try Group.list(list: .secondary)   // a premium group is a member of both windows
```

```kotlin
val primary = Group.list(GroupList.Primary)
val secondary = Group.list(GroupList.Secondary)
```

**Stable order.** A group thread must not reorder rows the user has seen. `stable_order: true` on a scope with `query`, `where` columns and an `order` (and no parameter without `where`) gives each row a place in the list when it joins, and later changes of the `order` values never move it. A message that the server confirms with a lower `sequenceNumber`, the answer that replaces its temporary id, an event that edits the number and a reloaded first page leave the rows where the user saw them. A row from the server (a page, an event) that is not in the list takes the place its `order` values give it among the rows already listed, so a page of older messages lands before the rows shown. A row this device creates goes after the last row. The places live in the cache: they survive a restart, and a reset or `logout()` drops them, so the pages that load afterwards come in the order of the server. Finding the place of a row costs work that does not grow with the size of the list.

```ruby
model Note do
  attribute :groupId, :id
  attribute :sequenceNumber, :int, null: true
  attribute :createdAt, :date
  scope :thread, insert_on: [:event, :response], stable_order: true do
    parameter :groupId, :id
    where groupId: :groupId
    order sequenceNumber: :asc_nulls_last, createdAt: :asc
    query "GroupThread"
    response from: "group.messages"
    paginate size: 30, direction: :forward
  end
end
```

`insert_on: [:event, :response]` adds event rows and operation response rows to matching loaded windows, including cached rows absent from those windows. Existing members keep their places. Events need `where` columns to match, so a scope with a parameter that has no `where` stops the run with `insert_on: [:event]`. The window reloads from the server when it is stale or marked by `refetch`, and the new answer replaces its members.

| Line | Meaning | Default |
| ---- | ------- | ------- |
| `query "Operation"` | GraphQL operation name (required) | none |
| `response from: "a.b"` | path of the rows inside the response, `node.x` for a list under a [node read](#node-reads) | first field of the operation |
| `response shape: :list` | the response is a plain list | single record |
| `paginate size: 20, direction: :forward` | Relay-style pages of 20 | whole response |
| `paginate size: 20, direction: :backward` | pages backward | forward |
| `paginate size: 20, direction: :forward, cursor: { parameter: :afterSequence, from: "lastSequenceNumber", type: :int }` | the server pages by its own variable: the next page sends `afterSequence`, taken from `lastSequenceNumber` of the connection. `parameter`, `from` and `type` are all required | Relay `first`/`after` |
| `paginate ..., max_pages: 5` | page limit per load | none |
| `total from: "node.membersCount"` | `total` of the list state comes from this field of the response, after `paginate` | `totalCount` of the connection |
| `freshness expires_in: 5.minutes` | how long a response counts as fresh | `5.minutes` |
| `freshness expires_in: 5.minutes, empty_in: 30.seconds` | shorter freshness for an empty response | same |
| `preload` or `preload channel: ["lobby", "stage"]` | the library loads the scope once the app has a session: after open, sign-in and reset, and after the network returns. A parameter lists its values, several parameters load every combination. A fresh answer sends nothing. Errors show in the state of the scope | not loaded until read |
| `stable_order: true` (on the `scope` line) | the list keeps the order the rows were shown in, see [stable order](#scopes) | sorted by the `order` values on every read |
| `variables statusFilter: :status, includeRewards: false` | name the GraphQL variables: a symbol takes the value of a scope parameter, any other value is sent as written | every parameter under its own name |
| `edge do attribute :impressionUuid, :string, null: true end` | values of the connection edge that carried the row, stored for this scope, see [edge values](#edge-values) | none |
| `subscribe "Subscription", apply: { ... }` | keep the scope current with a subscription. Write the line again for each subscription. `debounce: 300.milliseconds` merges bursts | none |

An operation name is a string like `"MomentsFeed"` or `"feed"`.

Durations are written `300.milliseconds`, `30.seconds`, `5.minutes`, `2.hours`, `1.day`. Mutations and the `subscribe` line are described in [server sync](server-sync.md).

## Edge values

A connection edge can carry data of its own next to `node`, for example `impressionUuid` on `feed { edges { impressionUuid node { ... } } }`. The value belongs to the connection, not to the row: the same moment can arrive from the main feed with a value and from a vibe feed with null, and each connection keeps its own.

```ruby
scope :feed do
  query "feed"
  paginate size: 20, direction: :forward
  edge do
    attribute :impressionUuid, :string, null: true
    attribute :impressionScore, :int, null: true
  end
end
scope :vibe_feed do
  parameter :vibeId, :string
  query "feed"
  paginate size: 20, direction: :forward
  edge do
    attribute :impressionUuid, :string, null: true
  end
end
```

`edge` holds `attribute` lines with the options `null:` or `optional:` and `from:` for another key in the response. A scope declares one `edge` block. Each scope with edges generates one value type and one read with the parameters of the scope: `feedEdge(of:)` is `async` in Swift and `suspend` in Kotlin and runs on a background thread of the library. The row has no new property.

```swift
let edge = try await Moment.feedEdge(of: moment.id)      // MomentFeedEdge?
let vibe = try await Moment.vibeFeedEdge(vibeId, of: moment.id)
edge?.impressionUuid                                      // String?
```

```kotlin
val edge = Moment.feedEdge(moment.id)                     // MomentFeedEdge?, suspend
val vibe = Moment.vibeFeedEdge(vibeId, moment.id)
edge?.impressionUuid                                      // String?
```

`from:` names the key in the response, the value is stored and read under the name of the edge field. An `:enum` edge attribute declares its type like a model attribute does.

```ruby
scope :vibe_feed do
  parameter :vibeId, :string
  query "feed"
  paginate size: 20, direction: :forward
  edge do
    attribute :score, :int, null: true, from: "impressionScore"
    attribute :rank, :enum, values: ["top", "normal"], as: FeedRank, null: true
  end
end
```

```swift
let edge = try Moment.vibeFeedEdge(vibeId, of: moment.id)
edge?.score                                               // Int?, the response key is impressionScore
edge?.rank                                                // FeedRank?
```

```kotlin
val edge = Moment.vibeFeedEdge(vibeId, moment.id)
edge?.score                                               // Long?, the response key is impressionScore
edge?.rank                                                // FeedRank?
```

- A read gives `nil` for a row that is not in the connection, and a value type with `nil` properties for a row without edge values.
- The answer of the same connection replaces the values of its rows, also with null. A row that leaves the connection loses its values. An answer without edges (a node read, a mutation, an event, a list, another connection) leaves them as they are.
- The values live in the cache. A database that is rebuilt or reset loses them, and the next load of the connection brings them back.
- A change of an edge value alone does not change the row or a live list. Read the value when the row is shown.
- A read takes one row by key from the reader connection. It does not wait for a write and costs the same at 20 and at 2000 rows.
- `edge` needs `paginate`, and each edge attribute needs `null: true` or `optional: true`. With `--schema` the key must exist on the edge type of the connection, the operation must select it, and the type must match the schema (`Int` is `:int`, `Float` is `:num`).
- Edges of a connection nested inside an association give no values. `cursor` and `sequenceNumber` of the edge are not read.

## Node reads

### Reading after a declared event

Declare one shared change subscription beside the model reads. The application registers its GraphQL document through the existing operations argument.

```ruby
model Notebook do
  attribute :title, :string
  scope :node, node: :one do
    parameter :id, :id
    query "NotebookNode"
    variables id: :id
  end
  changes "RecordEvents", from: "recordEvents.entries", area: "topic", kind: "action", id: "recordId"
  changed_by "NOTEBOOK", via: :node, removes_on: ["ERASED", "HIDDEN"]
end
```

`changes` names the registered subscription and the path to its array of changes.
`area`, `kind` and `id` name response paths within each entry.
Declare `changes` once across the input files. Every `changed_by` declaration uses that subscription.
Missing declarations, duplicate subscriptions and empty paths produce generator errors.

A node read uses the declared change ID. The `via` scope declared with `node: :one` has 1 parameter.
A singleton read needs no parameter: `changed_by "TOTALS", via: :current`.
`via: :observed` rereads every observed read of the model, parameterized ones included, with the same reread that follows a reconnect. It takes no parameter and no `removes_on`, and the model needs a remote read. A read that nobody observes is not requested.

```ruby
model WalletTransaction do
  scope :by_kind, kind: :enum do ... end
  changed_by "WALLET", via: :observed
end
```

Every applied event reaches the app as a typed model event: `area`, `kind`, `id`, the row before and the row after. The row after is the one the same reread stored, with no second request. A removal has no row after. Events of one model arrive in the order the core applied them. `ModelEvent` holds these fields and `ModelEventItem` is `event`, `missed` or `failed`. The generated `events` property reads `watchEvents()` in Swift and `modelEvents()` in Kotlin.

```swift
for await item in Message.events {
  switch item {
  case .event(let event): show(event.before, event.after)
  case .missed(let count): resync(count)
  case .failed(let error): report(error)
  }
}
```

```kotlin
Message.events.collect { item -> when (item) { is ModelEventItem.Event -> show(item.event.before, item.event.after); is ModelEventItem.Missed -> resync(item.count); is ModelEventItem.Failed -> report(item.error) } }
```

The queue of a reader holds 256 events. A reader that is slower reads `missed` with the number of dropped events, and never delays the next event of other readers. A failed row read publishes no event and raises the rejected count of the subscription. A failure of the stream itself arrives as the last item `failed`. Cancelling the stream releases the shared subscription after `live_linger`. The stream needs no notice model.
Area and removal values are strings that belong to the application.
An area can repeat for node reads of several models. Each receiver follows its own declaration.
`removes_on` requires a `via` scope declared with `node: :one`, whatever its name. Matching kinds remove rows. Other kinds reread them.
Missing entry fields and undeclared areas are rejected.

```swift
try await DB.open(endpoint: endpoint, modelsJSON: modelsJSON, operations: [RecordEvents.self])
try DB.listen()
```

```kotlin
DBlayer.open(context, endpoint = endpoint, modelsJson = modelsJSON, operations = listOf(RecordEvents), uploadNotification = notification)
DBlayer.listen()
```

The generated Swift and Kotlin model declarations carry identical subscription data.
`subscribe` accepts `apply` to select an action for a subscription response.
Without `apply`, the event follows the normal row upsert behavior.
`matches` compares response paths against exact strings supplied by the application. Nonmatching events do nothing.
`effect: :ephemeral` copies the response paths in `fields` into declared ephemeral attributes without changing stored columns.
`effect: :reread, via: :node` rereads the declared node with the event ID. A singleton read needs no ID.
`effect: :upsert` writes the row, with `values:` for ephemeral attributes and `delete_if:` for a payload path that deletes the row. `effect: :delete` deletes it. `effect: :ignore` discards the event. Unknown effects, keys that do not fit the effect, empty ephemeral fields and missing reread destinations produce generator errors.

```ruby
subscribe "PresenceSignal", from: "presenceSignal", id: "memberId", apply: { effect: :ephemeral, fields: { typing: "active" }, matches: { kind: "ACTIVE" } }
subscribe "RecordSignal", from: "recordSignal", id: "recordId", apply: { effect: :reread, via: :node }
```

Removal sends no mutation. It closes queued writes without sending or rollback and destroys children declared with `dependent: :destroy`.
Observers receive 1 change for the removal.
Observing a model opens the shared subscription. The last cancel releases it after `live_linger`.
After a reconnect, only observed reads whose answers have expired are reread.
Node reads and remote scopes default to `5.minutes` when `freshness` is omitted.
A `preload` scope loads on its own after open with a session, sign-in, reset and network return. It starts no request while offline or signed out, and none while its answer is fresh.
Expiration alone starts no request. A later reconnect or read checks freshness again.
Received events reread immediately. The first `Ready` does not trigger an extra reread.
The full exchange is described in [Cable protocol](cable-protocol.md).

GraphQL `node(id:)` reads one entity of any type that implements `Node`. A scope declared with `node: :one` reads it, a related list reads it through `response from:`.

```ruby
model Group do
  attribute :title, :string
  attribute :shareUrl, :string, null: true
  scope :node, node: :one do
    parameter :id, :id
    query "GroupNode"
    variables id: :id
  end
  scope :nodes, node: :many do
    query "GroupsByIds"
    response shape: :list
  end
end

model Note do
  attribute :groupId, :id
  attribute :media, NoteMedia, null: true do
    attribute :kind, :string, null: true
    attribute :fileUrl, :string, null: true
  end
  scope :transcode_status, node: :one do
    parameter :id, :id
    query "NoteTranscodeStatus"
    variables id: :id
  end
  scope :group_media do
    parameter :groupId, :id
    where groupId: :groupId
    query "GroupMedia"
    response from: "node.messages"
    paginate size: 30, direction: :forward
  end
end
```

```graphql
query GroupNode($id: ID!) { node(id: $id) { ... on Group { id title shareUrl } } }
query NoteTranscodeStatus($id: ID!) { node(id: $id) { ... on Note { id groupId media { kind fileUrl } } } }
query GroupMedia($groupId: ID!, $first: Int, $after: Cursor) { node(id: $groupId) { ... on Group { messages(first: $first, after: $after) { edges { node { ...NoteFragment } } pageInfo { hasNextPage endCursor } } } } }
query GroupsByIds($ids: [ID!]!) { nodes(ids: $ids) { ... on Group { id title shareUrl } } }
```

A paged list of the entities under a node, such as the members of a group, is a scope of the listed model with a parameter that has no `where`. It has no local `order`: the rows keep the order of the server across pages, a restart and a refresh of a row by another list. When the server orders the list by a param, such as `order: GroupMemberOrder` (`JOINED` or `OWNER_FIRST`), the parameter is an enum parameter: each pair of group and order has its own window, its own members, its own cursor and its own total, and a cursor is never sent with another order.

```ruby
model User do
  scope :group_members do
    parameter :groupId, :string
    parameter :order, GroupMemberOrder, default: "JOINED"
    query "GroupMembers"
    response from: "node.members"
    paginate size: 30, direction: :forward
    freshness expires_in: 5.minutes
    total from: "node.membersCount"
  end
end
```

```graphql
query GroupMembers($groupId: ID!, $order: GroupMemberOrder = JOINED, $first: Int, $after: Cursor) { node(id: $groupId) { ... on Group { membersCount members(first: $first, after: $after, order: $order) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }
```

```swift
let members = LiveList(try User.groupMembers(groupId))                      // JOINED, members.rows, members.snapshot.hasMore
let owners = LiveList(try User.groupMembers(groupId, order: .ownerFirst))   // its own window and cursor
try await User.groupMembers(groupId).loadMore()                         // .loaded(hasMore: true) while another page exists
```

```kotlin
val members = User.groupMembers(groupId)                                  // JOINED
val owners = User.groupMembers(groupId, order = GroupMemberOrder.OwnerFirst)  // its own window and cursor
members.loadMore()                                 // LoadMoreResult.Loaded(true) while another page exists
```

`total` is set only when the connection selects `totalCount`. Without it `total` stays nil and `hasMore` comes from `pageInfo`.

`total from: "node.membersCount"` takes `total` from a field beside the connection: the path starts at the first field of the operation, like `response from:`. The group size `Group.membersCount` counts every member, `totalCount` of the connection counts only the members the viewer can see. The document must select the field and the schema check refuses a path that is not an `Int`. Each page that carries the field sets `total`, a page without it keeps the last value, and the value survives a restart. The line needs `paginate`.

```swift
let snapshots = try await User.groupMembers(groupId).snapshots.stream()   // snapshot.total is Group.membersCount, 53 for a group of 53
```

```kotlin
val total = User.groupMembers(groupId).snapshots.flow.first { it.total != null }.total   // Group.membersCount
```

| Line | Meaning |
| ---- | ------- |
| `scope :node, node: :one` with `parameter :id, :id` and `query "Operation"` | one entity by id, generates `Model.node(id)`. The id goes into `$id` |
| `scope :name, node: :one` | a second read of the same model, such as a small status read, generates `Model.name(id)` |
| `variables momentId: :id` | the id goes into `$momentId` |
| `freshness expires_in: 1.minute, empty_in: 10.seconds` | how long an answer counts as fresh |
| `scope :nodes, node: :many` with `response shape: :list` | many entities in one request, generates `Model.nodes(ids)`. The operation takes `$ids` |
| `response from: "node.messages"` | a related list under the node: the path starts at `node` and continues inside the `... on` type |

```swift
let group = try await Group.node(groupId)                      // nil when the server has no such group or the viewer cannot see it
let fresh = try await Group.node(groupId, force: true)        // an event named this group: read it again now
let status = try await Note.transcodeStatus(messageId)
let groups = try await Group.nodes(ids)
```

```kotlin
val group = Group.node(groupId)                                // null when the server has no such group or the viewer cannot see it
val fresh = Group.node(groupId, force = true)                 // an event named this group: read it again now
val status = Note.transcodeStatus(messageId)
val groups = Group.nodes(ids)
```

- The answer lands as a normal server row of the model. Only the fields the operation selects change, so a small read leaves the other fields, the merge rules and local fields as they were. A local edit that is not confirmed yet is kept.
- A plain call inside the freshness time of an id reads the local row and sends nothing. `force: true` always asks the server. Each read of a model has its own freshness per id.
- Calls for one id that overlap share one request. Nothing needs to be started or released.
- The result is nil when the answer is `null`, has another type than the `... on` fragment (an empty object), or the id has no entity. Nothing is written then and the stored row stays as it was.
- `nodes` skips null and empty entries, preserves repeated IDs in response order, and writes each row once. Every call asks the server.
- Repeated rows merge selected fields before storage. The last supplied value wins, and root fields take precedence over nested copies.
- An answer that arrives after the database was reset (a logout or a cache reset of the model) is dropped. Nothing is written, `node` returns nil and `nodes` returns an empty list.
- `Model.entity(id)` and `Model.find(id)` stay the way to watch a row. A node read fills them.
- The generator stops with file and line when a field is selected under `node` outside `... on`, when the `... on` type is not an implementation of the interface, when one `node` selects two types, or when the model has an attribute that the `... on` type lacks. A selection outside `... on` would give a row for a foreign type, so it is an error.

## Validation

`dblayer-gen` reports mistakes with file and line (`user.dbl:12: unknown key `touch` for `has_many``). The same checks run in the core when a database is opened: unknown models, missing indexes on foreign keys, duplicate names and invalid combinations.

`dblayer-gen spec models/*.dbl` prints the model specification. `dblayer-gen explain models/*.dbl` creates an empty in-memory database and prints DDL, scope SQL and SQLite query plans. Plans use sample parameters and have no production statistics.

## Check against the server schema

`dblayer-gen --schema schema.graphql models/*.dbl` checks the declarations against the GraphQL schema of the server. It prints nothing but notes when every check passes, and exits with code 1 and a file and line for each mismatch.

For each scope with `query`, the check finds the named operation, takes its first field (the alias when there is one) and follows `response from:` from there, through connections (`nodes` or `edges { node }`) to the type that holds the rows. Operations and fragments are read from the `queries`, `mutations`, `subscriptions` and `fragments` directories next to the schema file, or from every `--operations <file or directory>` you pass. A spread of a fragment that no document declares is an error.

`from:` names the key of the response, which is the alias when the operation gives one (`rewardTier: kind` is read with `from: "rewardTier"`). The check follows each key through the selection of the operation, into inline fragments and named fragments, and tests the schema field behind it. The key of a field that the operation does not select is taken as the schema field name. A union or interface field must select `__typename` somewhere in its own selection, also inside a fragment. Without operation documents the operation name is read as the schema field.

### Paths into the response

A path that a declaration writes names keys of the response, so the check follows it through the selection of the operation that sends it: the aliases of the document, spreads of named fragments, inline fragments (with or without `... on`), union branches and the `edges { node }` or `nodes` of a connection. The path starts at the top-level key of the operation. These declarations are followed:

| Declaration | Starts at | Follows |
| --- | --- | --- |
| `response from:` of a scope, of a model `operation` and of a standalone operation | top of the operation | the path |
| `upsert Model, from:` | top of the operation | the path |
| `returns` attributes of an `operation` block | the first part of `response from:`, or the first field | each key, and the keys of nested objects and lists |
| `total from:` | top of the operation | the path, an alias of the document resolves to its schema field |
| `paginate ..., from:` | the connection | the path |
| `from:` and `id:` of `subscribe` and `changes` | top of the subscription, or the row | the path |

A path the document does not select is an error with the file and line of the declaration. The message names the declaration, the path, the segment where the path ends and the keys the document has at that place:

```
group.dbl:125: response `groupGroupLeave.group`: operation `GroupGroupLeave` has no `group` under `groupGroupLeave`, it has: removedGroupId
```

A path may not be empty and may not have an empty segment, with or without `--schema`. Operation documents come only with `--schema`. Without them the check stops at the shape of the path. A default response path (the first field of the operation) is not followed, because the declaration did not write it. `refetch`, `optimistic` and `match_by` name scopes, models and row attributes and are not paths into the response.

Every attribute of the model, every `from:` path, every `id_from:` and every attribute of a nested type must exist on that type. The check reports:

- an attribute or path the schema does not have;
- a list declared as a single value, or the reverse;
- a nullable schema field declared without `null: true`, `optional: true` or `default:`;
- a scalar, enum or object that does not fit the schema type, and an `enum` value the schema enum lacks;
- a scope that paginates, lists or returns one row against a schema field of another shape;
- an association `from:` path of the wrong shape.

Each `parameter` declared inside a model operation is checked against the mutation that sends it. With `wrap :input`, the check reads the input type of the wrapped variable. Without it, the check reads the variables of the operation. The check needs the mutation in the operation documents and reports:

- a variable that `variables` binds to a parameter but the input type or the operation does not have;
- a parameter whose type does not fit the schema type: `:string` fits `String`, `ID` or a scalar mapped to `:string` or `:date`, `:id` fits `ID`, `Int`, `String` or a scalar mapped to `:string`, `:int` or `:long`, `:int` fits `Int` or a scalar mapped to `:int` or `:long`, `:long` fits a scalar mapped to `:long`, `:num` fits `Float`, `Int` or a scalar mapped to `:num`, `:bool` fits `Boolean`, `[:id]` fits a list of `ID`, `Int` or `String`, and the name of an input object or enum (`WidgetKind`) fits that type;
- a parameter with `null: true` that the schema requires (`String!` without a default).

A list type (`[WidgetInput]`) fits a list of that type. `null_items: true` allows nil items and needs a schema item without `!`. `null: true` makes the list optional and needs a schema variable without `!`.

A `parameter` inside a model operation takes the same types as an operation without a model, including the name of a schema enum or input object. The call gets a typed parameter, and an `optimistic` line can write an enum parameter into an enum attribute of the model. Without `--schema` the generator stops and names the type.

For file uploads, name each operation and declare it with `operation :name, upload: true`. Each operation declares its own `response`, `upsert` and `parameter` lines. The sent mutation selects that declaration for the same response processing used by other actions. The generator emits `<operationName>Variables` on Swift and Kotlin when the operation has parameters. See [uploads](files.md#uploads).

```ruby
operation :answer_connection do
  mutation "ConnectionUpdate"
  parameter :decision, ConnectionAction
  parameter :outcome, ConnectionStatus
  variables userId: :id, action: :decision
  optimistic connection: :outcome
  wrap "input"
end
```

```swift
try await user.answerConnection(decision: .accept, outcome: .connected)
```

```kotlin
user.answerConnection(decision = ConnectionAction.Accept, outcome = ConnectionStatus.Connected)
```

Attributes marked `local: true`, `:file` attributes and models with `storage: :local` are skipped. Schema fields that the model leaves out are listed as notes, because a model may select a subset.

## Access level

`dblayer-gen --internal swift ...` emits models and their generated value types without `public`, for an app that generates them into its own module.

## Input objects of the schema

Standalone operations require named documents when `--schema` is set. The check validates operation kinds, argument types, required inputs and schema defaults. Input objects use `parameter :input, StickerInput` (see [server sync](server-sync.md)):

`dblayer-gen --schema schema.graphql swift models/*.dbl` checks the models as above and also writes the input objects. The same flag does both, so the schema file must satisfy the check.

A schema input object with `@oneOf` sets exactly one field, so it is an enum and not a struct. The case holds the value of its field. Both platforms send one key. Recursive input objects and recursive choices preserve nested values on both platforms.

```swift
let props: StickerPropsInput = .emoji(StickerEmojiInput(code: "fire"))   // {"emoji":{"code":"fire"}}
```

```kotlin
val props: StickerPropsInput = StickerPropsInput.Emoji(StickerEmojiInput(code = "fire"))   // {"emoji":{"code":"fire"}}
```

The generator writes every input object that an operation names, and every input object those fields use, once each, after the models. Input fields that match Swift or Kotlin keywords use escaped identifiers and retain their original GraphQL keys. A name that the schema does not have, a name that is neither an input object nor an enum, or an operation that names one without a schema file stops the run with code 1 and a message.

| Schema field | Swift | Kotlin |
| ------------ | ----- | ------ |
| `String`, `ID`, a scalar mapped to `:string` or `:date` | `String` | `String` |
| a scalar mapped to `:long` | `Int` | `Long` |
| an enum | its enum with an `unknown` case | its sealed class with `Unknown` |
| the scalar named `JSON` | `JSONValue` | `JsonElement` |
| `Int`, `Float`, `Boolean` | `Int`, `Double`, `Bool` | `Long`, `Double`, `Boolean` |
| `[T!]!`, `[T]` | `[T]`, `GraphQLNullable<[T?]>` | `List<T>`, `GraphQLNullable<List<T?>>` |
| another input object | its struct | its data class |
| an input object with `@oneOf` | an enum with one case per field | a sealed interface with one class per field |
| a field without `!` | `.omitted`, `.null`, `.value(value)` | `Omitted`, `Null`, `Value(value)` |
| a required field with a schema default | optional, omitted when nil | nullable, omitted when null |

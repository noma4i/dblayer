# Describing models

## Media library marker and tail

```ruby
scope :media do
  parameter :chatId, :id
  query "chatMedia"
  response from: "node.messages"
  paginate size: 100, direction: :forward
  freshness expires_in: 5.minutes
  marker "chatMediaState", from: "node.mediaLibrary", fields: [:revision, :visualCount, :audioCount, :giftCount], sequence: :latestSequence
  tail "chatMedia", after: :afterSequence
end
```

Declare `marker` and `tail` together inside a remote scope with cursor pagination.
`from` specifies the selected marker response object. Fields and sequence have the non-null type `Int` or `Int64`.
`Int64` accepts a decimal string in the server response. SQLite stores numeric values without JSON.
The generator requires a schema, both query documents, response paths and compatible variables.
Scope parameters and the existing `variables` bind variables for both queries.
The core passes `upToSequence` to marker, the declared `after` variable to tail, and standard pagination variables.
The tail query returns the same connection and model fields in ascending sequence order.
`latestSequence` refers to the entire media library. Marker counts refer to the `upToSequence` prefix.
The core confirms a loaded window when the sum of the marker fields other than `revision` equals the number of rows of this scope in that prefix. A scope that filters the library declares only the count of its own rows, for example `fields: [:revision, :audioCount]` for an audio scope.
`revision` comes from `ChatMediaLibrary.revision`, which corresponds to `chats.media_revision`.

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

GraphQL failures expose `extensions` on Swift `DbError.Graphql` and Kotlin `DbException.Graphql`. A refused mutation carries them in the `failure` of its rejected outcome.
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
  _ = try await DB.call("PurchaseCreate", variables: input)
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
    DBlayer.call("PurchaseCreate", input)
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

A local write checks nested values against their declared type. Create, update, actions, local-field setters and upload calls reject a key the type lacks with an unknown-field error that names the path (`media.id`, `replies[1].body`). They reject a value of the wrong type with an invalid-value error at the same path. A `:json` field accepts any value.

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
| `:stable_list` | never: the stored ids keep their places, ids that the server no longer lists are dropped and new ids follow in the order they arrived (`[:id]` or another list of scalars) |
| `{ key: rule, ... }` | `:json`: the object is merged key by key, a key without a rule ignores null, a null object keeps the stored one |

```ruby
attribute :media, MomentMedia, null: true, merge: { fileUrl: :non_empty, width: :positive, transcodeStep: [[:PENDING, :SUBMITTED, :PROGRESSING], [:COMPLETED, :FAILED]], transcodeProgress: :monotonic }
attribute :avatarUrl, :string, null: true, merge: :non_empty
attribute :momentIds, [:id], merge: :stable_list
```

`merge: :stable_list` on the id list of a `references` parent keeps the order the user has seen: a reloaded result shows the same rows in the same places and appends the new ones. The list lives in a column, so the order survives a restart. The work grows with the size of the arriving list.

A local edit that is not confirmed yet is never replaced, with or without a rule. A rule that does not fit the field type rejects the model.

## Local fields

Inside `operation :create`, repeat `local` for different attributes and singleton sources.
Each destination occurs once per operation. Duplicate destinations and incompatible source types fail generation.
An explicit non-null argument wins. An absent source leaves the attribute unset.
Sources share one read snapshot. `operation :update` accepts plain `local` and rejects `local from:`.

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

- Reads see the values: `user.typing` is part of the row.
- A write restarts the timer. When the time is up the value returns to `default`.
- A write and a reset reach readers as a change of the row in a delta of their stream.
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

- `expires_in:` is optional. A row that has not been written for that time is deleted, and readers receive its removal in a delta.
- Every write renews the time: a local `create` or `update`, a subscription event or a response that lands the row. A write of equal values renews it too and sends no change to observers.
- A memory model has no `:file` attributes and no model `operation` blocks, so it has no outbox.
- Use it for search results, prices and other data that is fetched, shown and dropped.

```swift
let online = Projected<Presence>(try scope.stream(Presence.where(Presence.room == "lobby")))
```

```kotlin
val online = Projected(scope.stream(Presence.where(Presence.room eq "lobby")), Presence, screenScope)
```

### Notices in memory

A local scope of a memory model accepts `subscribe` without `query`. A disk model must not declare it.
The subscription starts at the first reader of the scope and is shared between readers. It closes after the last reader leaves.
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
    subscribe "PresenceSignal", from: "presenceSignal", id: "userId", apply: { effect: :ephemeral, fields: { typing: "active" }, matches: { kind: ["ACTIVE"] } }
  end
end
```

```swift
let notices = Projected<Notice>(try scope.stream(Notice.recent))
let interactions = Projected<InteractionNotice>(try scope.stream(InteractionNotice.recent))
```

```kotlin
val notices = Projected(scope.stream(Notice.recent), Notice, screenScope)
val interactions = Projected(scope.stream(InteractionNotice.recent), InteractionNotice, screenScope)
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
let opponent = Projected<User>(try scope.entity(User.self, opponentId))   // opponent.value is the user
```

```kotlin
// group { id opponent { id } }       -> Group.opponent is the user
// group { id opponent { id } }       -> opponent: null, Group.opponent is null, the user row stays
// group { id title }                 -> opponent not selected, Group.opponent keeps its last user
val opponent = Projected(scope.entity(User, opponentId), User, screenScope)   // opponent.value is the user
```

- `dependent: :destroy` removes children in the same commit as the parent.
- `touch: true` bumps the parent's revision when a child changes, so observers of the parent refresh. See [partial child updates](parent-fields.md) for `sets` field presence.
- `sets: { lastNoteId: :id, lastNoteAt: :createdAt }` on a `belongs_to` writes the child fields to the parent fields in the same commit as every write of the child: a local `create`, an event, a page landing, the answer of a mutation, a replace, `land`. Destroying the child that the parent points at (the first parent field set from the child `id`), locally or from the server, moves the parent fields to the newest remaining child by the `newer` child fields, or to null when none remains. `newer: { sequenceNumber: :lastSequenceNumber, createdAt: :lastNoteAt }` writes only when the child is newer than what the parent holds. The pairs are a tuple in the order written: the first pair where both sides have a value and differ decides, a child with the id the parent already points at always refreshes, a temporary id outranks a confirmed one (client and server clocks never order them), and an empty parent value counts as older. The same tuple guards the parent row itself: a row of the parent that arrives from a list, an event or a mutation answer never moves these parent fields backward. An event for a child whose parent is not stored (a group outside the loaded pages) reloads the watched lists of the parent model. A discard or a refusal gives the parent values back, the swap of a temporary child id moves a parent field that points at it. Both options name fields that exist on their side, otherwise the model is rejected.
- Foreign-key indexes are inferred from associations. Declare the foreign-key attribute, without adding its index again.
- There are no counter caches. Count with a query: `scope.count(...)`.

## Scopes

`scope :name do ... end` declares a named selection. `parameter :ownerId, :id` declares a typed parameter and `where ownerId: :ownerId` binds the column to it. A parameter bound by `where` has the name of its column. Each column takes its own `where` pair. `order field: :asc` sorts rows. Null values come first in `:asc` and last in `:desc`. Use `:asc_nulls_last` or `:desc_nulls_first` to reverse their placement.

Integer comparisons remain exact above 2^53, including comparisons with real numbers in merges and sorted lists.

Scope keys encode each normalized parameter with its type and JSON value. Null and strings always identify distinct windows.
Variable bindings read the original parameters, including cross bindings. Ordinary requests pass remaining parameters, while marker and tail requests select declared variables.
Duration counts that overflow milliseconds are rejected.

When a response row has no value for a column the relation groups by, the param fills it. A response without an id becomes a row keyed by the param.

A `query "Operation"` line connects a scope to the server. A scope that depends on columns binds them: `where userId: :userId` loads and lists the rows whose `userId` equals the parameter. `order createdAt: :desc` sorts the rows locally, otherwise the server order is kept. A row whose param column changes (a message moving to another `groupId`) leaves the list of the old value and joins the head of the list of the new value in the same commit, for relations that list every row of their params. `insert_on: [:create]` on a scope without parameters makes the list take the rows this device creates, at its head; `insert_on: [:event, :response]` makes it take rows from its subscription events and from operation responses.

`where` accepts scalar and enum columns. A param passed as `nil` is a value of its own: `Moment.by_user(nil)` is the unfiltered list and has its own window. A parameter bound to a column gives the row one owner list: a row that belongs to several lists at once (a moment in the default feed and in a vibe feed) needs one scope per list, and the vibe id is a `parameter` without `where`, not a column parameter. A parameter without `where`, such as a search string, is sent as a variable and each value gets its own window: `parameter :query, :string` in `scope :search` generates `Model.search("cats")`.

`parameter :afterSequence, :int` and `parameter :includeRewards, :bool` declare parameters that are sent as a JSON integer or boolean. The call takes `Int` and `Bool` (`Long` and `Boolean` in Kotlin: `:int` is `Long` in every Kotlin output, like the attribute), and the core refuses a text value. Each value has its own window and its own members. The document variable of the same name (or the name on the left of `variables name: :parameter`) must be `Int` or `Boolean`. A scope that pages in ascending order and one that pages in descending order are two scopes: a window key holds the scope, its params and the direction, so their cursors never meet.

Names of attributes, parameters, scopes, associations and operations start with a lowercase letter and do not end with `_`. A scope, association or operation cannot take `id`, `find`, `where`, `save`, `count`, `exists`, `first`, `observe`, `order`, `retry`, `discard` or a generated member (`sync`, `uploads`, `modelName`, `layout`, `wireLayout`, `singleton`, `rowRevision`, `estimatedBytes`): the generator and `Database.open` read one list in the core. The generator also reads its own Swift and Kotlin output and refuses a model whose names print one member twice, such as an operation `upload_clip` beside a scope `uploadClipVariables`. The generator refuses, with the model and the function, a name that clashes with generated code: `account` in every function that takes an account, `id` in a `*Edge` read, `self`, `sync`, `uploads`, `modelName`, `layout`, `wireLayout` and `singleton` as fields, and the names of calls the generated code writes bare (`jsonValue`, `mapOf`, `onAccount` and the like). A model, type or enum named like a type the generated code imports (`Query`, `Condition`, `Order`, `Column`) and a GraphQL input field with such a name are refused too; `read`, `put` and `act` stay free names. Rename the attribute or parameter; the server name stays in `from:` or `variables`.

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

**Stable order.** A group thread must not reorder rows the user has seen. `stable_order: true` on a scope with `query`, `where` columns and an `order` (and no parameter without `where`) gives each row a place in the list when it joins, and later changes of the `order` values never move it. A message that the server confirms with a lower `sequenceNumber`, the answer that replaces its temporary id, an event that edits the number and a reloaded first page leave the rows where the user saw them. A row from the server (a page, an event) that is not in the list takes the place its `order` values give it among the rows already listed, so a page of older messages lands before the rows shown. A row this device creates goes after the last row. The places live in the cache: they survive a restart, and a session reset drops them, so the pages that load afterwards come in the order of the server. Finding the place of a row costs work that does not grow with the size of the list.

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

`insert_on: [:event, :response]` adds event and operation response rows to matching loaded windows, including cached rows absent from those windows.
Add `:node` to include node rereads started by `changed_by`. A manual node read does not acquire this insertion intent.
Every parameter needs `where`, `membership`, or a `greater_than` match. Hidden server parameters still prohibit automatic insertion.
Use `membership list: { contains: :lists }` when a row belongs to several lists.
Use `membership list: { equals: :status }` for scalar membership.
Rules combine with AND. The parameter type must match the scalar field or array element, including the enum name.
Use `matches` when the scope is a filter on row fields, such as a media list. `matches mediaKind: { in: ["IMAGE", "VIDEO"] }` admits a row whose field is in the set, `matches kind: { equals: "GIFT" }` admits a row whose field equals the literal, and `matches sequenceNumber: { greater_than: :afterSequence }` admits a row whose `:int` field is above the scope parameter. A row with a missing or null field does not match.
Each field takes one test, literals are strings, numbers or booleans of the field type, and `greater_than` needs an `:int` attribute and an `:int` parameter that no other line uses. A loaded window of the scope takes the row when the row passes every `matches` test for that window's parameters, so one row can join the window `afterSequence: 10` and not the window `afterSequence: 20`.
`matches` rules describe constant `variables`. A scope with constant `variables` and no `matches` or `membership` rule stays out of automatic insertion.
A member that stops matching after a row change leaves the window, like a member that loses its list.
Membership rules require `query`. Local scopes read their `where` columns.
When a nullable `where` column has a default, an explicit null selects its own window.
A row from an event, or from a node read started by `changed_by`, must carry the membership field after merging the stored row; without it the write is refused as invalid data. An operation response row without the field is written and enters no list. A `null` removes the field and counts as missing. An empty array removes previous membership. When a scope with membership lists `:response` in `insert_on`, generation fails for an operation that lands a row whose response selection does not carry the membership field under its path (`response from:`, `upsert ... from:` or the default root).
Missing members join the head once. Existing members keep their places. Unloaded windows are not created.
Membership and row changes share one commit. Pages retain their server membership, cursors and revision protection.
When an event joins an active manual node request, that request also inserts the row into matching loaded windows.

```ruby
scope :list, insert_on: [:event, :response, :node] do
  parameter :list, ChatList
  membership list: { contains: :lists }
  query "Chats"
end
```

```ruby
scope :audio_since, insert_on: [:event, :response, :node] do
  parameter :chatId, :id
  parameter :afterSequence, :int
  where chatId: :chatId
  matches mediaKind: { equals: "AUDIO" }, sequenceNumber: { greater_than: :afterSequence }
  order sequenceNumber: :asc
  query "ChatMedia"
  variables mediaKinds: ["AUDIO"], includeGifts: false
  response from: "node.messages"
  paginate size: 50, direction: :forward
end
```

```swift
let audio = try Message.audioSince(chatId, afterSequence: 120)
```

```kotlin
val audio = Message.audioSince(chatId, afterSequence = 120)
```

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
| `preload`, `preload channel: ["lobby", "stage"]`, `preload vibeId: { from: "Vibe.list" }, limit: 40` | Automatically after session, reset, network return and leaving power saving. For `from`, a positive `limit` is required. The limit applies to unique windows of the parameter product. Parameters follow declaration order, ids follow the source scope. SQLite reads a bounded prefix, and source changes add incoming windows and cancel unneeded requests. Fresh windows are not requested again, errors go to the snapshot | no preload |
| `scope :chat_members, members_complete: true do` | Automatic page chain, without `max_pages`. `settled` with `hasMore == false` only after the final `hasNextPage == false`. Full membership is stored in SQLite. Multiple readers share the request, the last cancels it, the next continues the cursor. Error with partial rows: Loaded + error, the list is not complete | ordinary paged loading |
| `refresh_on_foreground: true` (on the `scope` line, needs `query`) | an online idle window with a reader reloads on foreground, whether fresh or stale | foreground alone does not reload the window |
| `stable_order: true` (on the `scope` line) | the list keeps the order the rows were shown in, see [stable order](#scopes) | sorted by the `order` values on every read |
| `matches mediaKind: { in: ["IMAGE", "VIDEO"] }`, `matches kind: { equals: "GIFT" }`, `matches sequenceNumber: { greater_than: :afterSequence }` | row fields the scope admits for `insert_on`: a literal set, a literal, or a row field above a scope parameter, combined with `where` and `membership` by AND | every row of the `where` columns |
| `variables statusFilter: :status, includeRewards: false` | name the GraphQL variables: a symbol takes the value of a scope parameter, any other value is sent as written | every parameter under its own name |
| `edge do attribute :impressionUuid, :string, null: true end` | values of the connection edge that carried the row, stored for this scope, see [edge values](#edge-values) | none |
| `subscribe "Subscription", apply: { ... }` | keep the scope current with a subscription. Write the line again for each subscription. `debounce: 300.milliseconds` merges bursts: the model waits for the longest `debounce:` of its subscriptions | none |

Freshness starts when the server response arrives, not when the database stores it.

`loadAll()` uses the window freshness and the pages already stored, then reads the remaining pages. `loadAll(force: true)` (Kotlin `force = true`) reloads the first page even when the window is fresh, then reads the remaining pages. Concurrent calls for the same window can share one request in flight. Offline, the load waits for the network.

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
let vibe = try await Moment.vibeFeedEdge(vibeId, of: moment.id, in: scope)   // `in scope_: AccountScope? = nil`
edge?.impressionUuid                                      // String?
```

```kotlin
val edge = Moment.feedEdge(moment.id)                     // MomentFeedEdge?, suspend
val vibe = Moment.vibeFeedEdge(vibeId, moment.id, account = scope)   // `account: AccountScope? = null`
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
let edge = try await Moment.vibeFeedEdge(vibeId, of: moment.id)
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
A model removes its own rows when a row of another model lands with `removed_by Message, match: { chatId: :chatId, userId: :authorId }`. The rows of this model whose fields equal the listed fields of the new row go in the same commit, whether the new row comes from a local write, an answer or an event. The generator refuses an unknown model, an unknown field and fields of another type.

Declare `changes` once across the input files. Every `changed_by` declaration uses that subscription.
Missing declarations, duplicate subscriptions and empty paths produce generator errors.

A node read uses the declared change ID. The `via` scope declared with `node: :one` has 1 parameter.
A singleton read needs no parameter: `changed_by "TOTALS", via: :current`.
`via: :observed` rereads every observed read of the model, parameterized ones included, with the same reread that follows a reconnect. It takes no parameter and no `removes_on`, and the model needs a remote read. A read that nobody observes is not requested.

```ruby
model WalletTransaction do
  scope :by_kind do
    parameter :kind, :string
    query "WalletTransactions"
    variables kind: :kind
  end
  changes "RecordEvents", from: "recordEvents.entries", area: "topic", kind: "action", id: "recordId"
  changed_by "WALLET", via: :observed
end
```

Every applied event reaches the app as a typed model event: `area`, `kind`, `id`, the row before and the row after the commit of the event. An event that only rereads stores no row itself, so its before and after rows are equal: the reread lands later as a normal load. A removal has no row after. Events of one model arrive in the order the core applied them. `ModelEvent` holds these fields and `ModelEventItem` is `ready`, `event`, `lost`, `gap` or `ended`.

Any model with `changed_by` has an events stream (`scope.events(Model)`), whichever model declares the `changes` source. Opening it prepares the reader. Opening offline waits for connectivity without an app retry or a timer. Other opening failures throw their original `DbError` kind. The first item is `ready` once the source is open: every event after it reaches the stream, also before iteration or collection reads it. Concurrent readers receive their own events. A later opening failure arrives as the final `failed` item with its original error kind. An earlier event is not replayed, because rows before and after are read only while the model has a reader. Another model's reader or an observed read does not prove that your reader exists.

```swift
let events = try scope.events(Message.self)
for await item in events {
  switch item {
  case .ready: startWork()
  case .event(let event): show(event.before, event.after)
  case .lost(let first, let last): resync(last - first + 1)
  case .ended(let cause): report(cause)
  }
}
```

```kotlin
val events = scope.events(Message).events(Message)
events.collect { item -> when (item) { ModelEventItem.Ready -> startWork(); is ModelEventItem.Event -> show(item.event.before, item.event.after); is ModelEventItem.Lost -> resync(item.last - item.first + 1uL); is ModelEventItem.Ended -> report(item.cause) } }
```

The journal of a reader in the core holds 256 values. A slower reader receives `lost` with the range of missed positions before the events that were kept, without delaying other readers. The kept events and the lost positions add up to the events sent. A failed read of the rows before or after fails the whole commit: its events arrive as `lost` and the observed reads of their models become stale. When the subscription input overflows or the connection breaks, the number of missed events is unknown. The reader receives `gap`, and the library rereads observed selections of affected models. A failure of the stream itself arrives as the last item `ended(cause)`. Cancellation or completion releases that reader. Dropping an uncollected stream releases its prepared reader, through automatic reference release on Swift and garbage collection on Kotlin. Cancellation during opening also releases the pending reader. The last reader releases the shared subscription at once, without app cleanup. The stream needs no notice model.
Area and removal values are strings that belong to the application.
An area can repeat on several models, for `via: :observed`, node reads and named reads. One event rereads the read of each of those models and gives each model its own item on `events`. A model declares an area once per form: `via: :node` (or a named read) and `via: :observed` may both name the same area, and the event does one reread for each form and gives the model one item. A repeat of the same form inside one model is rejected.

```ruby
model Chat do
  attribute :title, :string
  scope :node, node: :one do
    parameter :id, :id
    query "ChatNode"
    variables id: :id
  end
  changes "RecordEvents", from: "recordEvents.entries", area: "topic", kind: "action", id: "recordId"
  changed_by "CHAT", via: :node, removes_on: ["DELETED", "ACCESS_REVOKED"]
  changed_by "CHAT", via: :observed
end
```
Full event rows use `payload` paths relative to each item in `changes`:

```ruby
model Chat do
  attribute :title, :string
  attribute :serverRevision, :int, from: "revision"
  server_revision :serverRevision
  attribute :lists, [:string]
  scope :node, node: :one do
    parameter :id, :id
    query "ChatNode"
    variables id: :id
  end
  changes "RecordEvents", from: "recordEvents.entries", area: "topic", kind: "action", id: "recordId"
  changed_by "MESSAGE", via: :node, payload: "chat"
  scope :list, insert_on: [:event, :response, :node] do
    parameter :list, :string
    membership list: { contains: :lists }
    query "ChatList"
    response from: "chats", shape: :list
  end
end

model Message do
  attribute :chatId, :id
  attribute :text, :string
  scope :node, node: :one do
    parameter :id, :id
    query "MessageNode"
    variables id: :id
  end
  changed_by "MESSAGE", via: :node, payload: "message", removes_on: ["DELETED", "ACCESS_REVOKED"]
end
```

Pass the GraphQL schema and subscription documents to the generator.
It checks payload paths, the target model type, id selection and declared fields, including nested objects.
The payload must contain every required model field, including nullable fields. Missing data rejects the event without an HTTP fallback.
`payload` requires a node scope and cannot combine with `id_of`, `parameter` or `via: :observed`.
`server_revision` names one required, non-local integer attribute. The generator rejects other fields and repeated declarations.
Map GraphQL Int64 with `scalar :Int64, as: :long`.
SQLite stores the attribute as an integer, Swift reads a 64-bit `Int`, and Kotlin reads `Long`.
An incoming server revision below the stored value drops the row before writing data or membership.
An equal or greater revision applies. A row without a stored revision accepts its first version.
Payload, node and page responses use this comparison. Optimistic writes retain their existing rules.
Payloads commit before HTTP rereads. A reset rejects events from the old subscription generation.
Terminal kinds in `removes_on` delete the row without requiring a payload.
Both payload declarations remove all MESSAGE node requests. Model events retain their active-reader snapshots and never replay earlier events.

```swift
let events = try scope.events(Chat.self)
let chats = try Chat.list("PRIMARY")
```

```kotlin
val events = scope.events(Chat).events(Chat)
val chats = Chat.list("PRIMARY")
```

No app refetch follows a MESSAGE.
For an identifier-only event, explicitly declare `id_of: { of: Message, field: :chatId }` to reread `Chat.node(chatId)`:

```ruby
model Chat do
  scope :node, node: :one do
    parameter :id, :id
    query "ChatNode"
    variables id: :id
  end
  changes "RecordEvents", from: "recordEvents.entries", area: "topic", kind: "action", id: "recordId"
  changed_by "MESSAGE", via: :node, id_of: { of: Message, field: :chatId }
end

model Message do
  attribute :chatId, :id
  scope :node, node: :one do
    parameter :id, :id
    query "MessageNode"
    variables id: :id
  end
  changed_by "MESSAGE", via: :node, removes_on: ["DELETED"]
end
```

`Message` declares the same area with a node read, and `chatId` is an id attribute. The core rereads the message first, then rereads the chat from the `chatId` the stored message holds. A deleted message gives the `chatId` it had before the removal. When the message is missing, its read failed or it holds no `chatId`, the chat read is skipped and the rejected count rises. `id_of` takes no `removes_on`.
`removes_on` requires a `via` scope declared with `node: :one`, whatever its name. Matching kinds remove rows. Other kinds reread them.
Missing entry fields and undeclared areas are rejected.

```swift
try await DB.open(endpoint: endpoint, modelsJSON: modelsJSON, operations: [RecordEvents.self])
```

```kotlin
DBlayer.open(context, endpoint = endpoint, modelsJson = modelsJSON, operations = listOf(RecordEvents), uploadNotification = notification)
```

The generated Swift and Kotlin model declarations carry identical subscription data.
`subscribe` accepts `apply` to select an action for a subscription response.
Without `apply`, the event follows the normal row upsert behavior.
`matches: { kind: ["TYPING", "RECORDING_AUDIO"] }` accepts any listed string.
`matches: { kind: { not: ["SCREENSHOT"] } }` accepts other strings.
Fields combine with AND. Missing, null and nonstring fields never match, including `not`.
Empty lists, duplicate paths, mixed types and unknown predicate keys fail generation. A single string is invalid.
Filters run before normalization, deletion and writes for every effect.
Declarations with the same operation, variables and account share one incoming stream.
A scope with `variables` sends its subscription with the variables of the open window, and windows with equal variables share one subscription. When such a scope writes `id:`, each entry names a variable of the scope, or generation fails.
All matching row declarations apply in one commit. `ephemeral` values land in that commit. A `reread` marks the read stale after that commit, and the read reloads then.
Model-level subscriptions are read with `scope.events(Model)`, without replay.
`effect: :ephemeral` copies the response paths in `fields` into declared ephemeral attributes without changing stored columns.
`effect: :reread, via: :node` rereads the declared node with the event ID. A singleton read needs no ID.
`effect: :upsert` writes the row, with `values:` for ephemeral attributes, `delete_if:` for a payload path that deletes the row when it is true and `delete_unless:` for a path that deletes the row when it is present and false. `effect: :delete` deletes it. `effect: :ignore` discards the event. Unknown effects, keys that do not fit the effect, empty ephemeral fields and missing reread destinations produce generator errors.

```ruby
subscribe "PresenceSignal", from: "presenceSignal", id: "memberId", apply: { effect: :ephemeral, fields: { typing: "active" }, matches: { kind: ["ACTIVE"] } }
subscribe "RecordSignal", from: "recordSignal", id: "recordId", apply: { effect: :reread, via: :node }
```

Removal sends no mutation. It closes queued writes without sending or rollback and destroys children declared with `dependent: :destroy`.
Observers receive 1 change for the removal.
Observing a model opens the shared subscription. The last cancel releases it.
After a reconnect that lost events, the watched reads of the affected models are reread once, fresh or not. A load after the network returns covers gaps that began before that return. A connection break without a network change triggers a reread.
Node reads and remote scopes default to `5.minutes` when `freshness` is omitted.
A `preload` scope loads on its own after open with a session, sign-in, reset and network return. It starts no request while offline, signed out, or while the device saves power, and none while its answer is fresh. At open the scope first reads its saved window: a window whose saved data is still fresh sends no request. It waits for the end of power saving.
The library reloads every `preload` scope when the window of the scope is stale. There is no call to reload all scopes at once.
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
let members = Projected<User>(try scope.stream(User.groupMembers(groupId)))                      // JOINED, members.rows, members.state?.hasMore
let owners = Projected<User>(try scope.stream(User.groupMembers(groupId, order: .ownerFirst)))   // its own window and cursor
try await User.groupMembers(groupId).loadMore()                         // .loaded(hasMore: true) while another page exists
```

```kotlin
val members = Projected(scope.stream(User.groupMembers(groupId)), User, screenScope)                                  // JOINED
val owners = Projected(scope.stream(User.groupMembers(groupId, order = GroupMemberOrder.OwnerFirst)), User, screenScope)  // its own window and cursor
val page = User.groupMembers(groupId)
page.loadMore()                                    // LoadMoreResult.Loaded(true) while another page exists
```

`total` is set only when the connection selects `totalCount`. Without it `total` stays nil and `hasMore` comes from `pageInfo`.

`total from: "node.membersCount"` takes `total` from a field beside the connection: the path starts at the first field of the operation, like `response from:`. The group size `Group.membersCount` counts every member, `totalCount` of the connection counts only the members the viewer can see. The document must select the field and the schema check refuses a path that is not an `Int`. Each page that carries the field sets `total`, a page without it keeps the last value, and the value survives a restart. The line needs `paginate`.

```swift
let members = Projected<User>(try scope.stream(User.groupMembers(groupId)))   // members.state?.total is Group.membersCount, 53 for a group of 53
```

```kotlin
val members = Projected(scope.stream(User.groupMembers(groupId)), User, screenScope)   // members.projection.value.state?.total is Group.membersCount
```

| Line | Meaning |
| ---- | ------- |
| `scope :node, node: :one` with `parameter :id, :id` and `query "Operation"` | one entity by id, generates `Model.node(id)`. The id goes into `$id` |
| `scope :name, node: :one` | a second read of the same model, such as a small status read, generates `Model.name(id)` |
| `variables momentId: :id` | the id goes into `$momentId` |
| `freshness expires_in: 1.minute, empty_in: 10.seconds` | how long an answer counts as fresh |
| `scope :nodes, node: :many` with `response shape: :list` | many entities in one request, generates `Model.nodes(ids)`. The operation takes `$ids` |
| `response from: "node.messages"` | a related list under the node: the path starts at `node` and continues inside the `... on` type |

Freshness starts when the server response arrives, not when the database stores it.

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
- A plain `node(id)` call inside the freshness time of that id reads the local row and sends nothing. `force: true` always asks the server. Freshness per id applies only to `node(id)`.
- Calls for one id that overlap share one request. Nothing needs to be started or released.
- The result is nil when the answer is `null`, has another type than the `... on` fragment (an empty object), or the id has no entity. Nothing is written then and the stored row stays as it was.
- `nodes(ids)` returns the rows in the order of the call. A repeated ID in the call gives the row again. An ID without a row in the answer is left out. Null and empty entries of the answer are skipped, and each row is written once. Every `nodes` call asks the server.
- Repeated rows merge selected fields before storage. The last supplied value wins, and root fields take precedence over nested copies.
- A read waiting across a session reset ends with `NoSession`. Nothing is written.
- `scope.entity(Model, id)` is the way to watch a row. A node read fills it.
- The generator stops with file and line when a field is selected under `node` outside `... on`, when the `... on` type is not an implementation of the interface, when one `node` selects two types, or when the model has an attribute that the `... on` type lacks. A selection outside `... on` would give a row for a foreign type, so it is an error.

## Waiting time of a queued operation

Every operation that reaches the server waits in the durable queue while the network is away. An option on the `operation` line limits that wait.

| Line | Meaning | Default |
| ---- | ------- | ------- |
| `operation :typing, expires_after: 30 do` | whole seconds a queued operation may wait before it is sent. When the library picks it after that time, it rolls the optimistic change back and ends the operation `rejected` with the failure kind `expired` | none: the operation waits as long as needed |

## Effects of an operation before the answer

An operation that waits for the server shows its change at once and takes it back when the server refuses. Four effects exist, one per operation, and no others: `removes_receiver`, `copies_receiver_to`, `joins_scope` and `leaves_scope`. Write `optimistic false` next to them.

```ruby
model Person do
  attribute :name, :string
  scope :club_members do
    parameter :clubId, :string
    parameter :order, :string
    query "ClubMembers"
    response from: "node.members"
    paginate size: 30, direction: :forward
  end
end

model Club do
  attribute :name, :string
  operation :leave do
    mutation "LeaveClub"
    variables clubId: :id
    optimistic false
    removes_receiver
    response :ignore
  end
  operation :add_member do
    mutation "AddClubMember"
    parameter :userIds, [:id]
    variables clubId: :id, userIds: :userIds
    optimistic false
    joins_scope :club_members, model: Person, rows: :userIds, with: { clubId: :id }, order: :order, orders: ["JOINED", "OWNER_FIRST"]
    response from: "addClubMember"
  end
end
```

- `removes_receiver` takes the row out of every list before the answer. A refusal puts it back at its place.
- `copies_receiver_to :target, field: :chatId, set: { isForwarded: true, authorId: { of: User, field: :id }, originId: { owner: :authorId } }` adds a copy of the row with `chatId` set to the argument `target`. `set:` puts a literal, a field of the copied row (`owner:`) or a field of the singleton row of a model (`of:`) on the copy. The answer row replaces the copy, a refusal removes it.
- `joins_scope` adds the rows named by `rows:` to the end of the scope list, and `leaves_scope` takes them out. A row that is not cached is skipped by both; the answer and the next read of the list bring it. `orders:` lists every value of the order parameter, so all sort orders of the list change together. A refusal restores the ids and the order of every list.
- The call returns after the answer. While the network is away it waits in the queue and the request goes out when the network returns. A refusal restores the lists.
- The core reads the answer of an operation call by the `returns` block of the `.dbl`, which the generator writes into the specs next to the models (`modelsJSON`): an `:id` is text, `:int` and `:num` are numbers (a number sent as text becomes a number), an absent field is `null`, a field outside the block is dropped, and a value of another type fails the call with `Invalid`. An operation without a `returns` block is declared raw: its answer arrives as the server sent it. A call of an operation that is not in the `.dbl` fails with `Invalid` before any request. Generated types decode the normalized form and refuse any other.

```swift
let left = try await club.leave()
let added = try await club.addMember(userIds: ["u4"])
```

```kotlin
val left = club.leave()
val added = club.addMember(userIds = listOf("u4"))
```

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

A path may not be empty and may not have an empty segment, with or without `--schema`. Operation documents come only with `--schema`. Without them the check stops at the shape of the path. A default response path (the first field of the operation) is not followed, because the declaration did not write it. `refetch`, `optimistic`, `match_by` and the effect lines name scopes, models and row attributes and are not paths into the response.

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

A top-level `operation` outside a model takes only its name and block. Named options produce a generator error naming the option.

For file uploads, name each operation and declare it with `operation :name, upload: true`. Each operation declares its own `response`, `upsert` and `parameter` lines. The sent mutation selects that declaration for the same response processing used by other actions. The generator emits `<operationName>Variables` on Swift and Kotlin when the operation has parameters. The steps of the upload (`createBlob`, multipart) are declared once per project in `uploads do ... end`, see [uploads](files.md#uploads).

An upload operation can declare that the server works on the file after the transfer: `processing "status", done: ["READY"], progress: "percent"`. The first argument is the field of the model that holds the state, `done` lists its terminal values and `progress` names the field that holds the progress. While the field holds another value, the upload entry of the action carries the processing and its progress. A new value of the row, from an answer or an event, updates it. A terminal value removes it. A path or a `done` value that the model does not declare fails the generator with the file and line.

```ruby
operation :answer_connection do
  mutation "ConnectionUpdate"
  parameter :decision, ConnectionAction
  variables userId: :id, action: :decision
  optimistic connection: { from: :decision, map: { ACCEPT: "CONNECTED", REJECT: "NEVER_MET", REMOVE: "NEVER_MET", REQUEST: "SENT_PENDING" } }
  wrap "input"
end
```

`optimistic attribute: { from: :parameter, map: { VALUE: "literal" } }` writes the literal that belongs to the value of an enum parameter, so the call carries no second parameter for the outcome. The map names every value of the schema enum: a missing value or a value the enum lacks stops the run. A refused mutation restores the attribute like any other optimistic change.

```swift
try await user.answerConnection(decision: .accept)
```

```kotlin
user.answerConnection(decision = ConnectionAction.Accept)
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

# Describing models

A model is described once in a `.dbl` file. The syntax follows Ruby on Rails: a word, a symbol name, then named options after colons and commas. Order of options does not matter.

## Fields

Declare fields and indexes directly inside `model`. Use `field :name, type: :string` for an explicit storage type.

With `--schema`, `field :name` infers its type and nullability from the matching GraphQL type. Local fields require an explicit type.
The DSL is parsed by Rust. It does not execute Ruby expressions or load Ruby gems.

| Word | Type |
| ---- | ---- |
| `string`, `num`, `int`, `bool`, `date`, `id` | scalars (`id` accepts a string or a number and stores a string, `date` accepts an ISO 8601 text or milliseconds and stores UTC text with milliseconds, `2026-01-03T00:00:05Z` becomes `2026-01-03T00:00:05.000Z`, so text order is time order) |
| `field :name, type: :enum, values: [:a, :b]` | one of the listed values, generated as a typed enum (see [Enums](#enums)). `as: Name` names the type |
| `object`, `array`, `raw` | JSON values |
| `field :name, type: [:string]` | array of scalars |
| `field :name, type: :file` | binary attachment, see [files](files.md) |

Field options:

| Option | Meaning |
| ------ | ------- |
| `null: true` | value may be null |
| `optional: true` | key may be absent in server responses |
| `default: value` | default value, also stored when the server answers null for a field without `null: true` |
| `id_from: :parent` | take the id from a nested parent object, also when the same object is a `belongs_to` association |
| `type: MomentMedia` or `type: [MomentMedia]` | named nested value or list, declared once by a field block |
| `from: "location.lat"` | read the value from a nested path of the response when the field name itself is absent |
| `merge: :non_empty` | what a row from the server may replace in a stored value, see below |
| `local: true` | the server never sends the field and a refresh never replaces or clears it, see [local fields](#local-fields) (needs `optional: true` or `null: true`) |

`index :a` declares a single-column SQLite index. `index :a, :b` declares a composite index without requiring individual indexes. Columns must be scalar or enum fields.

`search :text, :tags` selects string fields or lists of strings for [full-text search](live-data.md#text-search).

Declared queries generate indexes with equality filters first and sort columns next, preserving each sort direction. Duplicate indexes and indexes covered by a longer prefix are removed.

Every scalar and enum field is its own typed SQLite column, indexed or not. An update writes only the columns of the fields that changed. A null value and an absent value are stored the same way (SQL `NULL`), so a field read back is either present or missing, never `null`.

### Enums

Every `type: :enum` field becomes an enum named after its owner and field (`VibeKind` for `Vibe.kind`, `MessageReplyNodeKind` for a nested `kind`). `as: Kind` sets the name. A schema enum used by an input object, an operation arg or an operation result keeps the schema name (`WidgetKind`). Each name is declared once, and the same name with different values stops the run.

Case names come from the values: lowerCamel in Swift (`like`, `fooBar`, `` `default` `` for a keyword) and PascalCase in Kotlin (`Like`, `FooBar`). `HAPPY` and `FOO_BAR` read as words. A value that starts with a digit, becomes an empty name, becomes `unknown` (Swift) or `Unknown` and `Companion` (Kotlin), or gives the same name as another value stops the run with the value in the message.

Every enum has an `unknown` case that keeps the text. A value the declaration lacks is not an error on any path: a server row, a nested JSON value, an input, an argument and a result decode it into `unknown`, and writing it back sends the same text. The SQLite column stays text and is stored as it arrived, so a filter compares the text of the typed value.

```swift
let loved = Vibe.where(Vibe.kind == .love)
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

Declare nested values with `field :name, type: NamedType do ... end`. Use `type: [NamedType]` for a list.
An object generates a Swift `Codable`, `Sendable`, `Equatable` struct and a Kotlin serializable data class.
Models decode from row cells. Both platforms use the same type and property names. Use generated values when creating or updating a model, or decode a response with the platform serializer of the nested type. Do not write another struct, data class or decoder with the same fields.

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

Nested blocks contain `field` declarations. Their options include `type`, `null`, `optional`, `default`, `from`, and `values` for enums. Blocks can contain more blocks. An unnamed object joins the owning type and capitalized field name. All generated value types have top-level names.

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

An enum default must be one of its declared values. `from` inside a nested block names a literal JSON key, including dots. At the model level, `from` reads a response path.

Declare each named type once. Pass all files that declare or use it to `dblayer-gen`. `type` accepts a named value such as `GiftItem` or a list such as `[GiftItem]`. A missing declaration, duplicate type or invalid default reports its input file and line. Use `type: :raw` for unrestricted values.

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
field :media, type: MomentMedia, null: true, merge: { fileUrl: :non_empty, width: :positive, transcodeStep: [[:PENDING, :SUBMITTED, :PROGRESSING], [:COMPLETED, :FAILED]], transcodeProgress: :monotonic }
field :avatarUrl, type: :string, null: true, merge: :non_empty
```

A local edit that is not confirmed yet is never replaced, with or without a rule. A rule that does not fit the field type rejects the model.

## Local fields

A local field is written by the app and stored with the row. The server never sends it: it is not in the sync diff, no mutation carries it, and no answer, page, event or refresh replaces or clears it. It reads `nil` until it is written, survives a restart, and goes with the row when the row is destroyed. Every subscription of the row delivers it together with the server fields.

```ruby
field :tone, type: :string, optional: true, local: true
field :media, type: MomentMedia, null: true, merge: { fileUrl: :non_empty, tone: :local }
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

An `ephemeral do ... end` block inside `model` declares values that live only in memory. They are never written to SQLite and do not survive a restart.

```ruby
model User do
  field :name, type: :string

  ephemeral do
    field :typing, type: :bool, default: false, reset_after: 5.seconds
    field :mood, type: :string, null: true
  end

  scope :profile do
    fetch UserProfile
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

`model Presence, storage: :memory` keeps the whole model in memory. Rows are read, observed and loaded through scopes with `fetch` like any other model. They are not written to the database file and are empty after a restart.

```ruby
model Presence, storage: :memory, expire_after: 1.minute do
  field :userId, type: :id
  index :userId
  field :room, type: :string

  scope :online do
    fetch PresenceOnline
    list
    fresh_for 30.seconds
  end
end
```

- `expire_after:` is optional. A row that has not changed for that time is deleted, and observers receive it in `diff.removed`.
- A memory model has no `file` fields and no model `operation` blocks, so it has no outbox.
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
| `belongs_to :name` | `model:`, `key:`, `touch: true`, `sets:`, `newer:`, `from:` |
| `has_many :name` | `model:`, `key:`, `dependent: :destroy`, `from:` |
| `has_one :name` | `model:`, `key:`, `dependent: :destroy`, `from:` |
| `references :name` | `model:`, `key:` (array of ids), `from:` |

Singular associations capitalize their name, preserving a final `s`. Plural associations remove one final `s` before capitalization. Use `model:` for irregular names.
`belongs_to` defaults to `<name>Id`. `has_many` and `has_one` use the owner's lower-camel name plus `Id`. `references` uses its association name.

- `from: "visitorsList.nodes"` is the dotted path in a response where the association's objects sit. It defaults to the association name. A list on the way is walked element by element, and a connection (`nodes` or `edges { node }`) gives its nodes.
- Objects found there become rows of the target model. A `belongs_to` object that holds only its `id` (`owner { id }`) cannot become a row when the target needs more fields, and it still sets `key:` to that id. `has_many` and `has_one` rows get the parent id in `key:`. A `references` parent keeps the ids of its rows in `key:`; declare that field directly (`field :similarIds, type: [:id]`), the model is rejected without it.
- `dependent: :destroy` removes children in the same commit as the parent.
- `touch: true` bumps the parent's revision when a child changes, so observers of the parent refresh. See [partial child updates](parent-fields.md) for `sets` field presence.
- `sets: { lastMessageId: :id, lastMessageAt: :createdAt }` on a `belongs_to` writes the child fields to the parent fields in the same commit as every write of the child: a local `create`, an event, a page landing, the answer of a mutation, a replace, `land`. Destroying the child that the parent points at (the first parent field set from the child `id`), locally or from the server, moves the parent fields to the newest remaining child by the `newer` child fields, or to null when none remains. `newer: { sequenceNumber: :lastSequenceNumber, createdAt: :lastMessageAt }` writes only when the child is newer than what the parent holds. The pairs are a tuple in the order written: the first pair where both sides have a value and differ decides, a child with the id the parent already points at always refreshes, a temporary id outranks a confirmed one (client and server clocks never order them), and an empty parent value counts as older. The same tuple guards the parent row itself: a row of the parent that arrives from a list, an event or a mutation answer never moves these parent fields backward. An event for a child whose parent is not stored (a chat outside the loaded pages) reloads the watched lists of the parent model. A discard or a refusal gives the parent values back, the swap of a temporary child id moves a parent field that points at it. Both options name fields that exist on their side, otherwise the model is rejected.
- Foreign-key indexes are inferred from associations. Declare the foreign-key field, without adding its index again.
- There are no counter caches. Count with a query: `moment.vibes.count`.

## Scopes

`scope :name do ... end` declares a named selection. `where :field` binds the column to a generated parameter with the same name. Multiple columns use `where :ownerId, :status`. `order field: :asc` sorts rows. Null values come first in `:asc` and last in `:desc`. Use `:asc_nulls_last` or `:desc_nulls_first` to reverse their placement.

When a response row has no value for a column the relation groups by, the param fills it. A response without an id becomes a row keyed by the param.

A `fetch Operation` line connects a scope to the server. A relation that depends on columns names them: `where :userId` loads and lists the rows whose `userId` equals the param. `order createdAt: :desc` sorts the rows locally, otherwise the server order is kept. A row whose param column changes (a message moving to another `chatId`) leaves the list of the old value and joins the head of the list of the new value in the same commit, for relations that list every row of their params. `joins: :created` on a relation without params makes the list take the rows this device creates, at its head; `joins: :events` makes it take rows from its subscription events.

`where` accepts scalar and enum columns. A param passed as `nil` is a value of its own: `Moment.by_user(nil)` is the unfiltered list and has its own window. A param column gives the row one owner list: a row that belongs to several lists at once (a moment in the default feed and in a vibe feed) needs one relation per list, and the vibe id goes in `text: [:vibeId]`, not in a column param. `text: [:query]` declares params that are not columns, such as a search string. They are sent as variables, always strings, and each value gets its own window: `scope :search, text: [:query] do ... end` generates `Model.search("cats")`.

| Line | Meaning | Default |
| ---- | ------- | ------- |
| `fetch Operation` | GraphQL operation name (required) | none |
| `fetch Operation, root: "a.b"` | path of the rows inside the response, `node.x` for a list under a [node read](#node-reads) | first field of the operation |
| `paginate 20` | Relay-style pages of 20, forward | whole response |
| `paginate 20, backward: true` | pages backward | forward |
| `paginate 20, cursor: :afterSequence, from: "lastSequenceNumber", cursor_type: :int` | the server pages by its own variable: the next page sends `afterSequence`, taken from `lastSequenceNumber` of the connection. No page size is sent | Relay `first`/`after` |
| `total from: "node.membersCount"` | `total` of the list state comes from this field of the response, after `paginate` | `totalCount` of the connection |
| `list` | response is a plain list | single record |
| `fresh_for 5.minutes` | how long a response counts as fresh | `5.minutes` |
| `fresh_for 5.minutes, empty: 30.seconds` | shorter freshness for an empty response | same |
| `max_pages 5` | page limit per load | none |
| `variables statusFilter: :status, includeGifts: false` | name the GraphQL variables: a symbol takes the value of a relation param, any other value is sent as written | every param under its own name |
| `edge :impressionUuid, type: :string, null: true` | a value of the connection edge that carried the row, stored for this scope, see [edge values](#edge-values) | none |
| `updates Subscription, deleted_by: :field` | keep the relation current with a subscription. Write the line again for each subscription. `removes: true` makes every event delete its row, `debounce: 300.milliseconds` merges bursts | none |

An operation name is a constant like `MomentsFeed`, or a string like `"feed"` when the GraphQL name starts with a lowercase letter.

Durations are written `300.milliseconds`, `30.seconds`, `5.minutes`, `2.hours`, `1.day`. Mutations and the `updates` line are described in [server sync](server-sync.md).

## Edge values

A connection edge can carry data of its own next to `node`, for example `impressionUuid` on `feed { edges { impressionUuid node { ... } } }`. The value belongs to the connection, not to the row: the same moment can arrive from the main feed with a value and from a vibe feed with null, and each connection keeps its own.

```ruby
scope :feed do
  fetch "feed"
  paginate 20
  edge :impressionUuid, type: :string, null: true
  edge :impressionScore, type: :int, null: true
end

scope :vibe_feed, text: [:vibeId] do
  fetch "feed"
  paginate 20
  edge :impressionUuid, type: :string, null: true
end
```

`edge` takes the options of a field: `type:`, `null:` or `optional:`, `from:` for another key in the response. Each scope with edges generates one value type and two reads with the parameters of the scope: `feedEdge(of:)` reads in place, `readFeedEdge(of:)` waits on a background thread. The row has no new property.

```swift
let edge = try Moment.feedEdge(of: moment.id)            // MomentFeedEdge?
let vibe = try await Moment.readVibeFeedEdge(vibeId, of: moment.id)
edge?.impressionUuid                                      // String?
```

```kotlin
val edge = Moment.feedEdge(moment.id)                     // MomentFeedEdge?
val vibe = Moment.readVibeFeedEdge(vibeId, moment.id)
edge?.impressionUuid                                      // String?
```

- A read gives `nil` for a row that is not in the connection, and a value type with `nil` fields for a row without edge values.
- The answer of the same connection replaces the values of its rows, also with null. A row that leaves the connection loses its values. An answer without edges (a node read, a mutation, an event, a list, another connection) leaves them as they are.
- The values live in the cache. A database that is rebuilt or reset loses them, and the next load of the connection brings them back.
- A change of an edge value alone does not change the row or a live list. Read the value when the row is shown.
- A read takes one row by key from the reader connection. It does not wait for a write and costs the same at 20 and at 2000 rows.
- `edge` needs `paginate`, and `null: true` or `optional: true`. With `--schema` the key must exist on the edge type of the connection, the operation must select it, and the type must match the schema (`Int` is `:int`, `Float` is `:num`).
- Edges of a connection nested inside an association give no values. `cursor` and `sequenceNumber` of the edge are not read.

## Node reads

GraphQL `node(id:)` reads one entity of any type that implements `Node`. A model reads it with `node`, a related list reads it through `root:`.

```ruby
model Chat do
  field :title, type: :string
  field :shareUrl, type: :string, null: true
  node ChatNode
  nodes ChatsByIds
end

model Message do
  field :chatId, type: :id
  field :media, type: MessageMedia, null: true do
    field :kind, type: :string, null: true
    field :fileUrl, type: :string, null: true
  end
  node MessageTranscodeStatus, as: :transcode_status
  scope :chat_media do
    where :chatId
    fetch ChatMedia, root: "node.messages"
    paginate 30
  end
end
```

```graphql
query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title shareUrl } } }
query MessageTranscodeStatus($id: ID!) { node(id: $id) { ... on Message { id chatId media { kind fileUrl } } } }
query ChatMedia($chatId: ID!, $first: Int, $after: Cursor) { node(id: $chatId) { ... on Chat { messages(first: $first, after: $after) { edges { node { ...MessageFragment } } pageInfo { hasNextPage endCursor } } } } }
query ChatsByIds($ids: [ID!]!) { nodes(ids: $ids) { ... on Chat { id title shareUrl } } }
```

A paged list of the entities under a node, such as the members of a chat, is a scope of the listed model with a text parameter. It has no `order`: the rows keep the order of the server across pages, a restart and a refresh of a row by another list.

```ruby
model User do
  scope :chat_members, text: [:chatId] do
    fetch ChatMembers, root: "node.members"
    paginate 30
    total from: "node.membersCount"
    fresh_for 5.minutes
  end
end
```

```graphql
query ChatMembers($chatId: ID!, $first: Int, $after: Cursor) { node(id: $chatId) { ... on Chat { membersCount members(first: $first, after: $after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }
```

```swift
let members = LiveList(User.chatMembers(chatId))   // members.rows, members.state.hasMore
try await User.chatMembers(chatId).loadMore()      // true while another page exists
```

```kotlin
val members = User.chatMembers(chatId)
members.loadMore()                                 // true while another page exists
```

`total` is set only when the connection selects `totalCount`. Without it `total` stays nil and `hasMore` comes from `pageInfo`.

`total from: "node.membersCount"` takes `total` from a field beside the connection: the path starts at the first field of the operation, like `root:`. The group size `Chat.membersCount` counts every member, `totalCount` of the connection counts only the members the viewer can see. The document must select the field and the schema check refuses a path that is not an `Int`. Each page that carries the field sets `total`, a page without it keeps the last value, and the value survives a restart. The line needs `paginate`.

```swift
let state = await User.chatMembers(chatId).state.stream()   // state.total is Chat.membersCount, 53 for a group of 53
```

```kotlin
val total = User.chatMembers(chatId).state.flow.first { it.total != null }.total   // Chat.membersCount
```

| Line | Meaning |
| ---- | ------- |
| `node Operation` | one entity by id, generates `Model.node(id)`. The operation sends the id as `$id` |
| `node Operation, as: :name` | a second read of the same model, such as a small status read, generates `Model.name(id)` |
| `node Operation, variable: :momentId` | the id goes into `$momentId` |
| `node Operation, fresh_for: 1.minute, empty: 10.seconds` | how long an answer counts as fresh, as for `fresh_for` |
| `nodes Operation` | many entities in one request, generates `Model.nodes(ids)`. The operation takes `$ids` |
| `fetch Operation, root: "node.messages"` | a related list under the node: the path starts at `node` and continues inside the `... on` type |

```swift
let chat = try await Chat.node(chatId)                      // nil when the server has no such chat or the viewer cannot see it
let fresh = try await Chat.node(chatId, force: true)        // an event named this chat: read it again now
let status = try await Message.transcodeStatus(messageId)
let chats = try await Chat.nodes(ids)
```

```kotlin
val chat = Chat.node(chatId)                                // null when the server has no such chat or the viewer cannot see it
val fresh = Chat.node(chatId, force = true)                 // an event named this chat: read it again now
val status = Message.transcodeStatus(messageId)
val chats = Chat.nodes(ids)
```

- The answer lands as a normal server row of the model. Only the fields the operation selects change, so a small read leaves the other fields, the merge rules and local fields as they were. A local edit that is not confirmed yet is kept.
- A plain call inside the freshness time of an id reads the local row and sends nothing. `force: true` always asks the server. Each read of a model has its own freshness per id.
- Calls for one id that overlap share one request. Nothing needs to be started or released.
- The result is nil when the answer is `null`, has another type than the `... on` fragment (an empty object), or the id has no entity. Nothing is written then and the stored row stays as it was.
- `nodes` skips every `null` and empty entry, keeps the order of the answer, writes each row once and keeps no list of its own, so every call asks the server.
- `Model.entity(id)` and `Model.find(id)` stay the way to watch a row. A node read fills them.
- The generator stops with file and line when a field is selected under `node` outside `... on`, when the `... on` type is not an implementation of the interface, when one `node` selects two types, or when the model has a field that the `... on` type lacks. A selection outside `... on` would give a row for a foreign type, so it is an error.

## Validation

`dblayer-gen` reports mistakes with file and line (`user.dbl:12: unknown key touch for has_many`). The same checks run in the core when a database is opened: unknown models, missing indexes on foreign keys, duplicate names and invalid combinations.

`dblayer-gen spec models/*.dbl` prints the model specification. `dblayer-gen explain models/*.dbl` creates an empty in-memory database and prints DDL, scope SQL and SQLite query plans. Plans use sample parameters and have no production statistics.

## Check against the server schema

`dblayer-gen --schema schema.graphql models/*.dbl` checks the declarations against the GraphQL schema of the server. It prints nothing but notes when every check passes, and exits with code 1 and a file and line for each mismatch.

For each scope with `fetch`, the check finds the named operation, takes its first field (the alias when there is one) and follows `root:` from there, through connections (`nodes` or `edges { node }`) to the type that holds the rows. Operations and fragments are read from the `queries`, `mutations`, `subscriptions` and `fragments` directories next to the schema file, or from every `--operations <file or directory>` you pass. A spread of a fragment that no document declares is an error.

`from:` names the key of the response, which is the alias when the operation gives one (`giftTier: kind` is read with `from: "giftTier"`). The check follows each key through the selection of the operation, into inline fragments and named fragments, and tests the schema field behind it. The key of a field that the operation does not select is taken as the schema field name. A union or interface field must select `__typename` somewhere in its own selection, also inside a fragment. Without operation documents the operation name is read as the schema field.

Every field of the model, every `from:` path, every `id_from:` and every field of a nested type must exist on that type. The check reports:

- a field or path the schema does not have;
- a list declared as a single value, or the reverse;
- a nullable schema field declared without `null: true`, `optional: true` or `default:`;
- a scalar, enum or object that does not fit the schema type, and an `enum` value the schema enum lacks;
- a scope that paginates, lists or returns one row against a schema field of another shape;
- an association `from:` path of the wrong shape.

Each arg declared by an `args` line inside a model operation is checked against the mutation that sends it. With `wrap :input`, the check reads the input type of the wrapped variable. Without it, the check reads the variables of the operation. The check needs the mutation in the operation documents and reports:

- a variable that `input` binds to an arg but the input type or the operation does not have;
- an arg whose kind does not fit the type: `:string` fits `String`, `ID` or a scalar mapped to `:string` or `:date`, `:id` fits `ID`, `Int`, `String` or a scalar mapped to `:string`, `:int` or `:long`, `:int` fits `Int` or a scalar mapped to `:int` or `:long`, `:long` fits a scalar mapped to `:long`, `:num` fits `Float`, `Int` or a scalar mapped to `:num`, `:bool` fits `Boolean`, `:ids` fits a list of `ID`, `Int` or `String`, and the name of an input object or enum (`:WidgetKind`) fits that type;
- an arg with `?` that the schema requires (`String!` without a default).

A list kind (`[:WidgetInput]`, `[:WidgetInput?]`, `[:WidgetInput]?`) fits a list of that type. The `?` inside the brackets is the nil of an item and needs a schema item without `!`. The `?` after the bracket makes the list optional and needs a schema variable without `!`.

An `args` line inside a model operation takes the same kinds as an operation without a model, including the name of a schema enum or input object. The call gets a typed parameter, and an `optimistic` line can write an enum arg into an enum field of the model. Without `--schema` the generator stops and names the type.

```ruby
operation :answer_connection do
  mutation   ConnectionUpdate
  args       decision: :ConnectionAction, outcome: :ConnectionStatus
  input      userId: :id, action: :decision
  optimistic connection: :outcome
  wrap       :input
end
```

```swift
try await user.answerConnection(decision: .accept, outcome: .connected)
```

```kotlin
user.answerConnection(decision = ConnectionAction.Accept, outcome = ConnectionStatus.Connected)
```

Fields marked `local: true`, `file` fields and models with `storage: :local` are skipped. Schema fields that the model leaves out are listed as notes, because a model may select a subset.

## Access level

`dblayer-gen --internal swift ...` emits models and their generated value types without `public`, for an app that generates them into its own module.

## Input objects of the schema

Standalone operations require named documents when `--schema` is set. The check validates operation kinds, argument types, required inputs and schema defaults. Input objects use `args input: :StickerInput` (see [server sync](server-sync.md)):

`dblayer-gen --schema schema.graphql swift models/*.dbl` checks the models as above and also writes the input objects. The same flag does both, so the schema file must satisfy the check.

A schema input object with `@oneOf` sets exactly one field, so it is an enum and not a struct. The case holds the value of its field. Both platforms send one key.

```swift
let props: StickerPropsInput = .emoji(StickerEmojiInput(code: "fire"))   // {"emoji":{"code":"fire"}}
```

```kotlin
val props: StickerPropsInput = StickerPropsInput.Emoji(StickerEmojiInput(code = "fire"))   // {"emoji":{"code":"fire"}}
```

The generator writes every input object that an operation names, and every input object those fields use, once each, after the models. A name that the schema does not have, a name that is neither an input object nor an enum, or an operation that names one without a schema file stops the run with code 1 and a message.

| Schema field | Swift | Kotlin |
| ------------ | ----- | ------ |
| `String`, `ID`, a scalar mapped to `:string` or `:date` | `String` | `String` |
| a scalar mapped to `:long` | `Int` | `Long` |
| an enum | its enum with an `unknown` case | its sealed class with `Unknown` |
| the scalar named `JSON` | `JSONValue` | `JsonElement` |
| `Int`, `Float`, `Boolean` | `Int`, `Double`, `Bool` | `Int`, `Double`, `Boolean` |
| `[T!]!`, `[T]` | `[T]`, `GraphQLNullable<[T?]>` | `List<T>`, `GraphQLNullable<List<T?>>` |
| another input object | its struct | its data class |
| an input object with `@oneOf` | an enum with one case per field | a sealed interface with one class per field |
| a field without `!` | `.omitted`, `.null`, `.value(value)` | `Omitted`, `Null`, `Value(value)` |
| a required field with a schema default | optional, omitted when nil | nullable, omitted when null |

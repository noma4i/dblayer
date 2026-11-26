# Describing models

A model is described once in a `.dbl` file. The syntax follows Ruby on Rails: a word, a symbol name, then named options after colons and commas. Order of options does not matter. Rust parses the file. It does not run Ruby. `dblayer-gen` turns the files into typed Swift and Kotlin models and a model specification for the core. A declaration that the generator refuses stops the run with the file and line.

## Model

| Declaration                        | Meaning                                                                                                                                            |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `model Name do ... end`            | a synchronized model: rows live in SQLite, reads and writes go through scopes and operations                                                       |
| `model Name, storage: :local do`   | rows persist without an outbox and take no server declarations, see [storage rules](storage-models.md). `:disk` is the default and is not written |
| `model Name, storage: :memory do`  | rows live in memory and are empty after a restart, see [Memory models](#memory-models)                                                              |
| `index :a` / `index :a, :b`        | a single-column or composite SQLite index over scalar or enum fields. Foreign keys and the `where` and `order` of scopes get indexes by themselves  |
| `search :text, :tags`              | string fields or string lists for [full-text search](live-data.md#text-search)                                                                       |

Every scalar and enum field is its own typed SQLite column. An update writes only the changed fields. The `id` field is added by the generator and stored as text.

Names of attributes, parameters, scopes, associations and operations start with a lowercase letter and do not end with `_`. A scope, association or operation cannot take `id`, `find`, `where`, `save`, `count`, `exists`, `first`, `observe`, `order`, `retry`, `discard` or a name the generated code owns (`sync`, `uploads`, `modelName`, `layout`, `wireLayout`, `singleton`, `rowRevision`, `estimatedBytes`). Keywords of Swift, Kotlin and SQL are escaped in generated code and storage, while JSON keys keep their names. Rename the attribute or parameter and keep the server name in `from:` or `variables`.

Generated Kotlin models carry Compose `@Immutable`, so applications need no stability config for them. Treat collection contents as immutable.

`dblayer-gen spec models/*.dbl` prints the model specification. `dblayer-gen explain models/*.dbl` builds an empty in-memory database and prints the DDL, the scope SQL and the SQLite query plans, with sample parameters and no production statistics. `dblayer-gen --internal swift ...` emits generated types without `public`.

### Memory models

- Rows are read, observed and loaded through scopes like any other model. They never reach the database file or the outbox.
- A memory model has no `:file` attributes and no model `operation` blocks.
- `expires_in:` deletes a row that has not been written for that time, and readers receive its removal. Every write renews the time, also a write of equal values, which sends no change to observers.
- A `destroy` of a row that is already gone succeeds and changes nothing.
- A scope of a memory model accepts `subscribe` without `query`: the subscription opens with the first reader, is shared, closes after the last reader and loses events while no reader exists. An event without an id gets a new UUID.

## Attributes

`attribute :name, :type, options` declares a field. With `--schema`, `attribute :name` without a type takes its type and nullability from the GraphQL type of the same name, and an explicit `null:` wins. Local attributes need an explicit type.

| Type                                                | Meaning                                                                                                                      |
| --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `:string`, `:num`, `:int`, `:bool`, `:id`           | scalars. `:id` accepts a string or a number and stores a string. `:int` is `Long` in Kotlin                                  |
| `:date`                                             | ISO 8601 text or milliseconds, stored as UTC text with milliseconds (`2026-01-03T00:00:05.000Z`), so text order is time order |
| `:json`                                             | any JSON value                                                                                                               |
| `[:string]`, `[:id]`                                | list of scalars                                                                                                              |
| `:enum`, `[:enum]`                                  | one value or a list of values, see [Enums](#enums)                                                                           |
| `:file`                                             | attachment, see [files](files.md)                                                                                            |
| `Name`, `[Name]` with a block                       | nested object or list, see [Nested objects and lists](#nested-objects-and-lists)                                             |

| Option                | Meaning                                                                                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `null: true`          | the value may be null                                                                                                    |
| `optional: true`      | the key may be absent in server responses                                                                                |
| `default: value`      | the value of an absent key, and of a null for a field without `null: true`                                               |
| `from: "a.b"`         | the response path of the value when the attribute name is absent. An explicit null on the path clears a nullable field   |
| `id_from: :parent`    | take the id from a nested parent object                                                                                  |
| `parse: :json`        | a `:json` input is JSON text, the core stores the parsed value                                                           |
| `merge: rule`         | what a server row may replace, see [Merge rules](#merge-rules)                                                           |
| `local: true`         | written only by the app, see [Local fields](#local-fields)                                                               |
| `derived: true`       | computed from child rows by `sets:`, see [Associations](#associations)                                                   |

An absent path key leaves the stored field unchanged. A nullable field with a default keeps an explicit null after a reopen.

### Enums

```ruby
model Vibe do
  attribute :kind, :enum, values: ["like", "love", "laugh"]
  attribute :lists, [:enum], values: ["PRIMARY", "SECONDARY"], as: GroupList
end
```

An enum takes its name from its owner and field (`VibeKind` for `Vibe.kind`), or from `as:`. A type that the schema knows takes its values from the schema at every depth, so `values:` belongs only to an enum the schema lacks. Each name is declared once: the same name with different values stops the run.

```swift
switch vibe.kind {
case .like, .love, .laugh: break
case .unknown(let text): print(text)
}
```

```kotlin
when (val kind = vibe.kind) {
    VibeKind.Like, VibeKind.Love, VibeKind.Laugh -> Unit
    is VibeKind.Unknown -> println(kind.value)
}
```

Case names come from the values: lowerCamel in Swift, PascalCase in Kotlin. Every enum has an `unknown` case that keeps the text, so a value the declaration lacks is never an error on any path and is written back as received. The column stays text.

### Custom scalars

A schema scalar other than the built-in ones needs one line, once per project:

```ruby
scalar :ISO8601DateTime, as: :date
scalar :Int64, as: :long
```

`as:` is `:string`, `:date`, `:int`, `:long` (`Int` in Swift, `Long` in Kotlin), `:num`, `:bool` or `:json`. `JSON` needs no line. A scalar used without a line stops the run with the line to add.

### Nested objects and lists

```ruby
model Record do
  attribute :media, RecordMedia, null: true, merge: { fileUrl: :non_empty } do
    attribute :kind, :string, null: true
    attribute :fileUrl, :string, null: true
  end
  attribute :reactions, [RecordReactionNode], default: [] do
    attribute :body, :string, null: true
    attribute :userId, :id, null: true, from: "user.id"
  end
end
```

A block generates a Swift `Codable` struct and a Kotlin serializable data class with the same names on both platforms. Blocks nest. An unnamed object joins the owner and the field name. Use the generated values to create and update rows, and write no second struct. Nested values belong to their row: they have no table, index, relation, file or sync operation of their own. For reusable names, recursive lists, unions and interfaces see [nested value types](nested-types.md).

Decoding is the same on both platforms. An unknown key is ignored. An absent nullable or optional field is `nil`, an absent field with a default takes it, and a null required field with a default takes it too. A null nullable field is `nil` even with a default. An absent or null required field without a default, and a wrong type, are decoding errors. An ID accepts a string or a signed 64-bit integer. A nested enum takes `unknown` for an undeclared value.

`from:` reads a response path at every depth. The core reads the path once and stores the value under the key `user.id`. A write checks nested values against the declared type: an unknown key gives an unknown-field error with the path (`media.id`, `replies[1].body`), a wrong type an invalid-value error. A `:json` field accepts any value.

### Merge rules

A server row (page, event, answer, replace) replaces the fields it carries. A `merge:` rule decides what an arriving value may replace, so the order of answers does not matter. A local edit that is not confirmed is never replaced.

| Rule                                                | An arriving value is ignored when                                                                           |
| --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `:present`                                          | it is null and a value is stored                                                                            |
| `:non_empty`                                        | it is null, `""`, `[]` or `{}` and a non-empty value is stored                                              |
| `:positive`                                         | it is not above 0 and a positive number is stored (`int`, `num`, `date`)                                    |
| `:monotonic`                                        | it is null or lower than the stored number or date                                                          |
| `[[:PENDING, :PROGRESSING], [:COMPLETED, :FAILED]]` | its tier is below the stored tier, values inside a tier replace each other (`enum`, `string`)               |
| `:stable_list`                                      | never: stored ids keep their places, dropped ids go, new ids follow in arrival order (`[:id]`)              |
| `{ key: rule, ... }`                                | `:json`: merged key by key, a key without a rule ignores null                                               |

A rule that does not fit the field type rejects the model.

### Local fields

`attribute :tone, :string, local: true` declares a field that the app writes and the row stores. The server never sends it: no diff, mutation, answer, page, event or refresh replaces or clears it. It reads `nil` until written, survives a restart and goes with the row. It is part of the generated type and absent from `create` and `update`. A key of a `:json` attribute is local with `merge: { tone: :local }`.

`setLocal` writes a local field by path (`tone`, or `media.tone` for a key). One write is one delivery to the observers of the row, an unchanged value is silent, and null clears it. A write for a row that is not stored yet waits and lands when the row arrives. A path that is not declared local is refused.

Inside `operation :create`, `local :userId` fills an attribute locally without sending it, and `local :authorId, from: { of: CurrentUser, field: :id }` takes it from a singleton row. Each destination occurs once. An explicit argument wins, an absent source leaves the attribute unset. `operation :update` takes plain `local` only.

### Ephemeral fields

An `ephemeral do ... end` block (`attribute :typing, :bool, default: false, reset_after: 5.seconds`) declares values that live only in memory and never reach SQLite. `default:` is required unless `null: true`. After `reset_after:` without a write the value returns to the default. A write and a reset reach readers as a change of the row. Deleting the row removes its values. A write for a row that is not stored yet waits for the row. `where`, `order`, `index` and `scope` cannot use an ephemeral field, and a field written by a subscription `values:` must declare `reset_after`.

## Associations

| Word               | Options                                                                  |
| ------------------ | ------------------------------------------------------------------------ |
| `belongs_to :name` | `class_name:`, `foreign_key:`, `touch: true`, `sets:`, `newer:`, `from:` |
| `has_many :name`   | `class_name:`, `foreign_key:`, `dependent: :destroy`, `from:`            |
| `has_one :name`    | `class_name:`, `foreign_key:`, `dependent: :destroy`, `from:`            |
| `references :name` | `class_name:`, `foreign_key:`, `from:`                                   |

The target class is the capitalized name, and plural words lose one final `s` (`class_name:` for irregular names). `belongs_to` keys on `<name>Id`, `has_many` and `has_one` on the lower-camel owner name plus `Id`, `references` on its own name. A `references` parent keeps the ids of its rows in an attribute you declare (`attribute :similarIds, [:id]`).

- `from: "visitorsList.nodes"` is the response path of the associated objects, default the association name. Lists on the path are walked, and a connection gives its nodes. Objects there become rows of the target. A `belongs_to` object that holds only its `id` still sets the foreign key.
- An explicit null for a `belongs_to` object or id clears a nullable foreign key and leaves the target row. A null `references` path empties the list. An absent path keeps the key. `has_one` and `has_many` write no key on the parent.
- `dependent: :destroy` removes children in the same commit as the parent.
- `touch: true` bumps the revision of the parent when a child changes.
- There are no counter caches: count with `scope.count(...)`.

`sets:` on a `belongs_to` derives parent fields from the newest visible child. See [partial child updates](parent-fields.md).

```ruby
model Record do
  attribute :containerId, :id
  index :containerId
  attribute :createdAt, :date
  attribute :sequenceNumber, :int, null: true
  belongs_to :container, sets: { lastRecordId: :id, lastRecordAt: :createdAt }, newer: [:sequenceNumber, :createdAt]
end
model Container do
  attribute :lastRecordId, :id, derived: true
  attribute :lastRecordAt, :date, derived: true
end
```

The children are the only source of a `derived: true` field: no response, event, `create`, `update` or `setLocal` writes it, and it is null when no child remains. `newer:` lists the child fields that order the children: a temporary id first, then the listed fields (a missing value is the oldest), then the id. Both options name fields that exist on their side.

## Scopes

`scope :name do ... end` declares a named selection and generates `Model.name(...)`. `parameter :ownerId, :id` declares a typed parameter and `where ownerId: :ownerId` binds a column to it. Each column takes its own `where` pair, on scalar and enum columns. `order createdAt: :desc` sorts locally, null values first in `:asc` and last in `:desc`. Use `:asc_nulls_last` or `:desc_nulls_first` to reverse that. Without `order`, a remote scope keeps the server order.

| Line                                                | Meaning                                                                                                                      | Default                       |
| --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `query "Operation"`                                 | the GraphQL operation that loads the scope. A scope without it is local                                                      | local                         |
| `response from: "a.b"`                              | path of the rows in the response, `node.x` under a [node read](#node-reads)                                                  | first field of the operation  |
| `response shape: :list`                             | the response is a plain list                                                                                                 | single record                 |
| `variables name: :parameter, flag: false`           | name the GraphQL variables: a symbol takes a scope parameter, any other value is sent as written                              | each parameter by its name    |
| `total from: "node.membersCount"`                   | the `total` of the list state, after `paginate`                                                                              | `totalCount` of the connection|
| `edge do ... end`                                   | values of the connection edge, see [Edge values](#edge-values)                                                               | none                          |
| `subscribe "Name", apply: { ... }`                  | keeps the scope current, see [Events](#events)                                                                               | none                          |

Parameters and windows:

- A parameter with `where` identifies the rows of the list. A parameter without `where`, such as a search text, is sent as a variable and each value has its own window. A `nil` parameter is a value of its own.
- Types: `:string`, `:id`, `:int`, `:bool` (sent as JSON integer or boolean, `Long` in Kotlin), and the name of a schema enum (`parameter :list, GroupList`). An enum parameter takes `default: :PRIMARY`, which is always sent. The schema check needs a document variable of the same name (or the name left of `variables`) with that type.
- A row whose `where` column changes leaves the old list and joins the head of the new one in the same commit.
- `Model.since("c1", afterSequence: 120)` is `Note.since("c1", afterSequence = 120L)` in Kotlin. A scope that pages ascending and one that pages descending are two scopes.

### Pagination and freshness

| Line                                                                         | Meaning                                                                                                              | Default                    |
| ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `paginate size: 20, direction: :forward`                                     | Relay-style pages, `:backward` pages from the end                                                                    | whole response             |
| `paginate ..., cursor: { parameter: :afterSequence, from: "lastSequenceNumber", type: :int }` | the server pages by its own variable, all three keys required                                               | Relay `first`/`after`      |
| `paginate ..., max_pages: 5`                                                 | page limit per load, the list ends there                                                                             | none                       |
| `freshness expires_in: 5.minutes`                                            | how long a response counts fresh, counted from its arrival. A saved window stays fresh after a restart               | `5.minutes`                |
| `freshness expires_in: 5.minutes, empty_in: 30.seconds`                      | a shorter time for an empty response                                                                                 | same                       |

Time-based freshness defaults to `5.minutes` for remote scopes and node reads. Durations are written `300.milliseconds`, `30.seconds`, `5.minutes`, `2.hours`, `1.day`. Expiry alone starts no request: a later reconnect or read checks freshness again. `loadAll()` reads the remaining pages, `loadAll(force: true)` reloads the first page too.

A paginated scope can compare saved sequences when its first stream opens. For example, `freshness head: :parent, parent: :latestSequence, member: :sequence` names a belongs-to association, an integer parent field, and an integer member field. Bind the association's foreign key in the scope's `where`. Use this declaration in place of `expires_in` and `empty_in`.

For a paginated scope, the first page and refresh apply the declared order. Each following page appends new IDs in server order. A repeated ID updates its fields and keeps its position. See [loading more rows](live-data.md#loading-more-rows).

### Flags

Flags sit on the `scope` line and take only `true`: omit one for the default.

| Flag                                       | Meaning                                                                                                                           |
| ------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| `refresh_on_foreground: true`              | an online idle window with a reader reloads on foreground. Needs `query`                                                          |
| `keep_on_gap: true`                        | after a connection gap the window is not reread and its rows, order and position stay as shown. Needs `query`, not with the flag above |
| `stable_order: true`                       | each row keeps the place it joined with, whatever later `order` values say. Needs `query`, `where` and `order`                    |
| `members_complete: true`                   | an automatic page chain without `max_pages`: `settled` with `hasMore == false` only after the last page. |
| `insert_on: :server` or `:create`          | the list takes rows from subscription events and answers, or the rows this device creates, see below                              |
| `preload`                                  | the library loads the scope by itself, see below                                                                                  |

`stable_order` keeps places in the cache across restarts. A session reset drops them. New rows from continuation pages append in response order. Other server rows join where their `order` values place them. Rows created on this device go last.

### Insertion into windows

`insert_on: :server` adds rows from events and operation responses to matching loaded windows, and rows of a node reread started by `changed_by` join too. Each parameter needs `where`, `membership` or a `greater_than` match, and the rules combine with AND. Unloaded windows are never created, existing members keep their places and a new member joins the head once.

```ruby
scope :list, insert_on: :server do
  parameter :list, ChatList
  membership list: { contains: :lists }
  query "Chats"
end
```

- `membership list: { contains: :lists }` fits a row in several lists (array field). `membership list: { equals: :status }` fits a scalar field.
- `matches` admits rows of a filter scope: `{ in: [...] }`, `{ equals: literal }` or `{ greater_than: :parameter }` (an `:int` field and an `:int` parameter that no other line uses). A row with a missing field does not match, and a member that stops matching leaves the window.
- A row from an event must carry the membership field after merging the stored row. An answer row without it is written and enters no list. Generation fails for an operation whose response selection lacks the membership field.

### Preload

`preload`, `preload channel: ["lobby", "stage"]` or `preload vibeId: { from: "Vibe.list" }, limit: 40` makes the library load the scope after open with a session, sign-in, reset, return of the network and the end of power saving. Fresh windows send no request, and neither do offline, signed-out and power-saving states. With `from`, a positive `limit` is required, and the windows follow the rows of the source scope.

`preload when: { of: CurrentUser, field: :registrationCompleted }` loads only while the stored singleton has that bool field set to true. A missing row, missing field, null or false closes the condition. The condition can accompany preload parameters and does not restrict application readers.

### Edge values

A connection edge can carry data next to `node`, such as `impressionUuid`. The value belongs to the connection, not to the row: each connection keeps its own value for the same row.

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

Each edge attribute needs `null: true` or `optional: true`, and the scope needs `paginate` and declares one `edge` block. A read gives `nil` for a row outside the connection. An answer of the same connection replaces its values and a row that leaves loses them. Other answers leave them. A change of an edge value alone changes no row and no live list. Values live in the cache and come back with the next load.

### Media library marker and tail

```ruby
scope :media do
  parameter :chatId, :id
  query "chatMedia"
  response from: "node.messages"
  paginate size: 100, direction: :forward
  marker "chatMediaState", from: "node.mediaLibrary", fields: [:revision, :visualCount, :audioCount], sequence: :latestSequence
  tail "chatMedia", after: :afterSequence
end
```

Declare `marker` and `tail` together in a remote scope with cursor pagination. Fields and `sequence` have the non-null type `Int` or `Int64` (a decimal string is accepted). The core passes `upToSequence` to the marker query and the `after` variable to the tail query, which returns the same connection in ascending order. The core confirms a loaded window when the marker counts other than `revision` add up to the rows of the scope in that prefix. A scope that filters the library declares only its own counts.

If a media query returns `NOT_FOUND` for its parent node, the load reports the GraphQL error with its extensions. An app can use that error to handle a parent that was removed or is no longer visible.

### Node reads

A scope with `node: :one` reads one entity with GraphQL `node(id:)`, and `node: :many` reads several with `nodes(ids:)`.

```ruby
model Group do
  attribute :title, :string
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
```

```swift
let group = try await Group.node(groupId)                 // nil when there is no such group
let fresh = try await Group.node(groupId, force: true)    // force a read
let groups = try await Group.nodes(ids)
```

```kotlin
val group = Group.node(groupId)                           // null when there is no such group
val fresh = Group.node(groupId, force = true)
val groups = Group.nodes(ids)
```

- `variables id: :id` binds the parameter to the document variable (`variables momentId: :id` for another name). Each `variables` entry names a variable of the scope's document, every required document variable has a source, and a parameter must fit the type of its variable, or generation fails. Another `node: :one` scope of the same model reads a smaller selection (`Note.transcodeStatus(id)`). The name `nodeId` belongs to the node read.
- The answer lands as a normal server row: only the selected fields change, and merge rules, local fields and unconfirmed local edits stay.
- A plain call inside the freshness time of that id reads the local row and sends nothing, and `force: true` always asks. Overlapping calls for one id share a request, and a forced call within 1 second of the last answer gets that answer.
- The result is nil for `null`, an error `NOT_FOUND` on `node`, a foreign type or a missing id. `null` and `NOT_FOUND` delete the stored row and its `dependent: :destroy` children like a server delete event, and send no mutation. A foreign type writes nothing.
- `nodes(ids)` returns rows in call order, leaves out ids without a row, deletes the row of a `null` entry and always asks the server.
- A read that waits across a session reset ends with `NoSession`.
- The generator refuses a selection under `node` outside `... on`, a `... on` type that does not implement the interface, one `node` selecting two types, and a model attribute that the `... on` type lacks.

## Operations

`operation :name do ... end` inside a model declares a write. Every write applies to the local row at once and goes into a durable queue in the same commit. `create`, `update` and `destroy` are the standard names and generate calls on the type, other names generate calls on the row.

```ruby
model Moment do
  operation :create do
    mutation "CreateMoment"
  end
  operation :share do
    mutation "ShareMoment"
    optimistic false
    parameter :note, :string, null: true
    returns
    response from: "shareMoment.moment"
  end
end
```

| Line                                    | Meaning                                                                                                                              |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `mutation "Name"`                       | the GraphQL mutation. The generator needs `--schema` and the document of every named operation                                       |
| `parameter :note, :string, null: true`  | a value that is sent and never stored. Types: `:string`, `:int`, `:num`, `:bool`, `:id`, `[:id]`, a schema enum or input object      |
| `variables a: :b`                       | only renames: the input field `a` takes the source `b`. Repeating the name is an error                                               |
| `optimistic ...` / `optimistic false`   | the whole local change, or none                                                                                                      |
| `response from: "a.b"` / `:ignore`      | the row in the answer, by default the first field of the operation. `:ignore` skips the answer                                       |
| `upsert Model, from: "a.b"`             | lands the objects at the path as rows of `Model` with the answer                                                                     |
| `refetch :scope, model: Model`          | marks a scope stale after success or after a permanent refusal and rollback, and reloads it when read. Repeat the line per scope  |
| `on_failure :keep`                      | keeps a refused change for `retry` or `discard` instead of the default rollback                                                      |
| `returns`                               | types the answer, see below                                                                                                          |
| `expires_after: 30.seconds` on the line | a duration: how long the operation may wait in the queue before its first send                                                       |

### Inferred input

Each input field (a document variable, or a field of the input object) takes its value by name: the parameter, then the attribute of the model (`create`, `update`), then the row `id` (the name `id` or the model name plus `Id`). A field that is not null, has no default and no source is an error. A nullable field without a source is not sent. When the document has exactly one variable, an input object that the root field takes and no source of its own, the body goes under that variable and its fields bind by name. `wrap` is not a word of the language. A parameter named like the variable takes the whole input object, keeping omitted, null and set nested fields. An `:id` that holds a whole number goes as a JSON number.

A parameter with `null: true` that has no value in a call is not sent: the variables hold no key for it. One operation with optional parameters replaces copies that differ in their set of parameters.

### Optimistic changes

An operation without an `optimistic` line writes each parameter named like an attribute into the row at once and takes it back when the server refuses. Parameters named like a `local` or `derived` attribute and operations with an effect below write nothing. An `optimistic` block is the whole local change, and `optimistic false` means only the server changes the row. A parameter without a value in a call leaves its field alone.

```ruby
operation :like do
  mutation "LikeMoment"
  variables momentId: :id
  optimistic likedByMe: true
end
operation :answer_connection do
  mutation "ConnectionUpdate"
  parameter :decision, ConnectionAction
  optimistic connection: { from: :decision, map: { ACCEPT: "CONNECTED", REJECT: "NEVER_MET" } }
end
operation :react do
  mutation "RecordReact"
  parameter :reacted, :bool
  variables recordId: :id
  optimistic reacted: :reacted,
             visitorIds: { include: { of: CurrentUser, field: :id }, when: :reacted },
             visitsCount: { at_least: 1, when: :reacted }
end
```

- `attr: :parameter` writes the parameter, a literal writes the literal.
- `{ from: :param, map: {...} }` writes the literal of an enum value. The map names every value of the schema enum.
- `include:` puts the id of a row (a singleton, or `row: :parameter`) at the front of a list when the flag is true and takes it out when false. `at_least:` raises a number when the flag is true.
- `by:` and `element:` change one element of an array attribute by its key and keep the others. An element whose attributes other than the key are all empty is removed. A nested object declared with `ref: User, id: :userId` is built from the local row of that model.

### Effects of an operation before the answer

An operation can declare one effect that changes rows or lists before the answer. Use `optimistic false` with `removes_receiver`, `copies_receiver_to`, `joins_scope`, or `leaves_scope`.

```ruby
operation :leave do
  mutation "LeaveClub"
  variables clubId: :id
  optimistic false
  removes_receiver
  response :ignore
end
operation :add_member do
  mutation "AddClubMember"
  optimistic false
  parameter :userIds, [:id]
  variables clubId: :id
  joins_scope :club_members, model: Person, rows: :userIds, with: { clubId: :id }, order: :order, orders: ["JOINED", "OWNER_FIRST"]
  response from: "addClubMember"
end
operation :forward do
  mutation "ForwardLetter"
  optimistic false
  parameter :targetChatId, :id
  variables letterId: :id, chatId: :targetChatId
  copies_receiver_to :targetChatId, field: :chatId, set: { isForwarded: true, authorId: { of: CurrentUser, field: :id } }
  response from: "forwardLetter.letter"
end
```

- `removes_receiver` takes the row out of every list. A refusal puts it back in place.
- `copies_receiver_to :target, field: :chatId, set: {...}` adds a copy with `chatId` set to the argument. `set:` puts a literal, a field of the copied row (`owner: :field`) or a field of a singleton row (`of:`, `field:`) on the copy. The answer replaces the copy, a refusal removes it.
- `joins_scope` adds the rows named by `rows:` to the end of the list, `leaves_scope` removes them. A row that is not cached is skipped. `orders:` lists every value of the order parameter. A refusal restores every list.

Use `removes_row` for a related row identified by an ID field on the receiver. The condition names an enum parameter and one of its values. The operation can also update receiver fields with `optimistic`.

```ruby
operation :leave do
  mutation "MembershipChange"
  parameter :decision, MembershipAction
  parameter :state, MembershipState
  variables memberId: :id, action: :decision
  optimistic membershipState: :state
  removes_row Conversation, id: :conversationId, when: { decision: "LEAVE" }
  response from: "membershipChange.member"
end
```

When the argument matches, the related row disappears from its lists and entity readers. A missing ID or an uncached row has no local effect. A refusal restores the cached row at its former list positions. Confirmation removes dependent rows and files. The operation sends one mutation.

### Queue and failure

After a permanent server refusal and its rollback the library rereads the receiver row through the `node` scope of the model (when it has one) and marks the `refetch` scopes of the operation stale, as after success. A network failure and `Unauthorized` do not trigger it, and single-flight keeps it to 1 request. A write that reaches the server waits in the queue while the network is away, survives a restart and is sent oldest first, one request at a time. See [writing data](writing-data.md) for retries, temporary ids and rollback. `expires_after:` takes a duration. When the library picks a queued operation after that time for its first send, it rolls the optimistic change back and ends the operation `rejected` with the failure kind `expired`. Without it an operation waits as long as needed.

### Answer type

A call reads its answer by the answer type of its document, which the generator infers from the selection under the root field and the schema. Names follow the selected fields (an alias names the field), scalars follow the schema and the `scalar` lines, enums come from the schema, objects become types named by owner and field, lists follow the schema, and `... on T` fragments make a union (select `__typename`). A field of an interface or union type whose selection is `... on T` of one type becomes an object with the fields of `T`, so `node(id:) { ... on Message { readers { ... } } }` answers an object with `readers`. A spread of a document fragment expands in place at any depth. Every field is nullable. A value of another type fails the call with `Invalid`.

- A model operation answers with that type only with a bare `returns`. Without it the call gives the receipt of the row. `returns do ... end` is an error.
- `response from:` of a model operation names the row to land and does not narrow the type.

### Operations without a model

Sign-in, sign-out, a QR code or a price list belong to no row. A top-level `operation` takes only its name and block and always gives the answer type (Swift `Void` and Kotlin `Unit` when the selection is empty). It does not enter the outbox.

```ruby
operation :qr_code_create do
  mutation "QrCodeCreate"
  parameter :kind, :string
  parameter :size, :int, null: true
end
operation :giphy_load do
  query "Giphy"
  response shape: :list
  parameter :type, :string
end
operation :viewer_load do
  query "ViewerLoad"
  upsert Viewer, from: "viewerLoad.viewer"
end
```

```swift
let code = try await Operations.qrCodeCreate(kind: "group", size: 3)
```

```kotlin
val code = Operations.qrCodeCreate(kind = "group", size = 3)
```

- `response shape: :list` makes the call return a list of the selection of one element.
- `upsert Model, from: "path"` lands the rows under the path in one batch before the call returns. A refused landing fails the call and gives no answer.
- `refetch :scope, model: Model` marks that scope stale after the call succeeds and reloads it when read. `model:` is required. A refused call marks nothing.
- `parameter :input, StickerInput` names a schema input object. Pass the operation types to `DB.open` and `DBlayer.open`.

### Uploads

`operation :name, upload: true do` declares a file upload. Each upload operation declares its own `response`, `upsert`, `parameter` and `returns`. The steps (`createBlob`, multipart) are declared once per project in an `uploads do ... end` block, see [uploads](files.md#uploads). `dimensions width: :width, height: :height` names the integer attributes that take the size of the uploaded image. The generator emits `<operationName>Variables` for an upload with parameters. Upload operations get no automatic variable binding: the library sets the upload variables.

## Events

A model follows server events through one shared channel declared at the top level of a file, beside `model`. The app registers its GraphQL document through the `operations` argument of `open`.

```ruby
changes "RecordEvents", from: "recordEvents.entries", area: "topic", kind: "action", id: "recordId"

model Chat do
  attribute :serverRevision, :int, from: "revision"
  server_revision :serverRevision
  scope :node, node: :one do
    parameter :id, :id
    query "ChatNode"
    variables id: :id
  end
  changed_by "MESSAGE", via: :node, payload: "chat"
  changed_by "CHAT", via: :node, removes_on: ["DELETED", "ACCESS_REVOKED"]
end
```

`changes` names the registered subscription and the path to its array. `area`, `kind` and `id` name paths inside each entry. It is declared once across the input files, outside any model, and every `changed_by` reads it.

| Declaration                                         | Meaning                                                                                                                              |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `changed_by "AREA", via: :node`                     | an event of the area rereads the row through the named `node: :one` scope (the only one unless `via` names another)                  |
| `changed_by "AREA", via: :observed`                 | rereads every observed read of the model, the same reread that follows a reconnect. No parameter and no `removes_on`                 |
| `removes_on: ["DELETED"]`                           | kinds that delete the row without a payload. Other kinds reread. Needs a `node: :one` scope                                          |
| `payload: "chat"`                                   | the event carries the row at this path: it lands before any reread. Needs a node scope, not with `id_of` or `via: :observed`         |
| `id_of: { of: Message, field: :chatId }`            | an event that names another model: reread this model by the field the stored row of that model holds. Takes no `removes_on`          |
| `server_revision :field`                            | a required non-local integer attribute: an incoming value below the stored one drops the row, equal or higher applies                |
| `cable_when :field`                                 | on a singleton model, the cable opens only while the stored row has this bool attribute true                                         |
| `removed_by Message, match: { chatId: :chatId }`    | rows of this model whose fields equal the listed fields of a landed row of `Message` are removed in the same commit                  |

An area can repeat on several models and once per form in one model. The event rereads each and gives each model one item on its stream. The generator checks `payload` paths and `id_of` against the schema and the subscription documents: the payload must hold every required field, and a missing payload rejects the event without an HTTP fallback.

Every applied event reaches the app as a model event with `area`, `kind`, `id`, and the rows before and after the commit. A removal has no row after. An event that only rereads stores no row, so before and after are equal. Any model with `changed_by` has a stream `scope.events(Model)`. The first item is `ready` once the source is open, an earlier event is never replayed, and the last reader releases the shared subscription. The journal of a reader holds 256 values: a slower reader gets `lost` with the missed range, and a break of the connection gives `gap` while the library rereads the observed reads of the affected models. A failure ends the stream with `ended(cause)`.

```swift
for await item in try scope.events(Message.self) {
  switch item {
  case .ready: startWork()
  case .event(let event): show(event.before, event.after)
  case .lost(let first, let last): resync(last - first + 1)
  case .gap: break
  case .ended(let cause): report(cause)
  }
}
```

```kotlin
scope.events(Message).events(Message).collect { item ->
    when (item) {
        ModelEventItem.Ready -> startWork()
        is ModelEventItem.Event -> show(item.event.before, item.event.after)
        is ModelEventItem.Lost -> resync(item.last - item.first + 1uL)
        is ModelEventItem.Gap -> Unit
        is ModelEventItem.Ended -> report(item.cause)
    }
}
```

After a reconnect that lost events, the watched reads of the affected models are reread once, fresh or not, unless the scope declares `keep_on_gap: true`. See the [cable protocol](cable-protocol.md).

### Subscriptions of a scope

`subscribe "Name", apply: {...}` keeps a scope current. Without `apply`, an event follows the row upsert. Subscriptions with the same operation, variables and account share one stream, and a scope with `variables` subscribes with the variables of the open window. `debounce: 300.milliseconds` merges bursts.

| `apply:` key                   | Meaning                                                                                                              |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------- |
| `effect: :upsert`              | writes the row. `values:` sets ephemeral attributes, `delete_if: "path"` deletes the row when the path is true, `delete_if: { not: "path" }` when it is present and false |
| `effect: :ephemeral`           | copies the response paths of `fields: { typing: "active" }` into ephemeral attributes and changes no stored column   |
| `effect: :delete`, `:ignore`   | deletes the row, or discards the event                                                                               |
| `matches: { kind: ["A", "B"] }`| runs only when the field is one of the strings. `{ not: ["X"] }` accepts other strings. Missing and null never match |

```ruby
subscribe "PresenceSignal", from: "presenceSignal", id: "userId", apply: { effect: :ephemeral, fields: { typing: "active" }, matches: { kind: ["ACTIVE"] } }
```

Filters run before normalization and writes. All matching declarations apply in one commit. A removal sends no mutation: it closes queued writes without rollback and destroys `dependent: :destroy` children. Unknown effects, keys that do not fit the effect and empty lists are generator errors.

## Errors

Mistakes stop generation with the file and line (`user.dbl:12: unknown key ...`). The same checks run when a database opens. The common refusals:

| Declaration                                                      | Error                                                                                  |
| ---------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `storage: :disk`                                                 | `:disk` is the default, omit `storage`                                                 |
| `wrap` in an operation                                           | unknown word: the wrap comes from the document                                         |
| `returns do ... end`                                             | `returns` takes no block: the answer type comes from the document                      |
| `variables a: :a`                                                | repeats the name, omit it                                                              |
| `values:` on an enum the schema knows                            | the enum takes its values from the schema                                              |
| `changes` inside a model, a second `changes`, `changed_by` alone | unknown word in a model, declared twice, no channel declared                           |
| a scope or operation with `query` or `mutation` and no `--schema`| the operation needs `--schema` and its document                                        |
| a field with no source and no default                            | names the input field                                                                  |
| `keep_on_gap` without `query` or with `refresh_on_foreground`    | needs a `query`, or contradicts                                                        |
| a name that clashes with generated code                          | names the model and the function. Rename the attribute and keep the key in `from:`     |

### GraphQL errors

GraphQL failures expose `extensions` on Swift `DbError.Graphql` and Kotlin `DbException.Graphql`, and a refused mutation carries them in the `failure` of its rejected outcome. `code` is the optional server string of the first error, `transactionDisposition` is `finish`, `keep` or `unknown` (the original text), and `fields` holds every other entry as JSON text. Only `errors[0].extensions` supplies them. The disposition describes the server decision and does not change retry or rollback.

## Check against the server schema

`dblayer-gen --schema schema.graphql models/*.dbl` checks the declarations against the GraphQL schema and exits with code 1 and a file and line for each mismatch. Operations and fragments are read from the `queries`, `mutations`, `subscriptions` and `fragments` directories next to the schema file, or from each `--operations <file or directory>`. Every operation and every scope with `query` needs its document. A spread of an undeclared fragment is an error.

Paths follow the selection of the operation that sends them: aliases, named and inline fragments, union branches and the `edges { node }` or `nodes` of a connection. A union or interface field must select `__typename`. `from:` names the response key, which is the alias when there is one. A path the document does not select is an error that names the declaration, the path, the segment where it ends and the keys found there.

| Declaration                                                                         | Path starts at                          |
| ----------------------------------------------------------------------------------- | --------------------------------------- |
| `response from:` of a scope, a model operation and a standalone operation, `upsert`, `total from:` | top of the operation     |
| `paginate ..., from:`                                                               | the connection                          |
| `from:` and `id:` of `subscribe` and `changes`                                      | top of the subscription, or the row     |

The check reports an attribute or path the schema lacks, a list declared as a single value or the reverse, a nullable schema field crossed without `null: true`, `optional: true` or `default:`, a scalar, enum or object of another type, an enum value the schema lacks, and a scope whose shape differs from its schema field. Attributes marked `local: true`, `:file` attributes and `storage: :local` models are skipped. Schema fields the model leaves out are notes.

A `parameter` of a model operation is checked against the mutation: the input type (or the document variables) must have the field, and the type must fit: `:string` fits `String`, `ID` or a scalar mapped to `:string` or `:date`, `:id` fits `ID`, `Int`, `String`, `:int` fits `Int` or a scalar mapped to `:int` or `:long`, `:num` fits `Float` or `Int`, `:bool` fits `Boolean`, `[:id]` fits a list of `ID`, `Int` or `String`, and the name of an input object or enum fits that type. `null: true` needs a schema variable without `!`, and `null_items: true` a list item without `!`.

### Input objects of the schema

`parameter :input, StickerInput` names a schema input object. The generator writes every input object that an operation names, and every one those fields use, once each, after the models. Keyword field names are escaped and keep their GraphQL keys. An unknown name stops the run. An input object with `@oneOf` is an enum with one case per field, and one key is sent.

A schema enum becomes an enum with an `unknown` case, `JSON` becomes `JSONValue` in Swift and `JsonElement` in Kotlin, and a field without `!` distinguishes omitted, null and a value (`.omitted`, `.null`, `.value(v)` in Swift, `Omitted`, `Null`, `Value(v)` in Kotlin).


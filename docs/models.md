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
| `string`, `num`, `int`, `bool`, `date`, `id` | scalars (`id` accepts a string or a number and stores a string) |
| `enum :name, values: [:a, :b]` | one of the listed values |
| `object`, `array`, `raw` | JSON values |
| `list :name, of: :string` | array of scalars |
| `file :name` | binary attachment, see [files](files.md) |

Field options:

| Option | Meaning |
| ------ | ------- |
| `null: true` | value may be null |
| `optional: true` | key may be absent in server responses |
| `default: value` | default value |
| `index: true` | indexed column (not allowed for `object`, `array`, `raw`, `list`, `file`) |
| `id_from: :parent` | take the id from a nested parent object |
| `as: MomentMedia` | for `object`, `array`, `raw`: the app type the generated model decodes the value to (a `Codable` you provide) |
| `from: "location.lat"` | read the value from a nested path of the response when the field name itself is absent |

`index :a, :b` inside `schema` declares a composite index over fields that are already indexed.

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
| `belongs_to :name` | `model:`, `key:` (required), `touch: true`, `from:` |
| `has_many :name` | `model:`, `key:`, `dependent: :destroy`, `from:` |
| `has_one :name` | `model:`, `key:`, `dependent: :destroy`, `from:` |
| `references :name` | `model:`, `key:` (array of ids), `from:` |

- `from: "visitorsList.nodes"` is the dotted path in a response where the association's objects sit. It defaults to the association name. A list on the way is walked element by element, and a connection (`nodes` or `edges { node }`) gives its nodes.
- Objects found there become rows of the target model. `has_many` and `has_one` rows get the parent id in `key:`. A `references` parent keeps the ids of its rows in `key:`.
- `dependent: :destroy` removes children in the same commit as the parent.
- `touch: true` bumps the parent's revision when a child changes, so observers of the parent refresh.
- The foreign key of a `has_many` or `has_one` must be an indexed field of the target model.
- There are no counter caches. Count with a query: `moment.vibes.count`.

## Scopes and remote relations

`scope :name` declares a named query: `params:`, `where: { field: :param }`, `order: { field: :asc | :desc }`.

`remote :name do ... end` connects a relation to the server. A relation that depends on columns names them: `remote :by_user, params: [:userId] do ... end` loads and lists the rows whose `userId` equals the param. `order: { createdAt: :desc }` sorts the rows locally, otherwise the server order is kept.

| Line | Meaning | Default |
| ---- | ------- | ------- |
| `query Operation` | GraphQL operation name (required) | none |
| `paginate 20` | Relay-style pages of 20, forward | whole response |
| `paginate 20, backward: true` | pages backward | forward |
| `paginate 20, cursor: :afterSequence, from: "lastSequenceNumber", cursor_type: :int` | the server pages by its own variable: the next page sends `afterSequence`, taken from `lastSequenceNumber` of the connection. No page size is sent | Relay `first`/`after` |
| `list` | response is a plain list | single record |
| `fresh_for 5.minutes` | how long a response counts as fresh | `5.minutes` |
| `fresh_for 5.minutes, empty: 30.seconds` | shorter freshness for an empty response | same |
| `max_pages 5` | page limit per load | none |
| `variables statusFilter: :status, includeGifts: false` | name the GraphQL variables: a symbol takes the value of a relation param, any other value is sent as written | every param under its own name |
| `updates Subscription, deleted_by: :field` | keep the relation current with a subscription. Write the line again for each subscription. `removes: true` makes every event delete its row, `debounce: 300.milliseconds` merges bursts | none |
| `on_absent :detach` or `:destroy` | what happens to rows missing from a refreshed response | `:detach` |

An operation name is a constant like `MomentsFeed`, or a string like `"feed"` when the GraphQL name starts with a lowercase letter.

Durations are written `300.milliseconds`, `30.seconds`, `5.minutes`, `2.hours`, `1.day`. Mutations and the `updates` line are described in [server sync](server-sync.md).

## Validation

`dblayer-gen` reports mistakes with file and line (`user.dbl:12: unknown key touch for has_many`). The same checks run in the core when a database is opened: unknown models, missing indexes on foreign keys, duplicate names and invalid combinations.

```sh
cargo run -p dblayer-gen -- spec fixtures/*.dbl     # print the model specification as JSON
```

Example models live in `fixtures/`.

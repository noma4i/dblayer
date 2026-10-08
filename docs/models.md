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

`index :a, :b` inside `schema` declares a composite index over fields that are already indexed.

## Associations

| Word | Options |
| ---- | ------- |
| `belongs_to :name` | `model:`, `key:` (required), `touch: true` |
| `has_many :name` | `model:`, `key:`, `dependent: :destroy` |
| `has_one :name` | `model:`, `key:`, `dependent: :destroy` |
| `references :name` | `model:`, `key:` (array of ids) |

- `dependent: :destroy` removes children in the same commit as the parent.
- `touch: true` bumps the parent's revision when a child changes, so observers of the parent refresh.
- The foreign key of a `has_many` or `has_one` must be an indexed field of the target model.
- There are no counter caches. Count with a query: `moment.vibes.count`.

## Scopes and remote relations

`scope :name` declares a named query: `params:`, `where: { field: :param }`, `order: { field: :asc | :desc }`.

`remote :name do ... end` connects a relation to the server:

| Line | Meaning | Default |
| ---- | ------- | ------- |
| `query Operation` | GraphQL operation name (required) | none |
| `paginate 20` | Relay-style pages of 20, forward | whole response |
| `paginate 20, backward: true` | pages backward | forward |
| `list` | response is a plain list | single record |
| `fresh_for 5.minutes` | how long a response counts as fresh | `5.minutes` |
| `fresh_for 5.minutes, empty: 30.seconds` | shorter freshness for an empty response | same |
| `max_pages 5` | page limit per load | none |
| `updates Subscription, deleted_by: :field` | keep the relation current with a subscription | none |

Durations are written `30.seconds`, `5.minutes`, `2.hours`, `1.day`. Mutations and the `updates` line are described in [server sync](server-sync.md).

## Validation

`dblayer-gen` reports mistakes with file and line (`user.dbl:12: unknown key touch for has_many`). The same checks run in the core when a database is opened: unknown models, missing indexes on foreign keys, duplicate names and invalid combinations.

```sh
cargo run -p dblayer-gen -- spec fixtures/*.dbl     # print the model specification as JSON
```

Example models live in `fixtures/`.

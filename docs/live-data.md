# Live data and queries

Every read of a model is live. A read gives you the current value and a stream of changes. There is no snapshot-only read and no separate observe call: connecting to data is reading it.

## What a change contains

| Field | Meaning |
| ----- | ------- |
| `revision` | Commit revision the state reflects. It only grows for one observer |
| `rows` | Full current result |
| `diff.inserted`, `removed`, `updated`, `moved` | Row identities that changed |
| `diff.replaced` | Pairs `(old id, new id)`, for example a temporary id replaced by the server id, so a UI can keep the same element |

The first delivery contains every row as `inserted`.

## Guarantees

- One commit is one visible transition: a parent with its cascade-deleted children, a replaced list and an id swap appear together.
- Deliveries for one observer are in revision order. Older state never arrives after newer state. Several quick commits may be merged into one delivery, and the last state always arrives.
- If nothing the query reads has changed, nothing is delivered.
- After a write call returns, any new read sees the write.
- Dropping the subscription stops delivery at once.
- Listeners are called from library threads. Hop to your main thread yourself.

## Queries

```swift
Moment.find(id)
Moment.where(Moment.userId == id).order(Moment.createdAt.desc).limit(20)
Moment.recent                       // a named scope
moment.author                       // belongs_to
moment.vibes                        // has_many
moment.vibes.count                  // counted by SQLite, no cache field
```

Query description is plain data (`model`, `where`, `order`, `limit`, `offset`); the core compiles it to SQL. Indexed fields compare as real columns, other fields through JSON extraction.

Null handling follows one rule: a row with a null field matches `!(field == value)`, like a person would expect.

## Remote relations are live too

Connecting to a relation that has a `remote` block loads it when its data is stale, keeps it fresh through its `updates` subscription while someone reads it, and lets go a while after the last reader leaves. User gestures stay explicit: `refresh()` and `loadMore()`.

See [server sync](server-sync.md) and [threading](threading.md).

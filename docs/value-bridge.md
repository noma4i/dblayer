# Value bridge

A stream gives your screen values, not live objects.
The core sends a snapshot, then deltas keyed by row id.
Your screen keeps its own projection of those values.
Swift uses Observation. Kotlin uses a `StateFlow`.

## Account scope

Make one `AccountScope` after the sign-in.
It holds the session epoch. Every stream and command of the account carries it.
A sign-out or an account switch changes the epoch.
The streams of the old epoch end with one outcome.
The root of the app builds its screens again for the new scope.
An object of the old account does not live on in the new one.

```swift
let scope = try DB.account()
let user = Projected<CurrentUser>(scope.singleton(CurrentUser.self))
let thread = Projected<Message>(scope.stream(Message.thread(containerId: id)))
```

```kotlin
val scope = AccountScope.current()
val user = Projected(scope.stream(CurrentUser.current), CurrentUser, screenScope)
val thread = Projected(scope.stream(Message.thread(id)), Message, screenScope)
```

## Values

| Value | Meaning |
| --- | --- |
| `snapshot` | The whole result at a revision. It is the first value. |
| `delta` | Rows changed since `base`: upserts, removals, positions, replaced ids. |
| `state` | The load state changed and the rows did not. |
| `event` | One event of a model. |
| `lost` | The events at the positions `first` to `last` are gone: the reader was slow or the event could not be read. |
| `ended` | The stream ended by itself: session ended, database closed, query invalid. |

The core keeps the values of a stream in one journal of at most 256 values. The sink of a stream only hears that values wait, and the reader takes them one by one with `readStream`. Swift and Kotlin do this and keep no buffer of their own.
Every value names its stream, its epoch and its position in the journal. A `lost` value takes the place of the positions it names.
A full journal of a list drops the values that a new snapshot replaces and sends that snapshot. A full journal of events keeps the newest events and one `lost` range for the oldest ones.
The end of a stream is kept apart from the journal and comes once, after every other value. When the session ends or the database closes, the values of the old account that were not read are dropped and only the end follows.
Each state says whether the first load `settled` and up to which `revision` of the database its rows are read.
A projection ignores a value of another epoch.
A delta whose `base` is not the held revision asks for one new snapshot.
`Projected` does that for you: it calls `resync`.

## Reading a projection on screen

The first delivery of a stream is a snapshot. It carries no list of edits: `takeEdits()` returns `nil` and the screen reads `rows` again.
Edits come only with deltas, as the steps from the rows before to the rows after.
A delta that a projection cannot apply gives one new snapshot per stream, also when a screen follows several streams.

On iOS 17 a SwiftUI view reads `Projected` through `@Observable`: it tracks the properties it reads, and `withObservationTracking` reports the next change.
`Observations { }` needs iOS 26. Code that targets iOS 17 does not use it.
On Android the screen collects the `StateFlow` of the projection.

## Offline and reconnect

Only a message send and a media upload wait for the network.
A durable send whose attempt failed on the network goes out at once when the network returns, without a wait for its backoff.
While the network is away `refresh`, `loadMore` and a one-off load fail at once with the network error, and the stream state shows it.
A list stream opened with `refreshOnConnect` loads again once each time the network returns. The app writes no refresh for that moment.

An action that has a mutation and no `optimistic` line returns after the server answered.
It returns the row at the root of the answer. When that row has another id than the receiver, the receiver row stays unchanged and the answer row appears in its own chat.
An action with an `optimistic` line shows its change at once as unsent and ends as synced after the answer.

## Reading and stopping

A stream lives while its reader lives.
Release the `Projected` model, cancel its task or close its scope.
The core then drops the subscription. Nothing is delivered after that.

The first value comes from the cache of the core when it holds the query.
The call that opens a stream never reads the database on your thread.

## Every path is a stream

Queries, counts, related rows, a row by id, model events and upload status use the same values. See [live data](live-data.md).
One owner in the core serves each stream. The wrappers keep no registry, no lock and no cache for it.

## Calls on a stream

| Call | Result |
| --- | --- |
| `refresh()` | Loads the relation again. |
| `loadMore()` | Loads the next page. |
| `loaded()` | On `Projected`: returns when the projection shows the settled state of the first load. With rows it returns for any error of the load, and the state keeps the error in the `Loaded` phase. It throws the error of the `Failed` phase, which the core gives only without rows. The outcome of an own request comes from `beginLoad(...).result()`. |
| `shown(revision)` | On `Projected`: returns when the projection shows the database at that revision or later, also when the write of that revision changed none of its rows. |

A call that outlives its epoch ends as `Interrupted`.
Server work without a session or transport fails with `NoSession`.
Its late answer changes nothing, and a request of the stream does not start in the next epoch.
A delivery that was prepared and not yet handed over is withdrawn by the cancel of the reader and by the end of the epoch.
A stream whose source cannot be connected ends once with the `invalid` outcome.

## Sending a message

`scope.send` writes the row at once and queues it for the server.
It works without a network and after the app closed.
The row shows in the stream with its sync state.
The server confirms it and the temporary id becomes the real id at the same position.
The receipt carries the optimistic row and the revision of the local commit.
`outcome()` waits for the one result of that operation: confirmed, rejected, delivery unknown or interrupted.

```swift
let sent = try await scope.send(Message.self, values: ["text": .string("hi")])
let outcome = try await sent.outcome()
```

```kotlin
val sent = scope.send(Message, buildJsonObject { put("text", "hi") })
val outcome = sent.outcome()
```

A command for an epoch that has gone is refused.
A send needs a signed-in session.
Pending commands of an ended account end with it. They do not go to the next account.
Only message sends and media uploads survive the network and the app.
Any other action needs the network: it fails at once while the network is away and is rolled back if the network goes away before it is sent.

## Commands of an account

Every command, one-off read and blob call carries the epoch of an account.
Pass the scope of the account: a call of an account that left ends as `Interrupted` and changes nothing for the next account.
Without the argument the call belongs to the account that is signed in when it is called.
A create that the server answers, such as a new group, needs the network and returns a receipt with the optimistic row. Its `outcome()` carries the confirmed row.

```swift
let group = try await Chat.create(name: "Friends", userIds: ids, in: scope).outcome().confirmedValue
let rows = try await Chat.list.read(in: scope)
```

```kotlin
val group = Chat.create(name = "Friends", userIds = ids, account = scope).outcome().confirmedValue
val rows = Chat.list.read(scope)
```

## What the work costs

An idle stream sends no request and wakes nothing.
A stream that ended leaves no observer: a write after its last reader left reads nothing. `retainedObservers` counts only the observers of an explicit `warm`.
A reader that falls behind finds at most 256 values in the journal. A list reader then gets one new snapshot, an events reader gets one `lost` range before the events that were kept.
A change of one row sends one row and decodes one row.
The number of subscriptions is the number of readers.

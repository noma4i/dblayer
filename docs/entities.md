# Entities

An entity is the live value of one record, read by id. Use it for records that a screen refers to but does not list: the author of a moment, the people in a group, the last message of a group, the current user.

There is one entity per id. Every reader of an id reads the same object, so two places never show two different states of one record. A change of a record invalidates only the readers of that id. A screen that shows 50 moments of 10 authors redraws 5 views when one author changes, not 50.

## Reading

```swift
let author = User.entity(moment.userId)   // hold it while the author is shown
Text(author.value?.fullName ?? "")
```

```kotlin
val author = User.entity(moment.userId)   // hold it while the author is shown
val name by author.value.collectAsState()
```

`value` is the newest known state of the record. The first reader receives a cached row immediately when it is in memory. A second reader of a live id receives the same object and opens no new subscription.

A model with a `node: :one` scope can ask the core to read the record when a reader appears. `fetch:` takes `none` (the default), `stale` and `force`. `stale` reads the node only when the row is missing or its freshness ran out. `force` reads it for every new first reader, so opening a screen again sends a new request. A second reader of a live id sends nothing, and overlapping reads of one id share one request. The row lands like any server row. The outcome of the read shows in `read` next to `phase`: `notLoaded` until the read ends, then `loaded`, or `failed` with `offline` or `failed(message)`. A read that found no network repeats once when the network returns and then moves `read` to `loaded`. A model without a node scope has no `fetch:` overload.

```swift
let group = Group.entity(id, fetch: .force)   // one node read per new first reader
Text(group.read == .notLoaded ? "Loading" : "Loaded")
```

```kotlin
val group = Group.entity(id, fetch = EntityFetch.FORCE)   // one node read per new first reader
val read by group.read.collectAsState()
```

`waitForInitialLoad()` waits for the first load of an entity. It reuses the read the entity already started and sends no request of its own, which is the first read of `fetch:` for an id entity and the live read of the relation for a singleton. It returns when the read has ended or when a value is already there, so a loaded entity returns at once. It throws the error of a failed read (Swift: `EntityRefreshError`, Kotlin: `EntityReadException` with `error`) and an interrupted error when the database closes. An entity without `fetch:` has no read to wait for and returns at once.

```swift
let me = CurrentUser.entity()
try await me.waitForInitialLoad()   // no second request next to the load of the entity
```

```kotlin
val me = CurrentUser.entity()
me.waitForInitialLoad()   // no second request next to the load of the entity
```

To read the changes of an entity in code, not in a view, use `changes()` in Swift and collect `value` in Kotlin. Both hold the entity while they read, so no other reference is needed.

```swift
for await value in User.entity(id).changes() { render(value) }   // current value first, then each newer one
```

```kotlin
User.entity(id).value.collect { render(it) }   // current value first, then each newer one
```

The Swift stream gives the current value, then updates. A reader that falls behind gets only the newest value. Many readers of one entity open no new subscription. Cancelling or dropping one stream leaves the others. The stream ends when the database closes. A `nil` value means the record is absent or not loaded: `phase` tells which, and carries a read error.

Every entity has a `phase`: `opening` until the core answers, then `present` or `absent`. `failed(error)` keeps the last value and tells that the core could not lease the id, read the row or decode it on this build. `closed` follows `DB.close()`. Entity creation reads only memory. It never reads SQLite synchronously. A cache miss stays empty until the first delivery, even if the record exists on disk.
Cached and retained values have `isStale` set until a fresh delivery arrives. An older cached row never replaces a newer live or retained value.
A row that arrives after its entity reaches the entity. When the core cannot read the record for a lease, it counts the failure, keeps the id and reads it again on the next commit or the next lease of that id. `DBlayer.entityCensus()` and `DB.entityCensus()` show the facts told, the deliveries skipped as not newer, and the failed reads. `DBlayer.entityLeaseFailures()` and `DB.entityLeaseFailures()` count the lease calls that the platform registry could not hand to the core and the rows that failed to decode for an entity. Both write a line to the library log.
Lists that already loaded their rows can create entities directly, with no extra prefetch call:

```swift
let entity = Moment.entity(momentId)
let firstValue = entity.value
```

```kotlin
val entity = Moment.entity(momentId)
val firstValue = entity.value.value
```

## The one row of an account

A model that holds one row for the whole account, such as the current user, has an entity without an id. The generator declares it for a model that has exactly one global relation answered as a single record.

```swift
let me = CurrentUser.entity()   // no id: the app does not know it before the row has loaded
Text(me.value?.fullName ?? "")
```

```kotlin
val me = CurrentUser.entity()   // no id: the app does not know it before the row has loaded
val name by me.value.collectAsState()
```

There is one such entity per process and one core subscription for it, however many screens read it. Reading it opens 1 live read of the relation behind it, so a stale relation loads once and the relation updates subscription starts, like any live read of a relation. `value` is empty until the row has loaded. `id` is empty until then and holds the id of the row after. The entity is not leased: it lives as long as the database handle and never reaches the retention rules below, so no reader can drop its subscription. Logout empties the entity, tells its readers and cancels its subscription. Reading the same entity again leaves it stopped until the next sign-in. Reading it after opening a new database starts its subscription on that database. A delivery of the old subscription that arrives later writes nothing. `resume()` is the manual refresh: repeated calls keep the active subscription and reload stale relations.

## Lifetime

Nothing to start, stop or release. The library keeps the record current as long as the entity object is alive and lets go of it when the last holder drops it (Swift: when the object is freed; Kotlin: when it is no longer reachable). A delivery that was already on its way when the entity was dropped is ignored.

A dropped entity can keep its last value for 120 seconds, with at most 200 values per model and 8 MiB estimated across models. Rust applies these limits. Swift and Kotlin store the selected typed values. Oversized values are not retained. Limits apply to released entities, not values still held by screens.

Requesting a retained id returns its value with `isStale` set and reads the record again. A fresh delivery clears `isStale`. Expired entries are removed on the next retention operation. Kotlin processes garbage-collection notifications on one waiting thread, without polling.

`entity(id)` for many ids sends one batch to the database observer, however many entities are created in one turn.

## Order

The core owns every transition of an entity in one state machine (`EntityMachine`): seed, revision order, stale, failure, the outcome of the remote read (`read`), reset and close. Swift and Kotlin keep the typed value and show the verdict of the core. A closed entity never changes again. A failure keeps the accepted value. A reset of a singleton starts a new feed, and a delivery or a failure of an older feed changes nothing.

Every delivery carries the revision of the database. An entity ignores a delivery with a lower revision than the one it shows, whatever the order in which they arrive. A record that was deleted arrives as an empty value. A reset of the data (logout, `reset`) tells every leased entity that its record is gone. The entities keep their leases and receive the rows of the next account.

Native callbacks hold entity sources weakly. Releasing a source releases its observer without a separate cleanup call.

The process has one database. Entities, lists and subscriptions belong to it for the life of the process, and the platform code does not follow a change of database. `DB.close()` and `DBlayer.close()` end it: every held entity becomes closed (`phase` is `closed`, empty value, no further delivery) and the next `open` starts empty. After `close` and before `open`, `DB.database` throws `DatabaseNotOpen` and `DBlayer.database` throws `IllegalStateException`, both with the text "database is not open; call DBlayer.open once at app start". Every call that reads the database throws it, and a live source throws it from `stream()` (Swift) or from collecting `flow` (Kotlin).

## Ending a session

`try await DB.logout()` (Swift) and suspending `DBlayer.logout()` (Kotlin) clear the data and drop the kept values. Wait for completion before signing in to another account. Every entity that a screen holds stays the same object, goes back to empty and receives the rows of the next account. The core has told every leased entity that its row is gone before logout returns, and the empty values reach the main actor before the code after `await` runs. Logout deletes every queued, refused and unconfirmed operation.

Signing in again restarts the stopped singleton subscriptions. The app calls nothing after `sessionCall`:

```swift
try await DB.sessionCall("SignIn", body: credentials)
let me = CurrentUser.entity()
```

```kotlin
DBlayer.sessionSend("SignIn", credentials)
val me = CurrentUser.entity()
```

## Cost

A commit of one record costs one read and one delivery per observer of the model, and one decode per changed row. A record nobody holds costs nothing. Idle entities cost no timers and no requests. `DB.subscriptionCensus()` and `DBlayer.subscriptionCensus()` report the live subscriptions, so a test can assert how many a screen opens.

A read that fails keeps its ids dirty. The core reads them again at the next batch of events of any model, so a delete or an update is told later and never lost. Nothing runs on a timer. A reset that ends a poisoned store tells every held id as removed. A delivery names an id in `rows` or in `removed`, never in both. The counters `entity_facts_told`, `entity_position_skips` and `entity_reads_failed` of the core show how many facts were told, skipped because the read was not newer, and failed to read.

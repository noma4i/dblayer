# Entities

An entity is the live value of one record, read by id. Use it for records that a screen refers to but does not list: the author of a moment, the people in a chat, the last message of a chat, the current user.

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

`value` is the newest known state of the record. It is empty until the first delivery and when the record does not exist. Reading is synchronous. A second reader of an id that is already live gets the value at once and opens no new subscription.

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

There is one such entity per process and one core subscription for it, however many screens read it. `value` is empty until the row has loaded. `id` is empty until then and holds the id of the row after. The entity is not leased: it lives as long as the database handle and never reaches the retention rules below, so no reader can drop its subscription. A newly opened database and `logout()` empty it at once, tell its readers and open the subscription again. A delivery of the old subscription that arrives later writes nothing. The subscription is the relation that holds the row, so `resume()` keeps reloading it while the app runs.

## Lifetime

Nothing to start, stop or release. The library keeps the record current as long as the entity object is alive and lets go of it when the last holder drops it (Swift: when the object is freed; Kotlin: when it is no longer reachable). A delivery that was already on its way when the entity was dropped is ignored.

A dropped entity keeps its last value for 120 seconds, at most 200 values per model. Asking for the id again within that time returns the kept value at once with `isStale` set, and the record is read again so the fresh state replaces it. `isStale` turns false with the first fresh delivery. After the time or the count runs out the value is gone and the entity starts empty.

`entity(id)` for many ids sends one batch to the database observer, however many entities are created in one turn.

## Order

Every delivery carries the revision of the database. An entity ignores a delivery with a lower revision than the one it shows, whatever the order in which they arrive. A record that was deleted arrives as an empty value. A reset of the model (logout, `reset`) empties every entity.

## Ending a session

`DB.logout()` (Swift) and `DBlayer.logout()` (Kotlin) clear the data and drop every entity, lease and kept value of every model. The entity of an account row stays the same object and goes back to empty.

## Cost

A commit of one record costs one read and one delivery per observer of the model, and one decode per changed row. A record nobody holds costs nothing. Idle entities cost no timers and no requests. `DB.subscriptionCensus()` and `DBlayer.subscriptionCensus()` report the live subscriptions, so a test can assert how many a screen opens.

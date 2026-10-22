# Cloud sync of local data

Local data follows the user between devices that share one account. The core merges, journals and retries. The app supplies a cloud adapter with 3 calls.

## What syncs

- Models declared `storage: :local` sync the attributes listed under `cloud` in their model spec.
- On every other model only attributes declared `local: true` can be listed.
- Server data never enters the cloud. GraphQL values, the outbox and loaded pages stay on the device.
- An attribute of type `file` cannot be listed. The spec is refused with an error that names the attribute.
- iOS devices share through the CloudKit private database. Android devices share through Google Drive app data. The two platforms do not sync with each other.

## How changes merge

Each write to a listed attribute gets a hybrid logical clock stamp in the same transaction as the row. The journal keeps the unsent versions in SQLite columns, so a crash between the write and the send loses nothing.

- Scalars, enums and dates merge per attribute. An object, an array or a raw value merges as one value with the stamp of the attribute.
- The winner follows the `merge` rule of the attribute: `:present`, `:non_empty`, `:positive`, `:monotonic` or a ladder of tiers decide first. Without a rule, or with equal rank, the later stamp wins.
- Two edits of different attributes of one row both survive.
- Delivery order and repeats do not change the result.
- A delete wins over any edit of the same row. The deleted row id never comes back, a restore needs a new id. Delete marks have no expiry and are not compacted.

## The adapter

The app implements `CloudIo`. It moves bytes and reports errors. It never merges, retries or orders.

| Call | Job |
| --- | --- |
| `account` | Report `Available`, `SignedOut`, `NeedsConsent` or `Restricted` |
| `pull(cursor, limit)` | Return changes after an opaque cursor, the next cursor and `more` |
| `push(changes)` | Store the changes. Repeating a push is safe |

Errors are `Offline`, `Throttled(retryAfterMs)`, `Unavailable`, `NeedsAuthorization`, `QuotaExceeded` and `Rejected`.

```swift
final class Drive: CloudIo {
    func account() async throws -> CloudAccount { .available }
    func pull(cursor: Data?, limit: UInt32) async throws -> CloudPage { /* read the next page */ }
    func push(changes: [CloudChange]) async throws { /* store the changes */ }
}

let link = try database.connectCloud(io: Drive())
```

```kotlin
class Drive : CloudIo {
    override suspend fun account(): CloudAccount = CloudAccount.AVAILABLE
    override suspend fun pull(cursor: ByteArray?, limit: UInt): CloudPage { /* read the next page */ }
    override suspend fun push(changes: List<CloudChange>) { /* store the changes */ }
}

val link = database.connectCloud(Drive())
```

## When work happens

The core calls the cloud only for these events: the connection starts, a listed attribute changes locally, the app calls `remoteChanged`, or the connection returns after an error. An idle app makes no cloud calls and keeps no timer.

Call `remoteChanged` when the app returns to the foreground and when a CloudKit push arrives. On Android this is the only way to learn about changes of other devices, because Drive cannot push to a sleeping device.

Failures retry with the shared backoff and wait for connectivity. `Throttled` waits at least its retry-after. `NeedsAuthorization` and `QuotaExceeded` stop until the app calls `remoteChanged` or `syncNow`. The journal stays on disk.

## State

`CloudLink.state` is separate from the sync state of a record. It is one of `Idle`, `Pending`, `Syncing`, `Offline`, `NeedsAuthorization`, `QuotaExceeded`, `NeedsInitialMerge` and `Failed`. `listen` reports every change.

## First connection

Rows written while `cloud` was listed carry stamps from the first write, so a new device merges by attribute with no extra step. Rows that exist without stamps (a model that gained `cloud` after it held data) stop the sync in `NeedsInitialMerge`. The app calls `resolveInitialMerge` with `UseLocal` to make those rows win, or `UseCloud` to let any cloud value win. Equal values need no choice.

## Limits

- A `local: true` value written before its server row arrives is not sent until the row exists.
- Nested local keys inside an object (`merge: { key: :local }`) do not sync. List the whole attribute on a local model.
- Files do not sync.

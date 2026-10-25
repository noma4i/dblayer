# Cloud sync of local data

Local data follows the user between devices that share one account. The core merges, journals and retries. A ready adapter per platform moves the bytes.

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

## The adapters

The package ships a ready adapter for each platform. The app does not implement `CloudIo` and does not run a sync lifecycle. It passes one object in one call.

| Platform | Call | The app passes |
| --- | --- | --- |
| iOS | `database.connectCloudKit(container:)` | The `CKContainer` |
| Android | `database.connectDrive(activity)` | An `Activity` for the consent screen |

The adapter moves bytes and reports errors. The core merges, retries and orders. `CloudIo` is the protocol the adapters implement: `account`, `pull(cursor, limit)` and `push(blobs)`.

A blob is a key and opaque bytes. The core builds both. The key is a hash of replica, model, row and attribute, so a device overwrites only its own versions and a repeated push is safe. The bytes carry a format version, and the core rejects a version it does not know. Adapters store the pair as given and return it unchanged.

```swift
let link = try database.connectCloudKit(container: CKContainer(identifier: "iCloud.com.example.app"))

func application(_ application: UIApplication, didReceiveRemoteNotification userInfo: [AnyHashable: Any]) async -> UIBackgroundFetchResult {
    CloudKitLink.receive(userInfo) ? .newData : .noData
}
```

```kotlin
val link = database.connectDrive(this)
```

### CloudKit

- Records live in the private database, in the custom zone `DBlayerCloud`. One record holds one blob. The record name is the key. The one data field `bytes` holds the bytes. A blob larger than 900,000 bytes goes into the `asset` field as a `CKAsset`, because CloudKit limits the data of a record to 1 MB and does not count assets ([CKRecord](https://developer.apple.com/documentation/cloudkit/ckrecord)).
- The cursor is the server change token. A token CloudKit no longer accepts restarts the fetch from the beginning. Repeated changes do not alter the merge result.
- The adapter saves one `CKDatabaseSubscription` with silent pushes. The library registers for remote notifications and calls `remoteChanged` when the app returns to the foreground.
- The account maps from `CKAccountStatus`: `noAccount` is `SignedOut`, `restricted` is `Restricted`, `temporarilyUnavailable` is `NeedsConsent`.
- Silent pushes reach only the app delegate. The one call the app makes is `CloudKitLink.receive(userInfo)` in `application(_:didReceiveRemoteNotification:)`. It returns `false` for any other notification.
- The app target needs the iCloud capability with CloudKit and the container, the Push Notifications capability and the `remote-notification` background mode. The container is created once in Xcode and cannot be renamed or deleted.
- A schema created in the Development environment goes to Production with Deploy Schema Changes in the CloudKit Console before the app ships.

| CloudKit error | `CloudError` |
| --- | --- |
| `networkUnavailable`, `networkFailure` | `Offline` |
| `requestRateLimited`, `zoneBusy`, `serviceUnavailable` with a retry-after | `Throttled` with the retry-after |
| `serviceUnavailable` without it, `internalError`, `serverResponseLost`, `accountTemporarilyUnavailable` | `Unavailable` |
| `notAuthenticated`, `permissionFailure`, `managedAccountRestricted` | `NeedsAuthorization` |
| `quotaExceeded` | `QuotaExceeded` |
| `partialFailure` | The most severe error of the failed records |
| Any other code | `Rejected` |

### Google Drive

- Files live in the Drive `appDataFolder` under the `drive.appdata` scope. No other app and no Drive screen shows them.
- A push writes one immutable file per blob, named by the key. A device never rewrites a file, so devices do not conflict. A repeated push can add a file with the same content, which the merge ignores.
- The cursor is the `changes` page token. The first pull reads all files of the folder and then continues with the change list. The page size is the `limit` of the pull, capped at 100 files.
- Authorization uses `AuthorizationClient`. When Google needs the consent of the user, the library opens the consent screen through the `Activity` and reports `NeedsConsent`. The next return to the foreground asks again.
- The Android OAuth client in Google Cloud needs the package name and the SHA-1 of the signing key. Debug and release keys need one client each.
- The library listens to the process lifecycle and calls `remoteChanged` when the app returns to the foreground. Drive cannot wake a sleeping device, so this is the only way to learn about remote changes.

| Drive reply | `CloudError` |
| --- | --- |
| `401`, `403` with `insufficientPermissions` | `NeedsAuthorization` |
| `403` with a rate-limit reason, `429` | `Throttled` with `Retry-After` |
| `403` with `storageQuotaExceeded` | `QuotaExceeded` |
| `408`, `5xx` | `Unavailable` |
| Network failure | `Offline` |
| Any other status | `Rejected` |

## When work happens

The core calls the cloud only for these events: the connection starts, a listed attribute changes locally, the app calls `remoteChanged`, or the connection returns after an error. An idle app makes no cloud calls and keeps no timer.

The adapters call `remoteChanged` when the app returns to the foreground, and `CloudKitLink.receive` calls it when a CloudKit push arrives. The app calls `remoteChanged` on the link only to force a pull.

Failures retry with the shared backoff and wait for connectivity. `Throttled` waits at least its retry-after. `NeedsAuthorization` and `QuotaExceeded` stop until the app calls `remoteChanged` or `syncNow`. The journal stays on disk.

## State

`CloudLink.state` is separate from the sync state of a record. It is one of `Idle`, `Pending`, `Syncing`, `Offline`, `NeedsAuthorization`, `QuotaExceeded`, `NeedsInitialMerge` and `Failed`. `listen` reports every change.

## First connection

A full database reset clears local cloud values, stamps, delete marks, pending changes, replica identity and the pull cursor. Remote cloud data remains unchanged.
Cloud replies retain the database generation captured before their request. A reset rejects replies from the previous generation before they write rows or synchronization state.

Rows written while `cloud` was listed carry stamps from the first write, so a new device merges by attribute with no extra step. Rows that exist without stamps (a model that gained `cloud` after it held data) stop the sync in `NeedsInitialMerge`. The app calls `resolveInitialMerge` with `UseLocal` to make those rows win, or `UseCloud` to let any cloud value win. Equal values need no choice.

The choice applies to rows selected when the call starts. `UseLocal` reads their latest values after fetching remote clocks, preserving edits made during that fetch.

Cloud values received before a server row are kept until the row arrives. Explicit pending local values take priority and enter the outgoing journal when applied.

## Limits

- A `local: true` value written before its server row arrives is not sent until the row exists.
- Nested local keys inside an object (`merge: { key: :local }`) do not sync. List the whole attribute on a local model.
- Files do not sync.
- Drive files are never compacted, so the folder grows with every push.

# Cloud sync

Cloud sync is off by default. Pass a cloud configuration when opening the database to opt in.
Without one, the database performs no cloud I/O or cloud migration.
To let users opt in from settings later, supply the configuration with `initiallyEnabled` set to false.

## Configure cloud sync

For a standalone database, choose a stable namespace shared by installations of the same app.
On Apple platforms, enable iCloud and CloudKit in the app's signing capabilities and select the container.
The default container is used when `container` is omitted.

```swift
import CloudKit

let cloud = CloudConfig.cloudKit(
  application: .standalone(namespace: "notes"),
  container: CKContainer.default(),
  initiallyEnabled: false
)
try await DB.open(modelsJSON: modelsJSON, uploadTaskTitle: "Uploading", cloud: cloud)
```

```kotlin
val cloud = DriveSync.configuration(
    application = CloudApplication.Standalone(namespace = "notes"),
    authorization = authorization,
    directory = context.cacheDir,
    replica = replica,
    relay = null,
    initiallyEnabled = false,
)
DBlayer.open(context, modelsJson = modelsJson, uploadNotification = notification, cloud = cloud)
```

For Android, obtain Google authorization for the `https://www.googleapis.com/auth/drive.appdata` scope.
Drive stores app data in the private `appDataFolder`. Supply a `StateFlow<DriveAuthorization>` as `authorization`.
Use `Available(key, accessToken)` for a confirmed Google account and its current access token.
Publish `SignedOut` for a confirmed sign-out and `Unavailable` for temporary unavailability.
Keep `key` stable for the same Google account and update the flow when the token or account changes.
Give each installation a stable, nonempty `replica` string and a writable temporary `directory`.

An optional HTTPS `relay` enables foreground Drive change notifications.
See [Drive notifications](drive-notifications.md) for the server and its deployment.
Without a relay, cloud sync reads Drive when it starts or reconnects.

## Turn cloud sync on or off

Use `configured` to decide whether to show a toggle. Use `enabled` for its selected value.
The initial value applies until the first saved choice. Later opens use the saved choice.
Await the command before showing it as complete. Disable completes after the active work and account observation stop.

```swift
let enabled = try await DB.setCloudEnabled(true)
```

```kotlin
val enabled = DBlayer.setCloudEnabled(true)
```

Pass false to disable cloud sync. SQLite reads and writes continue.
Pending confirmed changes and the cursor stay on the device. Enabling the same accounts resumes the queue.
Confirmed local changes made while disabled are included when the same domain resumes.
The account change rules below apply if a different cloud account connects.

## Status

Read the current snapshot or observe changes while the database is open.

```swift
let status = try DB.cloudStatus()
for await snapshot in try DB.cloudChanges() {
  print(snapshot)
}
```

```kotlin
val status = DBlayer.cloudStatus()
DBlayer.cloudChanges().collect { snapshot ->
    println(snapshot)
}
```

The first change is the current snapshot. A slow reader receives the latest snapshot.
Without cloud configuration, the snapshot is Disabled and the changes stream ends after it.
Closing the database ends the stream. Cancelling a reader leaves other readers active.
The snapshot reports configuration, effective enablement, cloud phase, current activity, and record issues.
Use the phase for the overall state and activity for work such as an initial read, a send, or a retry wait.
A configured database continues to emit status changes while disabled.
TooLarge affects only the record whose serialized payload exceeds 900,000 bytes.
Other records continue syncing. A newer version clears the issue and can be sent.

| Activity | Meaning |
| --- | --- |
| Idle | No cloud work is in progress |
| Observing account | The library is attaching the platform account observer |
| Reading | The library is reading cloud changes |
| Bootstrapping | The library is preparing confirmed local rows for the first send |
| Sending | The library is processing a batch of queued changes |
| Waiting to retry | A failed operation is waiting for its retry or the next connection event |
| Stopping | The library is retiring a generation or detaching account observation |
| Erasing | The library is clearing data for a replaced cloud account |

## See what syncs

List the declared models to show which ones are eligible. Check a record by its model and row ID.
Use the application model name. The result contains status only.

```swift
let models = try await DB.cloudModels()
let record = try await DB.cloudRecordStatus(model: "Note", rowID: note.id)
```

```kotlin
val models = DBlayer.cloudModels()
val record = DBlayer.cloudRecordStatus(model = "Note", rowId = note.id)
```

| Record status | Meaning |
| --- | --- |
| Excluded | Memory storage or a temporary row ID |
| Not attached | An eligible record has no readable cloud domain yet |
| Missing | The current domain has no metadata and the row is absent locally |
| Waiting | The row exists locally and has no cloud metadata yet |
| Held | An open server operation must finish before the row can sync |
| Queued | A confirmed change is waiting to be sent |
| Synchronized | The current version was saved or read from cloud storage |
| Queued deletion | The deletion is waiting to be sent |
| Deleted | The current domain has a confirmed deletion |
| Too large or rejected | The current version is blocked with a record error |

These dispositions describe CloudKit or Drive. See [server sync state](sync-state.md) for GraphQL mutations and file transfers.

## Application accounts

For an encrypted app session, select a stable account scalar from the confirmed sign-in answer.
Use its dot-separated JSON path instead of a standalone namespace:

```swift
let application = CloudApplication.session(accountPath: "account.id")
let cloud = CloudConfig.cloudKit(application: application)
```

```kotlin
val application = CloudApplication.Session(accountPath = "account.id")
val cloud = DriveSync.configuration(application, authorization, context.cacheDir, replica)
```

The value must be a nonempty string or an integral number. A missing or invalid value leaves cloud sync waiting.
GraphQL sign-in remains separate. A cloud domain combines the platform, private cloud account, and application account key.
Different app accounts or cloud accounts do not share a domain.

## Data and reset

Cloud sync shares eligible persisted SQLite rows and local fields across installations in the same domain.
It reads remote data before publishing existing local data. Confirmed changes and deletes are synced.
Memory and ephemeral data, derived fields, files, transfers, GraphQL windows, and the mutation outbox are excluded.
Cloud sync does not populate GraphQL windows. GraphQL requests maintain those windows separately.

Ending an app session stops cloud I/O and clears local sync progress. It does not delete cloud data.
Signing in again with the same accounts reads the domain again.
A confirmed cloud account change or sign-out clears rows tracked by the old cloud domain and its metadata.
This happens before another account attaches.
Pending mutations and transfers linked to those rows report an account change failure.
The GraphQL credential is retained. Temporary account unavailability or an offline connection preserves the domain.

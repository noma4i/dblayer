# Public API index

Every public name of the Swift package and the Kotlin module, with the section that explains it. Names of generated models come from your `.dbl` files, see [describing models](models.md).

## Open, close, session

| Swift / Kotlin                                                                                                                                     | Section                                                                                                                                                               |
| -------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DB.open` / `DBlayer.open`, `UploadNotification` (Kotlin)                                                                                          | [connecting](server-sync.md#connecting), [Android background transfer](files.md#android-background-transfer)                                                          |
| `DB.close` / `DBlayer.close`                                                                                                                       | [errors](errors.md), [testing](testing.md#in-your-app)                                                                                                                |
| `DB.openReport` / `DBlayer.openReport`, `OpenReport`                                                                                               | [schema changes](getting-started.md#schema-changes)                                                                                                                   |
| `DB.subscriptionCensus` / `DBlayer.subscriptionCensus`, `SubscriptionCensus`                                                                       | [threading](threading.md#diagnostics), [cable protocol](cable-protocol.md#subscriptions)                                                                              |
| `DB.serverConfig` / `DBlayer.serverConfig`, `ServerConfig`, `EncryptedConfig`                                                                      | [encrypted session](server-sync.md#encrypted-session), [README](../README.md#encrypted-sign-in)                                                                       |
| `DB.sessionCall`, `DB.sessionReset`, `DB.sessionStatus`, `SessionStatus`, `SessionStanding`, `DB.interruptedSessionAnswer`, and the `DBlayer` versions                                 | [encrypted session](server-sync.md#encrypted-session), [ending a session](server-sync.md#ending-a-session), [reading the session](server-sync.md#reading-the-session) |
| `DB.call` / `DBlayer.call`                                                                                                                         | [operations without a model](server-sync.md#operations-without-a-model)                                                                                               |
| `CredentialStore`                                                                                                                                  | [encrypted session](server-sync.md#encrypted-session)                                                                                                                 |
| `RetryProgress`, `RetryChanges` (Swift), `RetryProgress.flow` (Kotlin), `RetrySnapshot`, `RetryEntry`                                              | [retries](server-sync.md#retries)                                                                                                                                     |
| `NetworkStatus`, `NetworkChanges` (Swift), `NetworkStatus.flow` (Kotlin), `isOnline`                                                               | [network status](sync-state.md#network-status)                                                                                                                        |
| `WireSource`, `WireUploadRequest`, `WireUploadItem`                                                                                                | [wire sources](files.md#wire-sources)                                                                                                                                 |
| `Transport`, `EncryptedHttp`, `UrlSessionTransport`, `OkHttpTransport`, `PlatformSocket`, `CableEvents`, `HttpRequest`, `RawOutcome`, `RawFailure` | [your own transport](cable-protocol.md#your-own-transport), [platform sockets](cable-protocol.md#platform-sockets)                                                    |
| `GraphQLOperation`, `GraphQLQuery`, `GraphQLMutation`, `GraphQLSubscription`, `GraphQLNullable`                                                    | [connecting](server-sync.md#connecting), [writing data](writing-data.md#custom-operations)                                                                            |
| Platform observers of the network, the foreground and power saving                                                                                 | [online and offline](sync-state.md#online-and-offline)                                                                                                                |

## Streams and screens

| Swift / Kotlin                                                                                           | Section                                                                                                                              |
| -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `DB.account()` / `AccountScope.current()`, `AccountScope`                                                | [account scope](value-bridge.md#account-scope)                                                                                       |
| `AccountScope.stream`, `count`, `entity`, `events`, `changes`, `uploadStatus`, `operations`, `singleton` (Swift only) | [streams](live-data.md#streams)                                                                                                      |
| `ValueStream`, `ReadValue`, `EndCause`, `ReadClaim`                                                      | [values](value-bridge.md#values), [errors](errors.md#where-an-error-arrives), [claiming a reader](value-bridge.md#claiming-a-reader) |
| `DB.cableCloses` / `DBlayer.cableCloses`, `CableCloseItem`, `CableClose`                                 | [cable closes](cable-protocol.md#cable-closes)                                                                                       |
| `ListDelivery`, `ListProjection`, `Projected`, `ListEdit`                                                | [what a delta contains](live-data.md#what-a-delta-contains), [reading a projection](value-bridge.md#reading-a-projection-on-screen)  |
| `ProjectedCount`, `CountProjection` (Kotlin only)                                                                      | [streams](live-data.md#streams)                                                                                                      |
| `ReadState`, `ReadPhase`                                                                                 | [state of a stream](live-data.md#state-of-a-stream)                                                                                  |
| `ReadFetch`                                                                                              | [streams](live-data.md#streams)                                                                                                      |
| `ModelEvent`, `ModelEventItem`, `EventStream` (Swift), `ValueStream.events` (Kotlin)                                                            | [model events](live-data.md#model-events)                                                                                            |
| `ChangeItem`, `ChangedRow`, `ChangeStream` (Swift), `AccountScope.changes` as a `Flow` (Kotlin)                                                               | [streams](live-data.md#streams)                                                                                                      |
| `ModelDecoder` (Kotlin: the companion of a generated model)                                              | [streams](live-data.md#streams)                                                                                                      |
| `DBSingleton` (Swift: a model with one record)                                                           | [streams](live-data.md#streams)                                                                                                      |
| Reconnect after lost subscription events                                                                | [subscriptions](server-sync.md#subscriptions)                                                                                      |

## Queries

| Swift / Kotlin                                                                         | Section                                                                             |
| -------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `Query`, `Condition`, `Column`, `Order`, `Association`, `Model.ids`                    | [queries](live-data.md#queries), [text search](live-data.md#text-search)            |
| `cachedRow`, `read`, `readRow`                                                         | [reading before the first delivery](live-data.md#reading-before-the-first-delivery) |
| `load`, `beginLoad`, `LoadRequest`, `refresh`, `loadMore`, `loadAll`, `LoadMoreResult` | [remote relations are live too](live-data.md#remote-relations-are-live-too)         |
| `edgeValues`, generated `<name>Edge`                                                   | [edge values](models.md#edge-values)                                                |
| `readNode`, `readNodes` (Swift), generated node reads (Swift and Kotlin)                                          | [node reads](models.md#node-reads)                                                  |
| `marker`, `tail`, prefix counts and sequence                                         | [media library marker and tail](models.md#media-library-marker-and-tail)            |

## Records and writes

| Swift / Kotlin                                                                      | Section                                                                                                                        |
| ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `DBModel`, `id`, `sync`, `uploads`, `rowRevision`, `estimatedBytes`, `errorMessage` | [what a record exposes](sync-state.md#what-a-record-exposes)                                                                   |
| `SyncInfo`, `SyncState`, `OperationFailure`, `OperationFailure.responseLimit`                                       | [what a record exposes](sync-state.md#what-a-record-exposes), [errors](errors.md#where-an-error-arrives)                       |
| `create`, `update`, `act`, `destroy`, `AccountScope.send`                           | [what a write does](writing-data.md#what-a-write-does), [writing without a network](writing-data.md#writing-without-a-network) |
| `set` (ephemeral values), `setLocal` (local fields)                                 | [ephemeral fields](models.md#ephemeral-fields), [local fields](models.md#local-fields)                                         |
| `MutationReceipt`, `ItemReceipt`, `OperationReceipt`, `Confirmed`, `Confirmed.value`, `Confirmed.answerJson`                 | [what the server answered](writing-data.md#what-the-server-answered)                                                           |
| `confirmed(discardOnFailure:)` (Swift), `confirmed(discardOnFailure)` (Kotlin)      | [what the server answered](writing-data.md#what-the-server-answered)                                                           |
| `retry`, `discard`, `OperationInfo`, `OperationQueueItem`, `OperationQueueStatus`   | [the queue of operations](sync-state.md#the-queue-of-operations)                                                               |
| `OperationInfo.upload`, `OperationInfo.uploadInfo`                                  | [the queue of operations](sync-state.md#the-queue-of-operations)                                                               |

## Files and uploads

| Swift / Kotlin                                                                                                          | Section                                                                                                                                  |
| ----------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `FileInfo`, `FileField` (Kotlin), `NoFiles` (Swift), `attach`, `file`, `url`, `detach`                                           | [files](files.md)                                                                                                                        |
| `UploadRequest`, `UploadItem`, `UploadBatchRequest`, `UploadCommand`, `AccountScope.upload`, `AccountScope.uploadBatch` | [uploads of a record](files.md#uploads-of-a-record), [create several records with uploads](files.md#create-several-records-with-uploads) |
| `Model.<action>(id:, source:, ...variables, in:)` (Swift), `Model.<action>(account, id, source, ...variables)` (Kotlin), without `id` for a singleton model| [upload to a row that does not exist](files.md#upload-to-a-row-that-does-not-exist)                                                      |
| `AccountScope.upload(_:id:action:_:)` (Swift), `AccountScope.upload(decoder, id, action, request)` (Kotlin)             | [upload to a row that does not exist](files.md#upload-to-a-row-that-does-not-exist)                                                      |
| `UploadRequest.pending`, `UploadSource`, `supplySource`, `failSource`                                                   | [deferred source](files.md#deferred-source)                                                                                              |
| `Prepare`, `VideoLimits`, `PhotoEdit`, `PixelSize`, `CropFractions`, `ImageFormat`, `CannotDecode` | [source preparation](files.md#source-preparation) |
| `UploadInfo`, `UploadStatus`, `UploadProcessing`                                                                        | [state](files.md#state)                                                                                                                  |
| `multipart`, `roots` for `initiate`, `presign`, `complete`                                                             | [uploads of a record](files.md#uploads-of-a-record)                                                                                       |
| `TransferProgress`, `TransferItem`, `UploadStatusStream` (Swift), `AccountScope.uploadStatus` as a `ValueStream` (Kotlin)                                                                | [rules](files.md#rules-1)                                                                                                                |
| `cancelUpload`                                                                                                          | [rules](files.md#rules-1)                                                                                                                |

## Values inside generated code

| Swift / Kotlin                                   | Section                                                                                           |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| `CellReader`, `Cell`                             | [cell types](value-bridge.md#cell-types)                                                          |
| `JSONValue` (Swift), `dbJson` (Kotlin)           | [JSON text in `:json` attributes](nested-types.md#json-text-in-json-attributes)                   |
| `RawEnum`, `UnionSerializer` (Kotlin), `UnionTypenameKey` (Swift) | [enums](models.md#enums), [unions](nested-types.md#unions)                                        |
| `GraphqlExtensions`, `SessionCode`               | [errors](errors.md#where-an-error-arrives), [encrypted session](server-sync.md#encrypted-session) |

## Errors and tests

| Swift / Kotlin                                                                                                                                                             | Section               |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------- |
| `DbError` / `DbException`, `Rejected`, `ResponseTooLarge`, `SecurityCode`, `ConfigCode`                                                                                                                  | [errors](errors.md)   |
| `MockTransport`, `SealedServer`, `EncryptedCablePeer`, `TestClock`, `openTesting`, `openSigned`, `openSignedClocked`, `openSignedOffline`, `loginAgain`, `closeForCleanup` | [testing](testing.md) |

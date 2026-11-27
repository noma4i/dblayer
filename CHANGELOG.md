# Changelog

## Unreleased

### Added

- Report authenticated API refusals through a database-wide stream on Swift and Kotlin.

## 6.51.0 - 2026-11-26

### Added

- Allow paginated scopes to use stored sequences to decide whether to load on open.

## 6.50.0 - 2026-11-26

### Changed

- Append new rows and update repeated rows in place when loading subsequent pages.

## 6.49.1 - 2026-11-26

### Fixed

- Return `NotFound` for a null parent in a media marker response.

## 6.49.0 - 2026-11-24

### Added

- Add conditional optimistic removal of a related row in model operations.

### Fixed

- Preserve GraphQL errors when a media window's parent is missing.

## 6.48.0 - 2026-11-23

### Added

- Add opt-in CloudKit and Drive sync for persisted model data.
- Add cloud enablement settings and report sync status for models and records.
- Add opt-in iCloud Drive folders and document sync for Apple platforms.

### Fixed

- Preserve local rows awaiting their first cloud confirmation when a cloud account changes.
- Remove pending cloud sends for rows cleared by a local reset.
- Settle each batch upload when the database rejects its final outcome write.

## 6.47.0 - 2026-11-21

### Added

- Add `StagedUpload.cancelUpload()`: it keeps the prepared file for the next upload of the same source.
- Add `forgetPrepared(request)` to drop a kept prepared file after a cancelled upload.
- Add `stage(..., row:)` to stage a file for an action on an existing row.

### Changed

- Reuse a prepared file kept by `cancelUpload` for 7 days, also after a restart.
- Make `StagedUpload.discard()` remove the prepared file of its source too.
- Restart a staged upload from part 1 when it is committed after 7 days.

### Fixed

- Remove deleted rows at the head of an ordered list when its first page is read again.

## 6.46.0 - 2026-11-21

### Added

- Add `preload when:` to preload a scope only while a singleton flag is true.

## 6.45.0 - 2026-11-21

### Added

- Add the development `trace` flag of `UrlSessionTransport` for per-request log lines.

### Changed

- Restart work at once on the new path when the phone network changes.
- Rotate connections after a network failure and end each HTTP exchange at 30 seconds.
- Require `rotateConnections()` in every `Transport` and `EncryptedHttp` implementation.
- Ping the cable after 25 seconds without an incoming frame.
- Accept only the pong that answers the outstanding ping.

### Fixed

- Remove the scratch directory of an in-memory database when it closes.

## 6.44.0 - 2026-11-20

### Changed

- Accept MediaLayer preparation descriptors through a connected file producer.

### Fixed

- Keep the pending request after a sequence conflict.
- Allow logout to end a session blocked by a sequence conflict.
- Avoid duplicate state frames after a related row is read again.
- Retry failed uploads while their failure commit finishes.

### Removed

- Remove media codecs and preparation settings from DBlayer.

## 6.43.1 - 2026-11-17

### Fixed

- Refuse a scope variable that its query document does not declare.

## 6.43.0 - 2026-11-16

### Added

- Give the receipt of an operation with `returns` the typed answer as `confirmed().answer`.
- Declare `refetch :scope, model: Model` on an operation without a model.
- Reload the `refetch` scopes and read the receiver row again after a refused operation is rolled back.

### Changed

- Show the status of a transfer stream as the `UploadInfo` of its row.
- Replace a failed member of an earlier batch with a new upload of its key.
- Throw `Invalid` when the round jobs sink of a database is set a second time.

### Fixed

- Discard a failed batch member after its batch confirmed without a row for it.
- Generate the answer type of a query whose root is an interface field selected by `... on T`.
- Resolve a temporary row id in uploads after a restart.

### Removed

- Remove `TransferProgress` from Swift and Kotlin.
- Remove `Confirmed.answerJson` from Swift and Kotlin.

## 6.41.0 - 2026-11-16

### Changed

- Take a duration for the operation option `expires_after`.
- Keep rows of a scope with `query` fresh for 5 minutes when `freshness` is omitted.
- Refuse `false` for the scope flags `keep_on_gap`, `stable_order`, `refresh_on_foreground` and `members_complete`.
- Refuse `local: true` with `default:` or `merge:`.
- Read the wrap of an operation from its document, the one input variable the root field takes.
- Bind each input field of an operation to the parameter, attribute or row id of the same name.
- Write the parameters named like attributes at once in an operation without `optimistic`.
- Stop the generator on an operation or a scope with `query` when `--schema` is missing.
- Read the answer type of an operation from its document and the schema.
- Give a model operation an answer type only with the line `returns`.
- Take the values of an enum from the schema when the schema has its type.
- Refuse `values:` on an enum that the schema knows.
- Declare the event channel `changes` at the top level of a file, not inside a model.
- Land the answer rows of an operation without a model with `upsert Model, from:`.
- Leave an optional parameter without a value out of the variables of an action call.

### Removed

- Remove the `returns do ... end` block of an operation.
- Remove the `wrap` operation line, and `variables` entries that repeat a name.
- Remove the default binding of an operation without `variables` to its row id.
- Remove the error for a named operation without an `optimistic` line.
- Remove the `processing` operation line and the processing of an upload.
- Remove the messages of removed model forms: they fail as unknown or invalid.

## 6.40.0 - 2026-11-16

### Added

- Call actions of a singleton model on the type, with or without a stored row.

### Fixed

- Answer an app load inside the request gap at once while a reread is deferred.

## 6.39.0 - 2026-11-16

### Added

- Open the cable only while the stored singleton field named by `cable_when` is true.
- Stream each request that leaves for the network with its reason through `requests`.

### Changed

- Delete the row and its dependent rows when a node read answers null.
- End the session on cable close code 4401 and wait for the pause of code 4429.
- Hold every request of the session until the `retryAfter` of a sealed rate limit.
- Share one request between overlapping reads and wait 1 second between requests of one read.

## 6.38.0 - 2026-11-15

### Changed

- Return the row as it is stored now from `confirmed`, not the row at landing.
- Keep the outcome of a finished write on disk for receipts after a restart.
- Refuse a server row with null in a required field that has no default.

### Fixed

- End a call or transfer with a runtime error when its task panics.
- End a session reset, receipt read or answer decode when its task panics.
- Refuse a stored operation or window whose saved text does not parse instead of guessing.
- Find an upload by the app id after its row gets a server id.
- Confirm a mutation whose answer rows do not decode instead of failing it.
- End a refresh with a runtime error when reading the saved window panics.
- Refuse a server row whose field value does not decode instead of dropping the field.
- Give a list value the revision of the commit that changed its rows.
- Read a row once after its event when a counter or list also shows it.
- Return the receipt of an accepted write when the session ends before the call returns.
- Show the loaded state when a window lands during the first read of its stream.
- Reload a window that changed while its last allowed page was loading.
- Keep the revision of an upload status from going back.
- Apply upload calls made while the database opens in the order they were made.
- Leave the revision and subscribers untouched when a write changes no stored data.
- Refuse a server row whose list holds a value of another type.
- Report an upload whose saved data cannot be read as failed to restore.
- Keep the server subscription closed when the database opens in the background.
- Reload a refresh-on-foreground window only when the app returns from the background.
- Release the state of finished calls and refused uploads instead of keeping it.
- Release a window nobody watches from memory instead of keeping it until the session ends.

## 6.37.0 - 2026-11-14

### Changed

- insert_on takes one value: :server or :create.

### Fixed

- Hold an operation on a temporary row until its create or upload lands.
- Refuse an operation on a temporary row whose upload was cancelled, discarded or expired.
- Open a database of a newer version with the saved session and an empty cache.

### Removed

- apply: { effect: :reread } in .dbl files: write changed_by with via.

## 6.36.0 - 2026-11-14

### Added

- Continue iOS uploads in the background with a system task, set by uploadTaskTitle in DB.open.
- Show the share of the running uploads as the progress of the background job.

### Changed

- Close the server cable while the app is in the background.
- local: true makes an attribute optional; optional: true next to it is an error.

### Fixed

- Keep the revision when the server repeats the delete of a removed row.
- Keep the rows of local models and local field values when the database format changes.
- Keep the order of calls started one after another when the device is under load.
- Find a row by its temporary id while the answer of its create lands.
- Find a row by its temporary id after an event row replaced it by key.
- Wait up to 1 second for a locked database file before a write is refused.
- Show in the operations queue the processing a server row reports for its upload.
- Never send a temporary id after restart once its create ended without a server id.
- Keep the server ids of created rows across a database format change.
- Treat a server id that starts with temp- as a server id.
- Keep a stream release, a resync and a load start behind earlier calls.
- Finish an upload after a refused database write is retried.

### Removed

- dataVersion of Config: the format of the database follows its declarations.
- storage: :disk in .dbl files: disk is the default.
- delete_unless in .dbl files: write delete_if: { not: path }.

## 6.35.0 - 2026-11-14

### Added

- recodeAboveBytes in the video limits: a smaller video is not encoded again.
- prepare and maxBytes on the generated upload and stage helpers.
- batchProgress in uploads: the share of the whole uploadBatch on each of its records.
- dimensions on an upload operation: the row gets the width and height of the uploaded file.

### Fixed

- Wait for the reconnect backoff when the cable drops again before any data.
- Land the answer rows of an uploadBatch whose sources were supplied with values.
- Confirm a created row edited before the answer with the edit on the real row.
- Keep a row the server removed before the answer of its create removed and end its queued edits.
- Show the failed or finished upload in the same revision as its operation outcome.

## 6.34.1 - 2026-11-14

### Fixed

- Fix the Android crash at start when a reader of the network status waits.

## 6.34.0 - 2026-11-13

### Added

- keep_on_gap on a scope keeps its window unchanged after a connection gap.
- maxBytes on the upload request, item and source: a larger file fails with too_large.

### Fixed

- Send operations of different rows in the order of their calls when a store write repeats.

### Removed

- maxBytes of the prepare settings and the too_large reason of prepare_failed.

## 6.33.1 - 2026-11-13

### Changed

- Require models generated by this version: older generated Kotlin code does not compile.

### Fixed

- Keep the order of calls started without awaiting the previous call.
- End a read started right after a cancelled read with the cancel, not with an error.

## 6.33.0 - 2026-11-13

### Changed

- Rebuild the server cache on the first open: queued operations, session and uploads stay.
- Run the body of every library call on library threads, never on the caller thread.
- Order calls by awaiting the previous call: calls started without waiting have no order.
- Return the network signal of the platform without waiting for the answer.

### Fixed

- Finish an upload that a restart cut during its preparation instead of failing it.
- Replace a temporary row by its `match_by` key for a server row from any source.
- Reread a row shown through `ref:` or a relation after a `changed_by` event.
- Reread the child of a shown parent after its event and recompute the `sets:` field.
- Reread a row with the `node: :one` scope that `via` names.

### Removed

- Remove `DB.handleBackgroundEvents` and the PUT in a background session on iOS.

## 6.32.1 - 2026-11-13

### Changed

- Switch off the PUT in a background session on iOS: `DB.handleBackgroundEvents` returns `false`.
- Rebuild the server cache on the first open: queued operations, session and uploads stay.

### Fixed

- Send a file on iOS in parts again, with resume.
- Cancel or discard a staged upload right after `commit` removes it instead of throwing `InFlight`.
- Wait for a library step in `commit` of a staged upload instead of throwing `InFlight`.
- Throw `NotFound` from `commit` of a staged upload that is being removed.
- Mirror a video on iOS that needs no re-encode when `mirror` is set.
- Refuse a video probe with a rotation outside 0, 90, 180, 270 as `prepare_failed`.
- Reread a row that a list shows after a `changed_by` event of its `node`.
- Wait for the first network value instead of failing a call with `Offline`.
- Mark parent lists stale by the `newer` order when a named child is destroyed.
- Keep the multipart abort owed across a restart after an invalid upload id write.
- Apply discard and cancelUpload sent during a replacement to the upload the key keeps.
- Refuse a key twice in one batch upload call with `Invalid`, not `InFlight`.

## 6.32.0 - 2026-11-13

### Added

- Add `DB.handleBackgroundEvents(identifier:completion:)` (returns `Bool`) and the PUT of a file in a background `URLSession` on iOS: an upload ends with the app in the background.

### Changed

- Send a new file on iOS as one part as large as the file.

### Fixed

- Remove an upload or an operation with `discard` or `cancelUpload` in any stage instead of `InFlight`.
- Abort the multipart that the server creates for an upload removed while its first request is in flight.
- Write the refused session record again at the next online or foreground event.
- Repeat the erase of the account data after a refused disk write at the next online or foreground event.
- Show no `localUrl` for a failed upload row without a copy.
- Refuse the copy of a source file that was emptied after the call read its size.

### Removed

- Remove the upload status `cancelled` and the wait reason `offline`, which the core never set.

## 6.31.1 - 2026-11-13

### Fixed

- Give `localUrl` of an upload from the first frame of its row, not from the return of the call.
- Fix upload attempts burning while a step waits for the session queue.
- Fail a saved write whose operation is not declared with `refused` instead of waiting.
- Fail a stored upload step with the state of the session instead of leaving it queued.
- Queue calls and writes while the secret is unavailable instead of refusing them.
- Read an EXIF orientation outside 1 to 8 as 1 in the file probe.
- Report the video turn of the file probe as 0, 90, 180 or 270 on both platforms.
- Name the location of a video `location` in the metadata keys of the file probe on both platforms.

## 6.31.0 - 2026-11-13

### Added

- Add `NetworkPath` with the kind and the number of the default network to `NetworkStatus`.
- Add `stage` and `StagedUpload` to both platforms: a file goes up before its row exists, and `commit` or `discard` ends it.
- Add `operationFailure`, `syncInfo` and `uploadInfo` builders to the test products of both platforms.

### Changed

- Deliver `NetworkPath` instead of `Bool` from `NetworkStatus.changes` and `NetworkStatus.flow`.
- Refuse operation definitions that lack an operation the library sends by itself.
- Remove an upload at open when its copy was not yet durable.
- Read the source file only during the call that brought its path.

### Fixed

- Touch the parents of a changed row once per batch up the `touch: true` links.
- Keep the queue order of restored operations, holding the pair of the earlier one.
- Send a prepared staged file in parts, also after a change of session.
- Release the lost pair of an operation that ends failed, so other writes go out.

## 6.30.0 - 2026-11-13

### Added

- `maxBytes` of `Prepare.video` and `Prepare.image`: a file above it fails the upload with the reason `too_large`.
- `reason` of `OperationFailure`: the cause of a `prepare_failed` error (`failed`, `device_cannot_decode`, `too_large`).
- `ScriptedPreparer` in the test stands of both platforms.

### Fixed

- The test stand accepts a source with `prepare` settings.

## 6.29.0 - 2026-11-13

### Added

- Source preparation: `prepare` settings recompress a video or an image before its upload.
- Error kinds `prepare_failed:failed` and `prepare_failed:device_cannot_decode` of an upload.

### Changed

- Upload `progress` is a share from 0 to 1 that never decreases.

## 6.28.1 - 2026-11-13

### Fixed

- Destroy of a memory or local row that is already gone succeeds.
- A window at its page limit reports no more pages and ends `loadMore`.

## 6.28.0 - 2026-11-13

### Changed

- Declare the fields of `sets:` as `derived: true`, they take their values from the children only.
- Name the child fields of `newer:` as a list.

### Fixed

- A stale answer of the parent no longer moves its `sets:` fields back.

### Removed

- Read no server value for the fields of `sets:` in the row of the parent.

## 6.27.2 - 2026-11-12

### Changed

- Start one network observer per process, read by the database and `serverConfig`.
- Sort rows with equal values, and a sort by `id`, in numeric id order.

### Fixed

- Reread a row of a reader with `fetch: never` on a `changed_by` node event.
- A call with a sent pair answers `dropped`, not `network`, on a network failure.
- A child hidden by an open destroy no longer shows in the fields its parent derives.
- Return the server outcome of a call when the database closes before its answer is saved.
- A call and a mutation started together both end.
- A sign-in started beside a session reset ends instead of waiting without an outcome.
- A saved server answer lands after a restart instead of rolling the operation back.
- A failed upload replaced by a new upload stays one row after a stale write.
- A refused removal write of a batch retries the same elements once.
- A group upload cut by a process death finishes after the restart and leaves no operation.
- A multipart upload cut by a process death finishes after the restart and sends its pending step.

## 6.27.1 - 2026-11-12

### Fixed

- A row created on the device keeps the value its create gives to a `sets:` field.

## 6.27.0 - 2026-11-12

### Added

- Add `NetworkStatus`: the network of the phone, live for the whole process.

### Changed

- Keep the server outcome of an operation when the database refuses its answer rows.
- Retry the rollback of a refused operation after a database refusal.
- Requeue an operation waiting to retry when the network drops, and retry on return.
- Carry the reason text of a refused invalid write in `Invalid` errors.
- Fail generation when `insert_on` lists `:node` and the model has no `node` scope.
- Fail generation when a scope without `node:` declares the parameter `nodeId`.
- Fail generation when an operation response omits the membership field of a joined list.
- Add an event row to the head of a loaded `[:event, :response]` window.
- Add a created row to the head of loaded membership lists of its parameters.
- Skip created rows in an `insert_on: [:create]` scope with constant variables and no rule.
- Keep the last load phase of a window that loses the network while loading.
- Clear the load error of a window when its last reader leaves.
- Reject model fields whose names start with `server__`.

### Fixed

- Derive parent fields set by children once per commit from the server value and children.
- Retry a database write of a confirmed upload part, completion or removal.
- Grow the retry pause of a write while the database refuses it in a row.
- Deliver the upload status to readers after a failed database write of it.
- Keep a finished multipart upload when a session ends after its upload id write failed.
- Keep an upload group going when the database refuses the removal of its removed member.
- End an operation on a server-removed row when the database refuses its removal.
- Show the window load error again after a failed stream read recovers.
- Refuse a write with data the database cannot accept as `Invalid`, without a retry.
- Count one attempt of an operation per send, also when the network drops before it.
- Insert rows of node rereads started by `changed_by` into `insert_on: [:node]` windows.
- Keep the other events of a feed batch when one event does not decode.
- Skip the request of a preload window whose saved data is still fresh at open.
- Send the real parent id in a write that names a row created before it.
- End writes and uploads on a row whose create was refused, without a request.
- Refuse a query, mutation call or session call that carries a temporary id.
- Fail generation when a `create` action maps a variable to the row `id`.

## 6.26.3 - 2026-11-11

### Fixed

- Remove a row from a loaded window when a page of another window changes its membership.
- Keep the window phase of a stream when a read error clears.
- Refuse a read whose parameter holds a temporary id that cannot be resolved.

## 6.26.2 - 2026-11-11

### Fixed

- Send one request when a stream and a load share a fresh window.
- Load a media window whose pages share a related row.

## 6.26.1 - 2026-11-11

### Fixed

- Keep a stream open when a row joins or leaves its open window.

## 6.26.0 - 2026-11-11

### Fixed

- Finish a restored upload batch when a step answer arrives before the operations are declared.
- Report a stream as not settled until its own load ends after a failed load call.
- Erase account data at the next open when the server ended the session before the app stopped.
- Keep files of pending uploads through a database format change.
- Keep upload outcomes after the database reopens.
- Label each stream value with the revision of the data it carries.
- Keep the answer of a session call that finished after a restart until the app reads it.
- Keep a retried upload in progress after the database reopens.
- Keep the refresh debt when the app stops before a broken subscription reconnects.
- Pause uploads while the OS power saving mode is on.
- End a direct session call with an error after its retry budget is used.
- Preserve the last relevant value revision when a stream resyncs.
- Delete the files of cached rows erased when the schema of a model changes.
- Keep the failure time of an upload across restarts.
- Keep ended sessions signed out after repeated crashes during database reopening.
- Remove rows when the server reports them missing during destroy.
- Discard a saved operation at open when the server deleted its row while the app was closed.
- Keep the attempt number of an operation that was recorded but not sent before a restart.
- End an upload start with a session-ended error when its data generation is stale.
- Keep the failure kind of an upload row after the database reopens.
- Mark an upload that used all its attempts as failed while every upload slot is busy.
- Keep the window refresh debt across a restart after a subscription gap.
- Keep the multipart abort debt of a replaced file copy across a restart.
- Send an accepted call with the document it was accepted with after the operation is declared again or removed.
- Start one database read for a value stream while a read that covers it is running.
- Count window freshness from the time the server response arrived.

### Removed

- Remove the retry status value from value streams.

## 6.25.4 - 2026-11-11

### Changed

- Replace failed uploads when a new upload starts for the same action.
- Batch row reads while landing events, references and write batches.

### Fixed

- Reread the node when a change event declared with a node and a payload arrives without the payload.
- Keep the operation of a failed upload after the database reopens.

## 6.25.3 - 2026-11-11

### Fixed

- Never reuse an upload identity after the database reopens.
- Read the outcome of the right upload after a reopen, so a refused upload is discarded.
- Remove an upload left without its transfer when the database opens.

## 6.25.1 - 2026-11-11

### Fixed

- Send the owed multipart abort after a session reset, logout or revoke.
- Keep rows of an earlier page when a later page shares their order value.
- Answer keepalive pings in the encrypted cable peer of the testing module.

## 6.25.0 - 2026-11-11

### Added

- Declare response roots for multipart upload steps.

### Changed

- Batch page and reference reads with fixed query counts within 500-row batches.

### Fixed

- Keep ordered pages and the history end when refreshing the first page.
- Reread watched selections once after a subscription breaks without a network change.

## 6.24.0 - 2026-11-10

### Added

- Discard a refused operation with `confirmed(discardOnFailure: true)`: it removes exactly the operation of the receipt.

### Changed

- Read nested references of a list with one query per target model.
- Build the operations queue with work linear in its size.

### Fixed

- A cursor page of a scope with a declared order keeps the rows of earlier pages.

## 6.23.2 - 2026-11-10

### Fixed

- A stream reader released by its caller always ends as closed.

## 6.23.1 - 2026-11-10

### Fixed

- Generated Swift upload calls without parameters compile without warnings.

## 6.23.0 - 2026-11-10

### Added

- Upload a file to a row that does not exist yet: the receipt shows no optimistic value and the answer of the final mutation writes the row.
- Declare nested objects in `returns` of a model action, with the syntax of a standalone operation.
- Read the value of an active file transfer from `OperationInfo.upload` (`uploadInfo`), also without a row.
- Wait for a receipt with `confirmed`, which returns the value or throws `Rejected`.
- Read the session with `sessionStatus`: epoch, identity and phase.
- Fail with `ResponseTooLarge` when an HTTP response exceeds its size limit.
- Read an oversized response limit from `OperationFailure.responseLimit`.
- Expose response limits in HTTP and upload requests.

### Changed

- Attach file fields from a local file path.
- Read file fields through their local URLs.
- Reject socket frames exceeding the inbound size limit.
- Report receipt termination causes as errors from `confirmed`.
- Return confirmed model values in `Confirmed.value`.
- Rename Swift `answerJSON` to `answerJson` in confirmed results.

### Fixed

- Defer stream and load requests until temporary row IDs become server IDs.
- A stream on a temporary row id switches to the server row after the swap.

### Removed

- Removed file-field reads that returned an in-memory byte array.
- Removed file attachment from in-memory byte arrays.
- Remove the receipt method `outcome`.
- Remove the type `OperationOutcome`.

## 6.22.0 - 2026-11-10

### Added

- `cableCloses` streams connection closures and their causes on Swift and Kotlin.
- `ServerContract` registers documents and looks them up by hash.
- The cable sends a client ping and reconnects when the pong is late.

### Changed

- Pending calls and loads fail with `Closed` after close and `NoSession` after session end.
- Outcomes and streams deliver the end cause as a value, `Ended(Closed)` or `Ended(SessionEnded)`.
- An owner answer given before close reaches the caller unchanged.
- `RetrySnapshot` lists waiting retries by owner, policy, attempt, and delay.
- `RetrySnapshot.waiting` replaces `activeCalls` and `lastAttempt`.
- `RetryEntry` replaces `RetryAttempt` in retry snapshots.
- `EndCause` replaces `ReadEnd` for stream termination.
- `OperationOutcome.Ended` replaces `Interrupted`.
- Swift `OperationOutcome.confirmed` names its value `confirmedValue`.
- `ReadStream.claimReader` returns `ReadClaim` with a reader or an end cause.
- `WireUploadRequest` and `WireUploadItem` carry `WireSource` instead of separate file fields.
- `ServerContract.admit` and `settle` reject invalid JSON requests.
- `SealedServer` refuses hash-only requests for undeclared documents.
- `SealedServer` registers documents for later operations.
- `onlineSink` fails with `Closed` after close.
- `bridgeToTransport` no longer accepts an operations argument.

### Fixed

- Multipart aborts are identified by upload ID.
- Multipart aborts resume after session end, close, or restart.
- Revoked responses end the session.
- Revoked responses reach callers as the original call and load errors.
- Call deadlines start when requests are accepted, including while awaiting network state.
- Resolved outcomes retain their values when the session ends.
- Uploads apply saved step refusals after a restart without sending the step again.
- Upload retries preserve the previous copy when replacement fails.
- Uploads resume retained request pairs after a restart without duplicating requests.
- Upload outcomes from previous cycles cannot finish a newer cycle.
- Removing one upload from a group preserves the other members and their mutation.
- Retry and discard act on the chosen operation across upload group members.
- Destroying an unsent creation discards its dependent queued operations.
- Server responses cannot restore rows deleted after the request started.
- Queued operations preserve server refusal details after a restart.
- Queries retry replayed requests with a new request identifier.
- Unauthenticated responses reach callers as `Unauthorized`.
- Subscription errors close only the affected channel.
- Empty pages with a continuation cursor preserve existing list members.
- A source awaiting upload preparation no longer expires.

## 6.21.0 - 2026-11-09

### Changed

- Test doubles refuse unscripted requests and a second body under one pair.

### Fixed

- The local values of a supplied source reach the row in the transaction of the file copy.

## 6.20.0 - 2026-11-09

### Added

- Uploads accept a deferred source and publish their rows before file preparation.
- `UploadSource`, `supplySource`, and `failSource` provide or fail a deferred source.

### Changed

- `retry` of an upload takes an `UploadSource` instead of an `UploadRequest`.

### Fixed

- Sequenced requests send the document hash first and the full document only on a miss.
- A sign-in answered without a sequence fails and erases its provisional credential.
- A nested field follows its own schema nullability, not a same-named root field.

## 6.19.0 - 2026-11-09

### Changed

- `DBModel` requires `rowRevision` and `estimatedBytes`: regenerate the models with the new generator.
- `FileInfo.size` is `ULong` on Kotlin.
- `OperationFailure` takes `sessionCode`, `requestSize` and `requestLimit` before `message`.
- `RetryProgress` is read by pull, the callback listener is gone.
- A stream accepts one reader, a second reader ends with `Invalid`.
- `upload` on a missing row fails with `NotFound` for every model.
- `MockTransport` needs one scripted reply per PUT.

### Fixed

- `UploadInfo` carries `localUrl` and `processing` on Swift and Kotlin.
- `OperationFailure` carries `sessionCode`, `requestSize` and `requestLimit` on Swift and Kotlin.
- A confirmed operation outcome carries the row its landing batch wrote.
- A damaged model event ends the read with an internal error.
- A closed database answers `Closed` from every call before it reads the arguments.
- A no-op write answers with the row and the revision of one snapshot.
- A failed window, a call and a task of the executor keep the type of their failure.
- A query or a cable connect with a missing secret ends with `StateLost` and creates no secret.
- A refused write of a multipart upload id is repeated before the upload is aborted.
- A round that ended its job starts a new job at the next need.
- A late start of an old upload job leaves the network of the running job on Android.

## 6.18.0 - 2026-11-08

### Changed

- Reorganise the source tree: the tests of each module live in a `tests` directory next to its code.

## 6.17.0 - 2026-11-08

### Added

- Snapshots carry the `edits` from the rows the reader holds to the new rows.
- The status of an upload carries `localUrl` and `processing`.
- `scope.changes()` streams the rows changed by each commit of the database on both platforms.
- `DB.interruptedSessionAnswer` and `DBlayer.interruptedSessionAnswer` return the answer of a session call that a killed process left running.
- `open` fails with `Config` on Android below API 34 when the manifest lacks `FOREGROUND_SERVICE_DATA_SYNC`.
- `ReadEnd.internal` reports a stream value that could not be read; `pull` never throws.
- `uploadStatus` and `operations` streams end with an `ended` item that carries the cause.
- `DbError.AlreadyExists` reports a `create` with an id that exists.
- A batch element without a row in the answer fails with the kind `protocol`.
- Ephemeral fields with `reset_after` return to their default after the window.
- `subscriptionCensus` reports refused subscription events.
- `ReadState` carries `hasMore`, `settled` and the server `total` of the window.
- `ModelEvent` carries `position` and the rows `before` and `after`.
- `ModelEventItem` has a `gap` item for a lost order of events.
- `copies_receiver_to` takes `set:` for literals and fields of the copied row (`owner:`) or a singleton row.
- `uploads do ... end` declares the steps of file uploads once per project.
- `removed_by Model, match: { field: :model_field }` removes the rows of a model when a matching row of another model lands.
- The state of an upload reaches the streams of its row.
- The local values of a temporary row move to the row that replaces it.
- `NoSession` reports server work without a session or transport on both platforms.
- Core encrypted calls and cable subscriptions take the admission of their caller.
- Value bridge: `AccountScope`, `ValueStream`, `Projected`, snapshots and keyed deltas with the session epoch.
- `AccountScope.send`: a message command with an operation receipt, refused for an old epoch.
- `refresh` and `loadMore` on a stream, interrupted when its session ends.
- Streams for counts, related rows, rows by id, model events and upload status.
- `AccountScope.send` carries a media upload; `uploadBatch` sends many files.
- `expires_after: <seconds>` on `operation` rolls back a queued operation that waited too long, with the outcome `rejected` of kind `expired`.
- `Query.read` and `Query.readRow` read storage once without a stream.
- `AccountScope.stream` takes `refreshOnConnect`; a list stream opened with it loads again when the network returns.
- A projection keeps the edits of its deltas and the temporary id of a row.
- `Query.beginLoad` returns the interest of one caller with `result()` and `cancel()`.
- `Query.offset` skips rows before the `limit` window on both platforms; a `limit` of 0 fails with `Invalid`.
- `ModelEventItem.ready` tells that the source of an events stream is open.
- `DbError.NotOpen` reports a call before the first successful `open`; a database that was opened and closed keeps answering `Closed`.
- `AccountScope.operations` streams the queue of operations as `OperationInfo` values with status, attempts, error and `blockedBy`.
- `AccountScope.retry` and `AccountScope.discard` take the model, id and action of an `OperationInfo`.
- `fetch_server_config_with_clock` and `FetchPorts` run the config fetch on a test clock.

### Changed

- `HttpRequest` carries `author`, `requestId` and `attempt`; a repeat of a request keeps its `requestId`.
- The core keeps the open database: `OpenDatabase.opened()` returns it, and `DB` and `DBlayer` keep no copy.
- `call` replaces `run`: the core reads the result by the `returns` block of the `.dbl`, and the generated types decode it strictly.
- One executor class per platform serves the transport and the encrypted HTTP, and the core picks it by `endpoint` and `encrypted`.
- `Projected.edits` is a list for every delivery: a snapshot removes the rows before it and inserts its rows.
- `RetryProgress.changes` is an `AsyncThrowingStream` and ends with the error of the subscription.
- `DB.open` and `DB.close` are not bound to the main actor.
- `DBlayer.database` is internal; use the typed calls of the library.
- `run` hands the transport the variables as canonical JSON text with sorted keys on both platforms.
- Swift reports the foreground at registration when the app is active, as Kotlin does; Kotlin reports a lost network as offline.
- Android `onStopJob` returns `false`; a refused round start names a kind (`RoundRefusal`) and the core words the reason.
- `DB.openTesting` and `DBlayer.openTesting` take `observers`; the test hooks left the production module.
- `sync.state` of a record shows `sending` while its write is sent, and a write that waits in the queue again shows `pending`.
- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- Rows reach Swift and Kotlin with every cell in its declared type, and a cell of another type throws instead of converting.
- A query is a typed record that the core compiles, and `relation` is optional on load calls.
- `Transport` and `EncryptedHttp` take one `HttpRequest` built by the core and return a raw `RawOutcome`; the core classifies the failure.
- `Transport.openCable` returns a `PlatformSocket` and reports frames to `CableEvents`; `CableLease` and `EncryptedSocketEvent` are removed.
- The status of an operation is `pending`, `sending`, `failed`, `committed` or `rolledback`, written with each transition.
- `DbError` names the failures of the cells: `Offline`, `Dropped`, `Unauthorized`, `InFlight`, `Storage`, `Runtime`, `Internal` and `Copy`. `Storage` replaces `Database`.
- `ListEdit.rewrite` carries the `temporary` id it settles, `Projected.edits` replaces `takeEdits`, and `ModelEventItem.ended` replaces `failed`.
- Projections mirror each delivery and drop `temporaryIds`, the decode counters and the resync decision.
- Redirect answers are reported as `Http` errors on every platform and are never followed.
- `registerOperations` is async and parses its definitions off the caller thread.
- `subscriptionCensus` is async and reads the streams and channels of the owners.
- `ReadState.error` is the typed `DbError`.
- `open` takes the upload steps from the models: `UploadOperations` and the `uploads` parameter are gone.
- `OperationReceipt.items` carry one `MutationReceipt` each.
- A refused sessionless call reports a network error without the HTTP status.
- `UrlSessionTransport` and `OkHttpTransport` have no `cablePath`. `Transport.openCable` receives the WebSocket address built by the core.
- `ReadFetch.none` is `ReadFetch.never` in Swift and `ReadFetch.NEVER` in Kotlin.
- `Query.beginLoad` reads its arguments off the caller's thread; the interest of the caller exists when the call returns.
- `Query.loadAll` gives up its pages when the caller's task or coroutine is cancelled. The core call is `Database.beginLoadAll`, which returns a `LoadRequest`; `Database.loadAll` is removed.
- Core writes and transport calls carry one `Admitted` value from the calling database.
- Transport requests carry a network `purpose`; transfer steps and their cleanup use `transfer`.
- `Transport.put` takes a `TransferIdentity` with the operation, item and step.
- Android ties a refused background start to the epoch of the accepted upload.
- A transfer whose item is missing after an upgrade fails with `missing_file` and refuses a retry without a new file.
- `retry` takes an optional upload `source`: the failed item gets the new file and keeps its operation and item key.
- Records of a batch below its minimum go through the single mutation one at a time in the same operation. The batch receipt reports each record.
- A range of subscription events lost to a full intake marks the lists of its models stale and reads each open list once.
- Requests, loads, uploads and subscriptions hold a lease that a session end, reset or close ends.
- `sessionReset` and `close` are async and return after clearing; `sessionReset` clears the data.
- A session end replays after a crash between the credential and the data clear.
- A test opens its database and closes it; there is no reset between tests.
- iOS uploads use a plain `URLSession` PUT.
- Every write is queued durably when offline and sent when the network returns, one request at a time in write order.
- `Model.create` replaces the generated `Model.send`.
- A send, a stream request and a prepared delivery end with the epoch they belong to.
- A server create returns its optimistic value and a separate outcome; `send` takes a model with `operation :create`.
- Every command, one-off read and blob call carries the session epoch of its account and is refused when that account left.
- A send needs a signed-in session.
- A queued write is kept while the network is away and after a reopen, and ends when another account signs in.
- A pinned write checks its epoch and commits while the lifecycle holds its commit line.
- A session end, a reset and a close end the work of the old epoch at once and do not wait for it. A late write of the old epoch is refused at its commit.
- A load, `loadMore` or node read whose answer arrives after a reset of model caches ends with `Interrupted` and stores nothing. A send of another model goes on.
- `cancel` of a stream does not wait for the reader; no delivery is admitted after it.
- A task started by a call of one session epoch commits and sends in that epoch only.
- A stream keeps at most 256 values in one journal in the core; platforms keep no buffer.
- A full list journal gets one new snapshot; a full events journal reports one lost range.
- The end of a stream comes once after its values; a session end drops unread values.
- A shared load stops only when its last caller cancelled its interest.
- A window that owes a read after a mutation is read again once while observed.
- A media window that owes a read after a mutation reads all its pages again.
- A blob reads its status stream while the scope of its reader lives (Kotlin).
- A list stream no longer keeps a warm observer after its reader left.
- Projections, count streams and event streams report platform decode errors and source errors.
- `create`, `update`, `act`, `destroy`, `send` and `upload` return a receipt with the optimistic value, revision and outcome.
- `uploadBatch` returns one receipt whose items keep their client keys and confirmed rows.
- `retry` of a record returns the receipt of the retried operation.
- A rejected outcome carries the session code of a session refusal.
- A write that failed on the network goes out at once when the network returns.
- `refresh`, `loadMore` and a one-off load fail at once with the network error while offline, and the stream state shows it.
- An action whose answer carries another row returns that row in its outcome and leaves the receiver unchanged.
- The first delivery of a projection is a snapshot; `takeEdits` returns `nil` until a delta arrives.
- `run`, loads, node reads, cache reads, `retryAll`, `cancelUpload` and the Blob calls carry the session epoch and are refused when that session ended.
- A scope keeps the database it was made for; a call with an old scope never reaches a database opened later.
- A plain action whose request never reached the server, or that was unsent when the network dropped, ends with the network error and rolls back.
- After a network return the first retry skips the backoff once; a second failure waits by the backoff.
- A projection asks for a snapshot again when a jump in the delivery sequence shows that a delivery, also the recovery snapshot, was dropped.
- A create the server answers has a generated typed `create` in Swift and Kotlin.
- Operations declare `removes_receiver`, `copies_receiver_to`, `joins_scope` and `leaves_scope`; the change shows before the answer and a refusal restores it.
- The DSL refuses capitalised attribute and parameter names.
- The DSL refuses an attribute or parameter named `account`, or `id` in an edge read.
- The DSL refuses the field names `self`, `sync`, `uploads`, `modelName`, `layout` and `wireLayout`.
- The DSL refuses names of calls the generated code writes, such as `jsonValue` and `onAccount`.
- The generator refuses a model or type named like a library type, such as `Query`.
- The generator refuses clashing GraphQL input field names and repeated one-of cases.
- The generator refuses scope and association names that repeat a generated member.
- The DSL refuses a name that ends with `_`.
- A delta carries the ordered steps from the reader's rows to the new rows, and `ListEdit` has a move.
- `OpenOptions` takes the operations as typed definitions and no power saving flag.
- `UploadInfo.percent` is `UploadInfo.progress`, computed by the core.
- `openReport`, `subscriptionCensus` and `DB.account()` return `Closed` after `close`.
- `close` returns after the core stopped the platform observers.
- The core rejects a load call without a relation with `Invalid` on both platforms.
- Kotlin reports the network value of the system on every callback.
- The core cancels a platform PUT or cable open through Swift task or Kotlin coroutine cancellation.
- `setOnline` waits for the owner answer without a time limit and returns `Closed` after `close`.
- A cable connection that closes drains its queued frames before the core cancels it.

### Fixed

- Operation calls get `Network` on a transport failure and the GraphQL error on a temporary server refusal.
- An upload step reads its answer from the `root:` of `uploads do` and names the root when a field is missing.
- A stream that outlives `close` no longer holds the database.
- A cable socket that opens after the core gave up on it is closed.
- An upload abort that the network interrupts is sent again when the network returns.
- A refused background start shows its reason as `refused` on every upload of the round until the next start is asked.
- A reopen replays the unanswered request of an upload step instead of sending the step again.
- An upload that stops after a reopen frees the mutation queue held by its saved step.
- A saved upload step is replayed when the network is up before the operations are declared.
- A list that refreshes on connect reads once when the network returns, not again for the resumed subscription.
- `discard` and `cancelUpload` of an upload whose final mutation failed remove its operation and its optimistic row.
- A media window marked stale reads the whole window again after a failed load.
- The Android upload service reports its start after `open` and stops when no transfer is unfinished.
- Android reachability follows the validated state that the system passes to its callback.
- A released `Projected` ends its stream in the core.
- Preserve validated local fields copied from singleton rows during creation.
- Preserve queued operations and uploads when model caches are reset.
- Keep readable snapshots after a failed database commit.
- Insert created and response rows into matching loaded windows.
- Share the same window for omitted and explicit enum defaults.
- Preserve upload attempt limits across restarts and network changes.
- Restore interrupted first logins and report damaged saved session requests until reset.
- Send current application headers to encrypted requests and cable connections.
- Keyed reactions match elements stored under a nested path.
- The receipt of an action carries the revision of its commit.
- A parent pointer follows a sent child to the id the server gave it.
- Events of the server subscription land in the cache and reach event streams.
- A destroy the server answers only with NOT_FOUND is confirmed and the open lists of the model are read once more.
- The first value of a stream of a loaded window holds the loaded phase.
- A stream opened on a closed database ends as closed.
- A request that cannot be read ends its stream as invalid.
- Sign-in returns after its session identity is visible to subsequent calls.
- Receipt waits return Interrupted when session reset races with their admission.
- Loads keep their caller's epoch when the account changes before dispatch.
- Pagination reports the page it completed when called during an earlier load.
- Refresh and pagination preserve GraphQL messages and extensions, including repeated cursor errors.
- Opening a database of an older layout no longer fails: tables kept across a format change move by the columns both layouts share.
- Operations of the previous account are not sent after an identity change without `sessionReset`.
- A confirmed create keeps its row when another model is reset before its answer lands.
- Encrypted request ids are random UUID v4 values that do not repeat after a reopen.
- A first sign-in interrupted before its answer lands its credential after the next open.
- A credential that is missing or does not match the stored digest ends the call with `stateLost` before any request is sent.
- A `CredentialStore` failure ends the call with an error instead of waiting or repeating the request.
- A cancelled request that already started in the network is not cancelled and reaches its answer.
- A rejected landing keeps the stored request and repeats it with the same request id.
- Only a `logout` answer with `revoked` ends the session.
- The session end erases the digest rows before the credential; a credential left by a crash is erased at open.
- A failed upload older than 7 days is removed when the network returns, without a reopen.
- A background round that starts before any session ends its job at once.
- A second `close` of a database returns success after the first finished.
- `flush` returns after each pending operation ends its send, also one sent by a background pass.
- Subscription frames dropped from a full input reach the events reader as lost positions.
- Closing the database returns after its threads release the file, so the same file opens again at once.
- A resumed window that owes a read is read without force: a debt that another request paid is not read again.
- A reread after an event that kept the stored rows reports a gap to the events reader.
- An event batch refused because its session ended is not reread in that session. The new session reloads the window once.
- `sessionResume` finishes a logout whose credential could not be cleared, and clears it.
- An older list answer no longer undoes a newer join or leave.
- An operation removed, discarded or failed outside its send ends the outcome of its receipt.
- A flush, load retry, preload, resume or upload cleanup woken during a model reset runs once after it.
- An events reader receives the exact range of lost positions, then one end.
- A window opened after a failed load of its query no longer shows that error.
- A parameter named like a query or like `read`, `put` or `act` no longer captures a generated call.
- `waitForInitialLoad` of a held entity ends after `sessionReset` and logout instead of waiting.
- Every terminal subscription failure reaches the query listener once.
- A `create` with an id given by the caller returns the confirmed row.
- A network change waits for the start of an action that already passed its check.
- A subscription event lost at a session start reloads the windows that watch its model.
- `sessionResume` writes in the session epoch of its call.
- Generated `*Edge` reads and the Kotlin singleton blob actions take the account like every other generated accessor.
- Generated Kotlin models compile in the module of an app: `onAccount` is public, and a module outside the library compiles the generated fixtures.
- A mutation call returns with its own answer and never waits for the reread.
- A load that began before a commit no longer cancels the reread that commit owes.
- A mutation committed during the first load of a list still rereads it afterwards.
- `joins_scope` skips a row that is not cached instead of failing the call.
- `commandStatus` right after the confirming commit reports delivered, not discarded.
- An explicit load that restores an ended session returns its result, not `Interrupted`.
- A cancelled `loadAll` or `refreshPreloads` call stops asking for pages.
- `Projected.loaded()` returns with the settled state that comes with the rows of the first load.
- A cached entity whose load failed on the network stays loaded and shows the network error.
- The stream of an entity that the library loads waits for the frame of the landed row before its first load settles.
- A refused multipart abort waits for `sessionResume` and reaches the server once.
- `cancelUpload` cancels an upload that starts while the call runs.
- A format change keeps queued operations and uploads when their table layout is unchanged and builds their list effects again. A queued create does not bring back its record.
- A multipart upload discarded before its first part is aborted on the server after a reconnect.
- `discard` and `cancelUpload` answer after the copy of the file is removed.
- A receipt carries the optimistic row as its intent commit wrote it.
- A feed event without a commit is refused and counted by `subscriptionCensus`.
- A call that waits for an owner returns `Closed` when `close` stops the owners.
- Android reports the network state before it registers the network callback.
- A list, count or row stream of a model with `changed_by` rereads on an event of its area without an `events` stream.
- `ModelEvent` carries the `area` and `kind` of its change record also after a reread or a removal.
- A failed upload shows the error of its refused step instead of the text `failed`.
- Scope subscriptions carry the window variables.
- Cancelling a stream reader ends the stream at once.

### Removed

- `OpenOptions.lowRam` is gone: the core has no input that reads it.
- The `durable` key of `operation` is gone: every write is queued.
- A write no longer fails with a network error while the network is away.
- `LoadMoreResult.alreadyLoading` is gone.
- `scope.related` is gone: read a server relation with a query.
- `Blob`, `BlobUpload`, `DB.blob` and `blob_upload` are gone: upload through a model action.
- `needsAttention`, `canRetry`, `loadedAt` and `errorKind` are gone: read `sync.state` and `ReadState.error`.
- `ValueSink`, `readStream` and `cancelStream` are gone: read a value stream.
- `Query.cached()` and `Database.peekQuery` are gone: read rows with `read()` or a stream.
- `deliveryUnknown` is gone from `SyncState`, `UploadStatus` and `OperationOutcome`: a send without an answer repeats with the same request id.
- `Database.flush`, `resume`, `resumeUploads`, `retryAll`, `refreshPreloads`, `warm`, `maintain` and `setListening` are gone from the bridge on both platforms. The library sends, resumes and maintains by itself.
- `sessionResume` is gone from Swift, Kotlin and the core. Opening the database with a session sends a request stored before the close.
- `enterForegroundForTesting` is gone. The foreground notice comes from the system.
- `OpenOptions` has no `preload` and no `listening`. `openTesting` and `openOnClock` have no `preload` and no `listening` parameter. A scope declared with `preload` loads on a test clock like it does in production.
- `DB.logout` and `DBlayer.logout`: use `sessionReset`.
- `settled`, `confirmedId`, `ActionAnswer`, `Sent`, `CommandStatus`, `commandStatus` and `commandOutcome`: use the receipt outcome.
- `resetForTesting`, `settleForTesting` and `testing_busy`.
- iCloud and Google Drive sync, the `cloud` model attribute and `CloudLink`.
- `LiveWindow`, `Window`, `expand` and `Query.window`.
- `LiveList`, `Live`, `Entity`, `Entities`, `LiveSource`, `.live`, `.snapshots`, `.find` and `Model.events()`.
- The `add_to`, `remove_from`, `create_row` and `change_row` lines of `.dbl`.
- Wrapper registries, flushers and value caches for entities, lists and events.
- The iOS background upload session: `handleBackgroundSession`, `BackgroundPuts`, `BackgroundUploadSession`.
- `streamLoaded`, `ValueStream.loaded()` and `ValueSink.delivered`: read the state and `readStream`.
- `StreamValue.missed` and `ModelEventItem.missed`: use `lost` with its range.
- `Database.load`: use `beginLoad` or `Query.load`.
- `Transport.subscribe`, `Transport.unsubscribe` and `SubscriptionSink` are gone: server events arrive on the encrypted cable.
- `restoreFailed` and `discardRestoreFailed` are gone.
- The `retries` parameter of `open` and `openTesting` is gone: `serverConfig` still takes it.
- `DBlayer.sessionSend`: use `sessionCall`.
- `Config.cacheRows`.
- `Connectivity` of `serverConfig`: the core takes the platform observer.
- `DbError.Sync` is removed: an upload without declared operations fails with `Invalid`.

## 6.16.1 - 2026-11-03

### Fixed

- `resetForTesting` returns the database to a fresh open and ends old work before `settleForTesting`.

## 6.16.0 - 2026-11-03

### Added

- `openTesting` takes the encrypted session parameters of `open`.

### Fixed

- Signing in again after a logout loads held entities and queries.
- `settleForTesting` returns after a logout.
- Queued changes and model events of a closed account never reach the next one.
- A reply to a request that started before a session reset or close is discarded.
- A close or a session reset ends calls waiting in a retry, queue or response.
- A session reset or logout stops sends and uploads in flight.
- `waitForInitialLoad` after a reset waits for the new load.
- A load after a reset starts its own request instead of joining the earlier one.
- A reset ends the explicit calls that wait for a load of the old data.
- Uploads queued when a session ends start again with the next session.
- An answer arriving after a close never changes the stored session.
- Closing the database twice waits until the first close finished.
- A session end leaves no rows of the ended session in a changes queue.

## 6.15.3 - 2026-11-03

### Fixed

- Held reads wait after logout and reload when the next session commits.

## 6.15.2 - 2026-11-03

### Fixed

- Held entities and live queries load again after a data reset.
- Server subscriptions reopen after a data reset.

## 6.15.1 - 2026-11-03

### Fixed

- The tagged Swift package includes the generated bindings of its release.

## 6.15.0 - 2026-11-03

### Added

- Reset and settle testing databases without reopening them.

### Fixed

- Tasks woken by a landed load response run after the landing lock is released.

## 6.14.1 - 2026-11-02

### Fixed

- Reading the database generation no longer waits for a write.
- Cancelling a live query no longer deadlocks with a delivered change.
- Entity listeners can retain, release, reset or close entities from inside `changed`.
- Live query listeners can cancel their subscription or change the online state from inside `changed`.
- Reset listeners can write or reset again from inside their callback.
- Tasks woken by loads, relation states, resumes or uploads can call the library again.
- A credential callback can reset the session, and a slow keychain no longer blocks reset.
- A listener that owns another subscription can be released without a deadlock.
- A listener thread that cannot start no longer runs the callback on the caller's thread.
- A pending native call resumes on a library lane, so cancelling it cannot deadlock.
- Swift and Kotlin adapters hold no lock while calling the core or your code.
- Closing the database ends every open stream and stops its cloud and refresh work.
- A busy store no longer stops asynchronous control work such as cancel and close.
- Models without row rules keep their 6.13.1 schema fingerprint, so an upgrade keeps their cache.

## 6.14.0 - 2026-11-02

### Added

- Membership rules match row fields against literal sets, literals and scope parameters.

### Fixed

- Settled returns the refusal of an action that was rolled back.

## 6.13.1 - 2026-10-31

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- Release tags are lightweight tags.

### Fixed

- Held entities receive the closed phase as soon as the database closes.
- Live windows compute their row ids off the main actor.
- Builds print no compiler warnings and no future-incompatibility notice.

## 6.13.0 - 2026-10-29

### Added

- Report lost session state as the typed session code StateLost.

### Fixed

- Preserve exact large-integer ordering in merges, sequence selection, and sorted lists.
- Preserve original parameter values during cross binding and distinguish typed scope keys.
- Reject duration counts whose converted milliseconds overflow.
- Preserve encrypted sessions and pending requests through database format changes and model resets.
- Report lost session sequence state without replacing the stored credential.
- Report corrupt stored rollback data, revisions, sync states, and pending requests as errors.
- Report upload state persistence failures instead of continuing with unsaved transitions.
- Report stored window and watched reload failures without treating them as missing or fresh.
- Reject malformed cloud cursors, upload headers, and unsupported top-level operation options.

### Removed

- Release attachments no longer include the macOS dylib.

## 6.12.0 - 2026-10-27

### Added

- Ship Intel macOS and Intel simulator slices and a universal JVM library.
- Apply full event rows with `changed_by payload` on Swift and Kotlin.
- Declare `server_revision` to reject older server rows before changing data or list membership.
- Upload durable Blobs before choosing a mutation, with typed consumers, progress and restoration.
- `marker` and `tail` store media markers and automatically finish loading new pages on both platforms.
- Limit derived preloads with `preload ..., limit: N`.
- Scopes with `members_complete: true` automatically load all members by cursor.
- Expose one `LiveWindow.snapshot` with `complete` on Swift and Kotlin.
- Publish `Entity.snapshot` built in the core on Swift and Kotlin.
- Read entity nodes only when the row is absent with `fetch: ifMissing`.
- Repeat `local` declarations with distinct singleton sources in create operations.
- Filter all subscription effects with value lists and `not`.
- Share subscription streams across matching model declarations with atomic row updates.
- Declare scalar or array membership for event-driven node insertion into loaded lists.
- Model event streams wait for subscription readiness and report opening failures.
- `enterForegroundForTesting()` sends a foreground event to a database opened with `openTesting`.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- Wait for the first local core snapshot in `waitForInitialLoad()` when an entity has no fetch.
- `members_complete` windows read SQLite slices without retaining all keys and reject sizes above 200 rows.
- Derived `preload` requires a positive `limit` for the entire parameter product.
- Reject `local from:` in update operations.
- Replace single-string subscription matches with lists of values.
- Replace the `events` property with the suspending `events()` call on Swift and Kotlin.

### Fixed

- Preserve nullable list membership when a column changes to null and back.
- Reset cached server data after membership rules or node insertion rules change.
- Share node requests while preserving insertion requested by an event.
- Keep upload operations open when the content response contains an unreadable record.
- Preserve each active subscription declaration's debounce, effects, nested fields, events, and failure recovery.
- Apply the supplied definition when subscribing directly to a declared operation.
- Give each event collection an independent reader, including after cancellation or completion.
- Wait for connectivity and server confirmation when opening model events.
- Wake every waiting event read on close and preserve opening error kinds.
- Rewrite temporary ids in reference arrays when their records are confirmed.
- Report upload file sizes before transfer starts, including while create confirmation is pending.

## 6.11.0 - 2026-10-27

### Added

- `openTesting(listening: false)` holds model subscriptions until `setListening(true)`.
- Upload actions return the declared fields of the final mutation.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- Uploads with empty operations report the cause and the required operation definitions.

## 6.10.2 - 2026-10-27

### Added

- `Entity.waitForInitialLoad()` waits for the read the entity started and sends no request.
- `open(listening: false)` and `DB.setListening(_:)` / `DBlayer.setListening(allowed)` hold the model subscriptions that a committed credential starts until the app allows them.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- `Entity.refreshError` becomes `Entity.read`: `notLoaded`, `loaded` or `failed(error)`.

## 6.10.1 - 2026-10-27

### Added

- `LiveList`, `Live`, `LiveView` and `rememberLiveList` take a `RelatedQuery` and load its source scope like a `Query`.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- An upload that waits for the network reports `waiting` with `waitReason` `offline`, and its record stays `pending`.

### Fixed

- A model with several node relations generates one `entity(id, fetch:)`.

## 6.10.0 - 2026-10-27

### Added

- Kotlin `rememberLiveList(query)` and `rememberLiveWindow(window)` own the scope of a live list in Compose.
- `Model.entity(id, fetch: .force|.stale|.none)` reads the node when a reader appears, and `refreshError` shows a failed read.
- `merge: :stable_list` keeps the places of a stored id list, appends new ids and drops vanished ones.
- `refresh_on_foreground: true` on a scope reloads its read windows at every foreground event.
- A window over a scope loads it, and `expand(before:)` loads an older page.
- `window(tail: 60, max: 240)` shrinks the window to `tail` rows past `max`.
- `window(tail:).related(Relations.x)` reads the targets of window rows.
- `optimistic ids: { include: { of: Model, field: :id }, when: :flag }` adds or removes an id.
- `optimistic count: { at_least: 1, when: :flag }` raises a number while the flag is true.
- `subscribe id: ["chatId", "userId"]` keys a row by a pair, and `delete_unless:` deletes on false.
- `changed_by "AREA", via: :node, id_of: { of: Model, field: :attr }` rereads the node a row points to.
- `optimistic attr: { from: :param, map: { VALUE: "literal" } }` writes the literal that belongs to an enum parameter.
- `local :attr, from: { of: Model, field: :name }` fills a `create` attribute from a singleton row.
- `changed_by` may name one area as `via: :node` and as `via: :observed` on a model.
- A committed credential reloads stale windows with readers and restarts stopped subscriptions.
- `preload vibeId: { from: "Vibe.list" }` loads one scope for each row of another scope, and a new row adds its scope.
- `DB.refreshPreloads()` and `DBlayer.refreshPreloads()` reload every `preload` scope in one call, fresh or not, and name the scopes that failed.
- `add_to` and `remove_from` join a row to a scope or take it out.
- `create_row` adds a row of another model that the server answer replaces.
- `change_row` changes attributes of another row and shifts numbers with `minus:` or `plus:`.
- A refused operation takes back only its own membership, created row and changed attributes.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- `LiveWindow` connects its scope like `LiveList` instead of reading the local database only.
- `preload` waits for the first snapshot of the app and runs behind loads the app asked for.
- A committed credential starts the model subscriptions, and `logout()` and `sessionReset()` stop them.

### Fixed

- A freed owner of entities returns `subscriptionCensus` to its base: the core drops the entity observer when its last id is released and creates it again on the next retain.

### Removed

- `DB.listen()`, `DB.unlisten()`, `DBlayer.listen()` and `DBlayer.unlisten()`.

## 6.9.2 - 2026-10-26

### Added

- An optimistic keyed element carries the nested object of the local row its declared `ref:` and `id:` name.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- `expires_in` of a memory model counts from the last write, and a write of equal values renews it.

### Fixed

- `entity(id)` of a record whose reader was freed no longer calls the core again while its value or absence is retained.
- `confirmed` returns the server id to a waiter that the closing commit of a create wakes.

## 6.9.1 - 2026-10-26

### Fixed

- A live read opened while a server reply replaces a temporary row returns the rows before or after the swap, never an empty set.

## 6.9.0 - 2026-10-26

### Added

- `preload` parameter on `DB.openTesting` and `DBlayer.openTesting`, default false, keeps scope preloads quiet in tests.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- `preload` scopes wait while the device saves power and never load on a low-RAM Android device.

### Fixed

- An action that declares a result reads its answer when the payload row is null.
- `changed_by` accepts one change area on several models, each model rereads its own read.
- Local writes check nested objects and lists against their declared type.
- `TestClock.advance` expires due memory rows and resets ephemeral values at once.

## 6.8.0 - 2026-10-26

### Added

- `DB.sessionCall` and `DBlayer.sessionCall` send a declared session operation and decode the answer.
- `sessionResume`, `sessionReset` and `retryAll` on `DB` and `DBlayer`.
- `Model.retry(id:action:)` and Kotlin `ModelDecoder.retry(id, action)` retry a record by id.
- `preload` in a `.dbl` scope loads the scope after open, sign-in, reset and network return.
- `RetryProgress` reports encrypted HTTP calls waiting for a retry through `retries` in `open` and `serverConfig`.
- `Model.events` streams declared change events with the row before and after.
- `missed` event item reports the count of events dropped by a full queue.
- `changed_by ..., via: :observed` rereads every observed read of the model.
- Background uploads on iOS through a background `URLSession` and `DB.handleBackgroundSession`.
- Background uploads on Android through a user-initiated job and a `dataSync` foreground task.
- `Query.related(Association)` reads typed related rows, and `Model.Relations` lists links.
- `Query.loadAll(force:)` reads every page of a related list to its confirmed end.
- `UploadOperations` declares upload operations once in `DB.open` and `DBlayer.open`.
- `uploads[action]` on a model and `SyncInfo.uploads` report one upload per declared action.
- Generated GraphQL inputs support recursive input objects and `@oneOf` choices.
- Kotlin `JsonElement.jsonText()` serializes JSON with the library settings.
- Swift `Entity.changes()` streams the current value of an entity, then its updates.
- Stale observed windows reload when the app returns to the foreground or the network returns.
- `attach`, `file`, `read`, `url` and `detach` on a record for each `:file` attribute.
- `FileInfo` describes a stored file on Swift and Kotlin.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- `create` and `update` refuse an undeclared field or a wrongly typed value.
- A row with a temporary id sorts before permanent rows of equal order.
- The store hook runs after the writer lock is released.
- `serverConfig(retryListener:)` becomes `serverConfig(retries:)`.
- A `:file` attribute reads as `FileInfo?`, and `create` and `update` take no file parameter.
- `takeEdits()` becomes `takeUpdate()`: rows, edits and revision from one delivery.
- A delivery that does not fit the rows gives one whole list.
- The subscription stops with a failure after two refusals in a row.
- `Snapshot.error` and `LiveWindow.failure` keep typed errors.
- A live query delivers again when only the GraphQL `extensions` of the same error change.
- Stored operation, upload and sync-state errors keep GraphQL `extensions` as node rows.
- An existing database file is recreated on open.
- Cable reopens right after the network returns, without waiting for a retry delay.
- Swift query conditions and `JSONValue` encoding throw on serialization failure.
- Generated Swift scopes and upload variable builders throw on serialization failure.
- Kotlin `parseJsonObject` throws when the body is not a JSON object.
- Cancelling a single upload after its content mutation starts returns false.
- Uploads reject local and memory models without creating records or files.
- `upload`, `uploadCreate` and `cancelUpload` take the declared upload action.
- A record holds one upload per declared action, and an undeclared action admits nothing.
- Upload requests are typed records, and the content mutation comes from the declared upload action.
- `retry`, `discard` and `settled` take an `action`, `nil` or `null` for the whole record.
- `settled` of one action reports the failure of that action only.
- `SyncInfo.actions` lists ordinary operations only.
- Failed uploads older than 7 days are removed at the first flush after start.
- A failed batch item is removed on its own and its siblings stay.
- `Query.cached()` and `cachedRow()` are `async` in Swift and `suspend` in Kotlin.
- `Database.peekQuery`, `Database.edgeValues` and `<scope>Edge` reads are `async` or `suspend`.
- Database calls, writes, actions and operations run off the caller thread.
- Kotlin live lists and windows publish their state from a background thread.
- Observation reads queued, running or awaiting application are limited to two.
- A commit that touches no row of a query no longer notifies that live query.
- A refresh deferred by the read limit rereads only its touched rows.
- A live query on a remote relation delivers its first snapshot without waiting.

### Fixed

- An expiring memory row written again before its removal commits stays alive.
- A batch of server events keeps both rows that touch the same parent.
- The answer of an action is ready when its operation turns committed.
- A refused batch of server events reloads the observed lists of its model.
- A failed reload reports a gap to the readers of model events.
- A session failure is not hidden by another subscription's failure.
- An unreadable mutation answer keeps the operation in delivery unknown.
- A destroy answered with `NOT_FOUND` closes as deleted.
- Explicit null stays distinct from an absent field after reopening the database.
- Non-finite numbers fail serialization instead of becoming null.
- Fields named like SQL keywords work in storage, indexes, filters and updates.
- Generated APIs escape Swift and Kotlin keywords and keep serialized keys.
- Rows joining one list in a single commit receive distinct order keys.
- Kotlin GraphQL subscriptions end when the server closes the WebSocket.
- Recovery and temporary-id replacement keep equal ids of different models separate.
- Interrupted database resets resume from the complete recorded model list.
- Cloud replies from before a database reset are rejected.
- `UseLocal` keeps edits made while remote clocks are fetched.
- Cloud values received before their server row are kept until the row arrives.
- Subscriptions connect without waiting for storage.
- File cleanup failures are reported, and committed row changes stay committed.
- A failed multipart `complete` keeps confirmed parts and the upload id.
- A database reset rejects upload admission queued before the reset.
- Bundled SQLite respects the macOS 14 deployment target.
- Discarding an upload keeps its multipart abort until the server answers.
- A finished upload never records a multipart abort.
- Discarding one item of a rejected batch operation removes only that item.
- A rejected batch operation older than 7 days expires like a failed item.
- A panic inside an upload task fails that upload and frees its file slot.
- One unreadable upload does not stop the other waiting uploads.
- Two uploads of one file no longer fail when they write the cache copy together.
- `flush` reports a failure while removing expired uploads and retries at the next start.

### Removed

- The `liveHasMany` bridge call; use `Query.related`.
- `readEdgeValues` and generated `read<Scope>Edge`; use `edgeValues` and `<scope>Edge`.
- `createBlob` and `multipart` from `UploadRequest` and `UploadBatchRequest`.
- `RetryListener` and `Database.setRetryListener`.
- Swift `Model.retryAll()` and Kotlin top-level `retryAll()`; use `DB.retryAll()`.
- The single `upload` property of a record.
- Swift `JSONValue.parse`.

## 6.7.0 - 2026-10-24

### Added

- Entities expose opening, present, absent, failed and closed phases.
- `CableLease` and `SubscriptionSink` receive the transport token through `opened`.
- `loadMore` reports loaded pages, exhausted pagination and concurrent loading.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- Custom transports report tokens through leases instead of returning strings.
- `DB.open` and `DBlayer.open` take a headers closure read on every request.
- Kotlin `update` and `set` write null through `GraphQLNullable`.
- Kotlin maps the DSL type `:int` to `Long` in every generated output.
- Swift `JSONValue` keeps integers exact as `Int64`.
- Swift cached reads throw when reading or decoding fails.
- `Snapshot.total` and `WindowEdges.total` use one integer type on each platform.
- Android uses JNA 5.19.1.

### Fixed

- A Kotlin process no longer aborts on a corrupted foreign future result.
- A stored operation with an invalid id fails instead of losing the id.
- Waiting for a record matches its model as well as its id.
- An answer kept for one model stays available when another model asks first.
- A connection with an invalid `hasNextPage` fails instead of ending the list.
- Cancelled transport calls release tokens reported before or after cancellation.
- Interrupted cable closing keeps token ownership until cleanup completes.
- Logout waits until subscribers receive the reset revision.
- Cache resets discard unobserved loading windows.
- Subscription failures survive notification queue overflow.
- Read and decoding errors reach callers or live snapshots.
- Invalid subscription requests leave no open socket.
- Cancelling live reads does not show a failure.
- Concurrent initialization with another UniFFI library avoids a JNA lock inversion.

### Removed

- Separate query loading-state streams.
- Swift `UrlSessionTransport` initializer that takes a headers dictionary.

## 6.6.0 - 2026-10-23

### Added

- `DatabaseNotOpen` error for calls on a closed or unopened database.
- `generate_kotlin_bindings.sh` generates Kotlin bindings from explicit paths.
- `MockTransport` mode `networkError` answers an unqueued call with a network error.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- A subscription delivers rows, phase, error and revision in one value.
- The first value of a subscription waits for the first read of its rows.
- A background load error with rows keeps the phase loaded and carries the error.
- `DB.database` throws `DatabaseNotOpen` instead of crashing when the database is closed.
- `subscriptionCensus`, `entityCensus` and `openReport` throw on a closed database.
- `stream()` of live sources and query changes is `async throws`.
- Kotlin `LiveSource.flow` throws the subscription error when collected.
- A singleton entity of a closed database becomes closed.
- A rejected query fails only its caller, the relation window stays intact.
- An entity delivery carries `failure` when its read failed.
- `MockTransport` fails the test that makes a call without a queued reply.
- `MockTransport` records an unknown unsubscribe token on both platforms without throwing.

### Fixed

- `logout` returns after held entities received their removal.
- Switching lists no longer opens a second viewer changes subscription.
- A Kotlin process no longer aborts when an async transport callback completes early.

## 6.5.0 - 2026-10-23

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- A subscription returns at once, its first value arrives after a reader thread reads it.
- `retain` returns without a database error, a failed read keeps the last rows.
- An invalid relation query fails the window of that relation with `invalid`.
- On Kotlin a second class loader in one process throws `IllegalStateException`.
- Mock transports record unsubscribe tokens they never issued, the Kotlin mock throws.

### Fixed

- A panic while handling one observed group no longer stops other groups.
- Sync, uploads and cloud share one network state per database.

## 6.4.0 - 2026-10-23

### Added

- `DBlayer.close()` and `DB.close()` close the database for tests and process end.
- `Entity.isClosed` marks an entity whose database closed.
- `entityCensus()` and `entityLeaseFailures()` count entity reads, skips and failed leases.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- A second `open` while the database is open throws. Open it once at app start.
- `DBlayer.database` and `DB.database` cannot be assigned from outside.
- Signing out keeps held entities: they show empty, then the next account's rows.
- A returning network reloads observed windows whose load failed offline, once.

### Removed

- Public `Database.open` and `Database.openWithClock`.

### Fixed

- An entity receives its row when the first read for it failed.
- A load after reopening the database is no longer discarded.

## 6.3.0 - 2026-10-22

### Added

- Local models and `local: true` attributes sync across devices of one account.
- `cloud` in a model lists the attributes that sync.
- `connectCloudKit(container:)` syncs through the CloudKit private database on iOS.
- `connectDrive(activity)` syncs through Google Drive app data on Android.
- `CloudKitLink.receive` passes a CloudKit push to the library.
- The open report names the changed models and the reset models.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- Android requires API 33 or newer.
- A changed spec of any server model clears the whole server cache.
- Local models and `local: true` values survive a server cache reset.
- Associations and storage join the model fingerprint, which rebuilds the cache once.

### Fixed

- Cancelling one caller of a shared load no longer fails the other callers.
- A cancelled load no longer leaves its relation in the loading state.

## 6.2.0 - 2026-10-22

### Added

- A scope with parameters opens one live subscription per parameter set.
- Generation with `--schema` checks subscription arguments against the server schema.

### Fixed

- Scope subscriptions send the read parameters as subscription variables.

## 6.1.0 - 2026-10-22

### Added

- `.dbl` declares fields with `attribute` and scope arguments with `parameter`.
- `query`, `response from:`, `paginate size:`, `freshness` and `subscribe` describe the server side of a scope.
- `upsert`, `match_by`, `insert_on` and `stable_order` name response, correlation and list rules.
- `validates :items, length:` sets the size limits of a batch upload.
- Generation fails on an unknown, incomplete, repeated or misplaced declaration.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- One change to a scoped list reads only the changed rows.
- A second window over the same query shares its order index.
- `max_pages` counts fetched pages and resets on refresh.
- A confirmed create no longer reloads lists that insert rows from events.
- The cache format changed. The cache is rebuilt on the first open.

### Removed

- The previous `.dbl` words: `field`, `fetch`, `root`, `lands`, `correlate`, `joins`, `listen`, `updates` and others.
- `local_effect` and its `reason`.
- Untyped `:object` and `:array` fields. Use `:json` or a named type.
- `default` on non-enum scope parameters.

## 6.0.0 - 2026-10-22

### Added

- `parse: :json` on a `raw` field stores a JSON text string as a JSON value.
- `Query.changes` collapses an overflowing queue into one `replaced` change with a full snapshot.
- `retry()` and `retryAll()` return the number of retried operations and throw errors.
- Kotlin `DBlayer.open` and `DBlayer.serverConfig` take a `Context` and watch the network.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- A repeated `open` reuses the database only for the same callback instances.
- Encrypted requests retry without a limit online and make no attempt offline.
- `load()` fails at once with `Offline` when the device has no network.
- Bootstrap waits for the network before the first request.
- A mutation that is not delivered after 40 attempts stays `Failed` with its edit.
- A rollback happens only when the server refuses a mutation.
- A local write keeps fields of pending edits against loads, events and responses.
- A row from an event or `lands` joins a loaded list missing it.
- `lands` accepts a list root.
- `discard` of a batch member updates the saved operation input.
- The database format changed. The cache is rebuilt on the first open.

### Fixed

- Several rows written in one transaction no longer get the same list position.
- A batch rollback restores the parent row values.
- A dropped upload item removes all its saved state.
- Cancelling a native HTTP request stops the response body read.

### Removed

- The second platform transport: Swift `DatabaseTransport`, `GraphQLTransport`, payload signing and Kotlin `Crypto`.
- Equality of callback objects in `open`.

## 5.2.0 - 2026-10-21

### Added

- `renumber` in a batch declaration rewrites position and size of the remaining rows after `discard` or `cancelUpload`.

## 5.1.0 - 2026-10-21

### Added

- `uploadCreate` with a list of `UploadItem` creates 2 to 10 rows with uploads.
- `batch` in an upload operation declares one content mutation with an items list.
- Batch rows are replaced by the answer through their `correlate` key without duplicates.
- One failed file fails only its row. Below the minimum, single mutations are sent.

## 5.0.3 - 2026-10-21

### Fixed

- A new row from an operation response with `lands` joins loaded `joins: :events` lists.

## 5.0.2 - 2026-10-21

### Fixed

- A repeated open with equal credentials, transport and HTTP values reuses the database.

## 5.0.1 - 2026-10-21

### Fixed

- A `loadMore` refused with `INVALID_CURSOR` replaces the list window with the first page.

## 5.0.0 - 2026-10-21

### Added

- `Query.changes` delivers inserted, updated, moved and removed rows of a live query in Swift and Kotlin.
- `RetryListener.retrying(attempt, delayMs, reason)` reports encrypted HTTP retries.
- `Database.setRetryListener` installs or removes the listener. `serverConfig` takes `retryListener`.
- Generated Kotlin models carry Compose `@Immutable`, so applications need no stability config.
- `build_ios.sh --device` builds the iPhone slice only. `build_kotlin.sh --release` builds the two ARM libraries only.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- `DB.open` is `async throws` in Swift and `DBlayer.open` is `suspend` in Kotlin.
- Opening runs on the library worker pool. Repeated identical opens do nothing, changed parameters throw.
- Encrypted requests wait through network errors, timeouts, HTTP 408, 429 and 5xx instead of failing.
- `TIMEOUT`, `SERVICE_UNAVAILABLE`, `INTERNAL_SERVER_ERROR` and `DATABASE_ERROR` retry without an attempt limit.
- Delays double from 1 to 30 seconds with 20% jitter. Applications need no startup retry.
- A confirmed temporary refusal repeats with a new requestId and the next sequence.
- Cancelling an encrypted call stops the HTTP request and backoff and keeps Pending for resume.
- Swift `Task.cancel` reaches the core for bootstrap, `sessionCall` and `sessionResume` only.
- Kotlin encrypted HTTP uses 30-second call and read timeouts and a 10-second connect timeout.
- Swift encrypted HTTP uses 30-second request and resource timeouts without a separate connect timeout.
- Native release libraries use one codegen unit and symbol stripping. Android libraries shrink by about 39%.
- Android consumer rules keep JNA classes and native methods instead of every DBlayer class.
- Build scripts read the core count with `nproc` and fall back to `sysctl`.

### Removed

- The x86_64 simulator slice is no longer in the default iOS build.
- The default Android build no longer includes x86 and x86_64. `--abis` builds all four ABIs.

## 4.1.0 - 2026-10-21

### Added

- `typed: [afterSequence: :int, pinned: :bool]` declares scope params that are sent as a JSON integer or boolean. Each value has its own window.
- Swift and Kotlin take the typed params as `Int` and `Bool` (`Long` and `Boolean` in Kotlin).

## 4.0.0 - 2026-10-20

### Added

- `EncryptedSessionOperation` marks a session operation as sessionless in `sessionOperations`.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- `sessionOperations` maps each name to an `EncryptedSessionOperation` with a path and `sessionless`.
- A sessionless call is sealed without a credential and a sequence.
- A sessionless call stores no pending request and takes no mutation lock.
- A pending request refused with HTTP 400, 404, 405, 413, 415 or 422 is dropped.
- A dropped request keeps the sequence and fails its queued operation.
- `sessionResume()` returns `nil` or `null` for a dropped request instead of throwing.

### Fixed

- A stored pending request of a sessionless operation no longer blocks every app start.

## 3.0.0 - 2026-10-20

### Added

- `node` and `nodes` read entities by id, with freshness windows and `force`.
- Interface and union lists, input lists and `oneOf` inputs in operation arguments.
- Enum lists and enum scope params with defaults.
- Connection edge values and totals are stored per scope and read beside the list.
- Stable order for scopes with `order_stable: true`.
- Encrypted cable subscriptions with Swift and Kotlin sockets.
- Declared change subscriptions: `changes` and `changed_by` reread or remove rows.
- `apply` on `listen` and `updates` selects `ephemeral`, `reread` or `ignore`.
- `EncryptedConfig` takes `endpoints`, `protocolLabel`, `cablePurpose` and `cableChannel`.
- `GraphqlExtensions.fields` carries any other extensions key as JSON text.
- Persisted query retry and versioned user agents.
- Database opens without a transport.
- Uploads are stored as operation items and resume through their saved requests.
- Upload retries are kept per step and `UploadInfo.attempts` sums unfinished steps.
- Mutation idempotency keys survive restarts.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- Cable retries back off with jitter and reset only after a server frame.
- Any server error code of 1 to 64 uppercase characters is accepted.
- A refused subscription stops until an explicit resume.
- Singleton subscriptions stay stopped after logout until `resume`.
- Queued mutations are kept after a lost session.
- Server paths and protocol labels come from the application, not the library.
- Bootstrap config is fetched before the database opens.

### Fixed

- A cable that closed after `hello` no longer reconnects every second.
- Upload status checks and offline cancellation no longer block.
- An explicit null resets a `belongs_to` key.

### Removed

- Window membership snapshots.
- Built-in server paths, channel names and change subscription.

## 2.0.0 - 2026-10-17

### Added

- Typed enums in Swift and Kotlin, with an `unknown` case that keeps the server text.
- `scalar :Name, as: :kind` maps a custom scalar to a generated type.
- Union fields declared with `on` blocks become a Swift enum and a Kotlin sealed class.
- A union with an unknown `__typename` is read as `unknown` and keeps the type name.
- A union is stored in columns, so `where` by variant kind works.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- Enum fields, inputs, arguments and results are typed enums instead of strings.
- A model enum column keeps a value outside its declared list.
- A custom scalar without a mapping fails generation.

### Removed

- The string form of enum values in generated code.

## 1.2.0 - 2026-10-17

### Added

- `Query.window(tail:)` and `LiveWindow` keep only the last rows of an ordered query live.
- Window snapshots carry `hasBefore`, `hasAfter` and `total`; `expand(before:)` grows the window.
- Windows work over sorted relations with a parameter column and joins.
- `Database.openEncrypted` sends GraphQL V3 requests as JWE with a pinned server key.
- `sessionCall`, `sessionResume`, `sessionReset` and `serverConfig` run the V3 sign-in session.
- A V3 mutation is stored before sending and resent unchanged after a restart.
- Typed session errors in Swift and Kotlin, with `CredentialStore` and `EncryptedHttp` hooks.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- One change to an ordered list reads the touched rows, not the whole list.
- A landing page looks up its members by id instead of loading the list.

## 1.1.0 - 2026-10-16

### Added

- `Snapshot.edits` and `LiveList.takeEdits()` give ordered row steps, joined across missed deliveries.
- `replay` applies the steps to a mirror of the rows.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- The list feed keeps decoded rows between deliveries and undoes a refused delivery without copying.

## 1.0.1 - 2026-10-16

### Fixed

- The `dblayer-gen` crate builds against the core version.

## 1.0.0 - 2026-10-16

### Added

- Normalized SQLite storage: typed columns, child tables for lists, objects and arrays, node rows for free-shaped values.
- Redesigned model declarations in `.dbl`.
- Nested value types generated from model declarations.
- Check of model declarations and `args` against the GraphQL schema.
- Typed `args` on create and update.
- Operation blocks for calls without a model.
- FTS5 text search and searchable fields in query builders.
- Local storage mode: writes never enter the outbox.
- Local fields that sync never overwrites.
- `peek` and warm reads from memory.
- Typed transport errors in Swift and Kotlin.
- Rows carried as cells over the bridge.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- Outbox, sync state, window ids, quarantine and reset intent are stored in tables.
- A database file that does not fit the build is recreated on open.
- File calls run off the caller thread.
- A commit batches the old-row read and the node and list inserts.
- An operation whose root field is a list returns a list.

### Fixed

- Parent fields omitted by a child update are preserved.
- Entities are seeded from cached rows, which carry stored revisions.
- Background synchronization stops when its owners leave.
- Entity sources are released when their readers leave.
- Entity deliveries from replaced databases are ignored.
- Live lists await forced initial loads.

### Removed

- Multi-row inserts, which made upsert slower.

## 0.3.0 - 2026-10-13

### Added

- Entity registry shared by Swift and Kotlin, with a singleton entity.
- List deliveries carry changed rows with positions and apply to the array.
- `Query.load` loads a stale relation without a subscription.
- Test clock for core time windows.
- Replaced ids and load kind exposed in Swift and Kotlin.
- Merge rule declared on a field.
- Patch of one array element by key in an optimistic action.
- Parent pointer ordered by a tuple of child fields.
- Cable payloads delivered as database responses.
- Multipart init sends the checksum.
- R8 keep rules for JNA and UniFFI bindings.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- Dates are stored as UTC text with milliseconds.
- An event row joins the loaded lists that declare it without a reload.
- Cable reconnect backs off by failed opens.
- Outbox is retired on logout.
- Kotlin module compiles against SDK 37.
- Unused write policy, admission, pacing and sync code removed.

### Fixed

- Confirmed list members survive stale responses.
- A sent temp row is replaced by the server row that lands first.
- A param-less remote list takes the rows this device creates.
- A thread that cannot start is reported as an open error.
- Poisoned locks are recovered.
- Failed entity reads stay dirty and are re-read at the next batch.

## 0.2.0 - 2026-10-11

### Added

- One multiplexed cable channel for all subscriptions of a connection.
- `resume` on the DB entry.
- Format version derived from a hash of the core table DDL.
- Parent field sets and a newer guard on `belongs_to`.
- Create of a record together with its upload.
- Declared result of an action.
- Server total in the load state of a paged list.
- Rows move between lists on param change; lists order by relation.
- Responses merge by the fields they select.
- Rows outside the response root land from an action.
- Refetch of a relation of another model after an action.
- Debounced model listen merges events per row.
- Auth token is read per request.

### Changed

- `Transport` and `EncryptedHttp` report raw failure facts and the core merges the headers and the `User-Agent`.
- A reload after a commit waits for in-flight loads.
- Transport response bodies are read off the caller thread.
- A wipe of an incompatible database keeps `row_sync`; missing core tables are created at open.

### Fixed

- A failed subscription ends its live stream.
- A frame without data fails as internal.
- A refusal is kept until settled reads it.
- A send of an unregistered mutation fails.
- The default is stored when the server answers null for a non-null field.
- Integer ids are sent as numbers.
- The nested parent of an `id_from` field is kept.

## 0.1.0 - 2026-10-09

### Added

- Rust core with SQLite storage.
- Swift package and Kotlin module over a UniFFI bridge.
- `.dbl` parser and code generation for Swift, Kotlin and the core spec.
- GraphQL queries, mutations, actions and subscriptions.
- Live data with diffs, merge-based replace and automatic refetch.
- Relations with params, scopes and enum grouping.
- Ephemeral values.
- File fields and uploads.
- iOS and Android build scripts and `check_dblayer.sh`.

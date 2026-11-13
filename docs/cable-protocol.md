# Cable protocol

`transport::CableLink` implements the cryptographic exchange for `EncryptedConfig.endpoints.cable`. It has no socket, timer, database or credential storage access.

| Call                                             | Result                                                  |
| ------------------------------------------------ | ------------------------------------------------------- |
| `CableLink::open(config, challenge, credential)` | Connection state and RSA-OAEP/A256GCM hello             |
| `seal(body)`                                     | Client frame with the next sequence number              |
| `receive(token)`                                 | Authenticated server message with validated fields      |
| `close()`                                        | Erases both directional keys and refuses further frames |

Use the HTTP `EncryptedConfig` for cable. The bundled SPKI must match its SHA-256 pin, and the challenge must name that same pin.
The hello binds the audience, path, challenge, credential, client X25519 public key and random response key.
HKDF-SHA256 derives separate client and server keys from the X25519 secret and response key.
The first server frame must be `ready` with the challenge digest. Repeated ready messages, reordered frames and unknown fields are refused.

Frames use compact JWE with the exact protected header `{"alg":"dir","enc":"A256GCM"}`.
Client frames are limited to 32 KiB. Server frames are limited to 256 KiB. Each direction permits 2^20 frames per connection.
An incoming failure retires both keys. An authenticated `closing` message returns its code and reason, then retires both keys.
`CableError` distinguishes untrusted pins, invalid challenges, invalid frames, sequence failures, size limits, exhausted keys and closed connections.

X25519 private material, derived keys and temporary hello plaintext use zeroing storage. Dropping a connection also erases its stored keys.
The public vectors in `fixtures/change-events/kat.json` contain test keys only. Tests compare hello plaintext and all four encrypted frames without printing key material.

Swift and Kotlin use the same `EncryptedConfig(origin, serverKey, pin, endpoints, protocolLabel, cablePurpose, cableChannel)` for config and `open` calls.
Fetch config with `DB.serverConfig` or `DBlayer.serverConfig` before opening the database.
Config posts to `EncryptedConfig.endpoints.bootstrap` without credentials, sequence or Cable connection. See [server sync](server-sync.md).
The platform opens the WebSocket with `openCable` and reports every frame to the `CableEvents` the core passes in. Rust handles encryption, subscriptions, reopening and timers.

## Platform sockets

`DB.open` and `DBlayer.open` choose a platform transport when `transport` is omitted.
Swift uses `URLSessionWebSocketTask`. Kotlin uses OkHttp WebSocket.
The core builds the address: it combines `EncryptedConfig.endpoints.cable` with `EncryptedConfig.origin`, HTTPS becomes WSS and development HTTP becomes WS. The platform opens that address as given.
Every open creates one `PlatformSocket` object that the core holds. The core sends text with `send` and ends the socket with `close`. The platform reports in order what happens on that socket and keeps no registry of sockets.

## Your own transport

`DB.open` and `DBlayer.open` take any `Transport` in `transport`. The interface has four calls:

| Call                                         | What the core expects                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `appHeaders()`                               | The headers of the app. The core reads them again for every request and cable, merges them with its own headers and the `User-Agent`, and passes the result in `HttpRequest.headers`                                                                                                                                                                                                                                                                                                                                                    |
| `send(request)`                              | One HTTP exchange of `HttpRequest` (`author`, `requestId`, `attempt`, `url`, `method`, `headers`, `body`, `connectMs`, `requestMs`, `purpose`, `responseLimit`). `responseLimit` is the most bytes of the answer to read: count while reading and fail with `RawFailure` domain `DBlayer.ResponseLimit` and code 1 at the next byte. Set the headers as given. Return `RawOutcome.response(status, etag, body)` for any HTTP answer, or `RawOutcome.failed(RawFailure)` when no answer came. Do not follow redirects, retry or classify |
| `openCable(url, headers, connectMs, events)` | Open the text socket at `url` with `headers` as given, report to `events` and return `CableOutcome.open(connection)` after the handshake, or `CableOutcome.failed(failure, handshakeStatus)`. When the core cancels the call, cancel the socket and end the call                                                                                                                                                                                                                                                                        |
| `put(target, file, identity, progress)`      | `target` is `PutTarget(url, headers, connectMs, requestMs, responseLimit)`. Send the `length` bytes of the file from `offset` with PUT, set the headers as given and return `RawOutcome.response` with the status and the `ETag`. Call `progress.sent(bytes)` while it sends. The core cancels the call when it gives up (Swift task cancellation, Kotlin coroutine cancellation): end the HTTP request then, there is no flag to poll. `requestMs` is the idle bound of the upload, a file has no total bound                          |

The GraphQL address of a database without an encrypted session is `endpoint` of the open call. The core validates the addresses it builds before it hands them to a transport.

`RawFailure` has the raw facts of the failure: `domain` (the `NSError` domain, or the class name of the exception), an optional `code` and the `message`. The core derives the class from a table of known domains, codes and exception classes. An error the table does not know is a failed connection.

| `domain` and `code`                                                                                                                                                                                                                                | Class                                                                                              |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `NSURLErrorDomain` -999, `Swift.CancellationError`, `java.util.concurrent.CancellationException`, `kotlinx.coroutines.JobCancellationException`, `kotlinx.coroutines.TimeoutCancellationException`, `java.io.IOException` with the text `Canceled` | The call was cancelled                                                                             |
| `NSURLErrorDomain` -1200 to -1206, `javax.net.ssl.SSLHandshakeException`, `javax.net.ssl.SSLPeerUnverifiedException`                                                                                                                               | The server certificate is not trusted                                                              |
| `NSURLErrorDomain` -1003, -1004, -1006, -1009, -1018, -1020, `java.net.UnknownHostException`, `java.net.ConnectException`, `java.net.NoRouteToHostException`                                                                                       | The host is unreachable or the device is offline                                                   |
| `NSURLErrorDomain` -1001, `java.net.SocketTimeoutException`, `java.io.InterruptedIOException`                                                                                                                                                      | A timeout of the call expired                                                                      |
| `DBlayer.ResponseLimit` code 1                                                                                                                                                                                                                     | The answer passed `responseLimit`: a read fails with `ResponseTooLarge`, a mutation is interrupted |
| anything else, `url`                                                                                                                                                                                                                               | Any other failure of the connection                                                                |

The core sets the timeouts and the platform applies them. On Apple platforms the applied bound is the idle timeout of the request (`URLRequest.timeoutInterval = requestMs`): there is no connect bound and no total bound. Kotlin applies all four: connect, read, write and call.

`CableOutcome.failed` carries `handshakeStatus`, the HTTP status of a refused upgrade when the server answered. The core turns a status other than 101 into an `Http` error.

`CableEvents` has `frame(text)`, `binary(bytes)` and `ended(closeCode, reason, failure)`. Call them from one serial context per socket, in the order they happen. Call `ended` once when the socket ends: `closeCode` and `reason` are the close frame of the peer when it sent one, `failure` is the raw error when the connection broke. A binary frame ends the cable: the core closes the connection with code 1003. The core keeps at most 256 unread frames per connection and ends a connection that outruns its reader with code 1013.

`PlatformSocket.send(text)` returns false when the socket refuses the text. `PlatformSocket.close(code, reason)` sends a close frame with that code. The core calls `PlatformSocket.cancel()` 2 seconds later when the peer has not answered. Both are safe to call again.

A transport that does not open a cable socket returns `CableOutcome.failed`, like `MockTransport`.

```swift
try await DB.open(encrypted: encrypted, credentials: credentials, modelsJSON: modelsJSON, uploadTaskTitle: "Uploading")
```

```kotlin
DBlayer.open(context, encrypted = encrypted, credentials = credentials, modelsJson = modelsJson, uploadNotification = notification)
```

## Cable closes

`DB.cableCloses()` in Swift and `DBlayer.cableCloses()` in Kotlin give a stream of `CableCloseItem` (`CableCloseStream` in Swift): `ready` when the stream starts, `close` with the `position` and the `CableClose` event (connection id, code, reason, failure, initiator and `intended`), `lost` with the `first` and `last` positions of closes that are gone, and `ended` with an `EndCause` when the stream ends by itself.

```swift
for await item in try DB.cableCloses() {
  switch item {
  case .ready: break
  case let .close(position, event): log(position, event.code, event.initiator)
  case let .lost(first, last): log(first, last)
  case let .ended(cause): log(cause)
  }
}
```

```kotlin
DBlayer.cableCloses().collect { item ->
    when (item) {
        CableCloseItem.Ready -> Unit
        is CableCloseItem.Close -> log(item.position, item.event.code, item.event.initiator)
        is CableCloseItem.Lost -> log(item.first, item.last)
        is CableCloseItem.Ended -> log(item.cause)
    }
}
```

## Subscriptions

Opening goes through the challenge, the channel confirmation, hello and an encrypted ready.
Each subscription gets its own string id. The `subscribed` confirmation yields `Ready`. A `next` message yields a GraphQL response.
Cancel sends `unsubscribe` for the chosen id only. The last cancel closes the socket.

### Server codes and subscription refusals

Copied server enumerations can reject authenticated refusals and cause repeated connections instead of stopping the affected subscription.
The client validates code shapes without copying the server's vocabulary.

| Field               | Validation and meaning                                                                          |
| ------------------- | ----------------------------------------------------------------------------------------------- |
| `error.code`        | 1-64 ASCII uppercase letters, digits or underscores. Preserve the string in the GraphQL failure |
| `error.retryable`   | Boolean. This flag alone decides whether the refused subscription retries                       |
| `closing.reason`    | The same string shape as `error.code`. Unknown reasons remain valid                             |
| `closing.code`      | Integer from 1000 through 4999. Unknown codes remain valid                                      |
| `t` and field names | Closed message grammar. Unknown message types and fields remain invalid                         |

An `error` for a subscription contains its `id`, `code` and `retryable`, without a message or reason field.
With `retryable=false`, the models of that subscription leave the live set. Their `scope.events(Model)` streams end with `ended(invalid)`. Windows of those models get no error and keep their normal freshness rules. The subscription opens again only when a new reader asks for live data. The foreground event and the return of the network do not restart it.
With `retryable=true`, the readers of `scope.events(Model)` get `gap`, and the subscription retries after backoff.
Code spelling never selects retry behavior. For example, `REFUSED` can stop or retry according to the flag.
Removing the last subscription closes the socket without cable retry attempts, timers or a new connection.
Other active subscriptions keep using their connection when one subscription is refused.

An authenticated `closing` still closes the connection as requested by the peer.
The documented session-ending close values below keep their meaning. Other valid close values use normal reconnect backoff when subscriptions remain.
A connection that drops after it delivered data reconnects at once. A connection that drops again before any new data waits for the reconnect backoff.
The code-shape check is shared by error codes and closing reasons, so new server vocabulary needs no client enumeration update.

| Subscription     | Handling                                                                                        |
| ---------------- | ----------------------------------------------------------------------------------------------- |
| `recordEvents`   | Declared response paths select the reread or local removal from `changed_by`                    |
| `bulletinAdded`  | The event creates a row of the memory model Notice without an HTTP read                         |
| `presenceSignal` | `apply` copies declared fields when `matches` accepts the event, `reset_after` resets the value |
| `recordSignal`   | The event creates a row of the memory model InteractionNotice without an HTTP read              |

After an event is applied, the core publishes a model event with the rows before and after. `via: :observed` rereads each observed read of the model once.
`recordEvents` does not replace row fields. A matching `removes_on` kind removes the cached row without HTTP or a mutation.
Other kinds use the HTTP reread through the encrypted client and the normal SQLite column write.
Memory model notices get a UUID when the server sends no id. Their rows live in memory only and `expires_in` deletes them.
A local scope with `subscribe` opens the subscription at the first reader. It releases the subscription after the last reader leaves.
Events are lost while no reader exists. The server does not replay them.
Records of undeclared areas are ignored and are not counted. A record without area, id or kind increases `refusedEvents` of `subscriptionCensus()`.
After missed events from a broken subscription or lost channel, the library marks observed selections of affected models for rereading.
The mark survives an app restart and is cleared only by a load started after the mark.
Time-based freshness does not clear the mark, even when the selection is fresh.
Node reads and remote relations default to `5.minutes`. A zero duration always expires. The first `Ready` starts no extra reread.
Expiration alone starts no request. Received events still trigger their normal immediate reread.
The REACTION area may point to several node reads. Then 1 HTTP request runs for each declared model.

While a live subscription exists, 25 seconds without an outgoing frame start an encrypted ping. Incoming frames do not restart this interval.
If no pong arrives within 10 seconds, the client closes the socket and reopens it. Other frames do not count as a pong.
Each ping gets a new session counter value, so delayed pongs cannot confirm a later ping.
The pending socket receive survives timer, subscription and cancellation events until it returns a frame or the connection closes.
The client reads its credential before opening a socket. A handshake timeout closes the socket.
Without subscriptions there are no connections and no timers. Failed opens and closes before any server frame increase a cable backoff: 1 s doubling to 30 s with 20% jitter.
Each session owns its jitter seed, allocated from an atomic session counter without reading a clock.
The first event of a subscription resets the counter. Ready and `subscribed` do not reset it.
Unknown subscription ids and malformed or empty responses preserve the retry count and pending ping deadline.
Offline state and an empty subscription list use no attempts and add no retry delay.
Going offline drops a pending retry delay. Coming back online reopens at once while subscriptions remain, and the failure count starts from zero.
The last cancellation, offline transition or transport drop interrupts a pending socket open or handshake receive.
The platform observer reports the connection state to the core, which passes it to the transport.

| Close                                                    | Result                                                                                         |
| -------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| HTTP 401 or 403 during open, a session error during open | Reopen with backoff, like a dropped connection                                                 |
| 4401, 4403, ACCOUNT_LOCKED or LOGGED_OUT                 | Reopen with backoff, like a dropped connection                                                 |
| 1003, 1006, 4400, 4408, 4409, 4429, 4430, 4500, 4503     | Reopen with backoff. A close before the first server frame after ready keeps growing the delay |

No close deletes the credential and no close ends the session. Cable receives only the function that reads the credential.
A subscription failure never reaches the `error` of a list or entity stream: windows keep their own load state. User-initiated configuration requests and queries remain available.
`DbError` keeps socket close codes and GraphQL extensions. Kotlin exposes the same data through `DbException`.

The fixtures `fixtures/change-events/subscriptions.json` hold the requests and responses of all 4 subscriptions.
`fixtures/change-events/changes.dbl` shows the choice of HTTP reads. A fake socket checks the full exchange, the HTTP JWE and the row write.

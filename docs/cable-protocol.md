# Cable protocol

`transport::CableLink` implements the cryptographic exchange for `EncryptedConfig.endpoints.cable`. It has no socket, timer, database or credential storage access.

| Call | Result |
| --- | --- |
| `CableLink::open(config, challenge, credential)` | Connection state and RSA-OAEP/A256GCM hello |
| `seal(body)` | Client frame with the next sequence number |
| `receive(token)` | Authenticated server message with validated fields |
| `close()` | Erases both directional keys and refuses further frames |

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
The platform passes WebSocket text through `openCable`, `cableSend`, `cableNext` and `cableClose`. Rust handles encryption, subscriptions, reopening and timers.

## Platform sockets

`DB.open` and `DBlayer.open` choose a platform transport when `transport` is omitted.
Swift uses `URLSessionWebSocketTask`. Kotlin uses OkHttp WebSocket.
Both combine `EncryptedConfig.endpoints.cable` with `EncryptedConfig.origin`: HTTPS becomes WSS, and development HTTP becomes WS.
Every open creates a new token. `openCable` and `subscribe` return nothing: the call reports the token with `lease.opened(token)` or `sink.opened(token)`.
Calls for that token send and receive text without interpreting the encrypted payload.
`cableClose` removes the token and cancels its socket. Further reads return no event.

## Your own transport

`DB.open` and `DBlayer.open` take any `Transport` in `transport`. The interface has nine calls:

| Call | What the core expects |
| ---- | --------------------- |
| `query(request)`, `mutate(request)` | The JSON text of the GraphQL response. A failure throws `DbError` (`DbException` in Kotlin) |
| `subscribe(request, sink)` | Opens one subscription and returns when the socket exists. Reports the token with `sink.opened`, then calls `sink.ready()` and `sink.event(json)` for each frame |
| `unsubscribe(token)` | Ends the subscription of the token. It is not async and does not throw |
| `openCable(lease)` | Opens one text socket and reports its token with `lease.opened` |
| `cableSend(token, text)` | Sends one text frame |
| `cableNext(token)` | Returns the next `EncryptedSocketEvent` (`Text` or `Closed`), or nothing once the socket is closed |
| `cableClose(token)` | Removes the token and cancels its socket. It does not throw |
| `put(url, headers, bytes, progress)` | Sends the bytes with PUT and returns a `PutReply` with the status and the `ETag`. Calls `progress.sent(bytes)` while it sends and stops when `progress.isCancelled()` is true |

A sink also takes `failed(error)` for a subscription error and `closed()` for the end of the stream. Events sent after the core released the subscription are dropped.

Order of a subscription: socket exists, `sink.opened(token)`, `sink.ready()`, then events. The shipped transports follow this order. The core does not check that `ready` comes after `opened`.

| Case | Result |
| ---- | ------ |
| `opened` once, then the call finishes | The core holds the token |
| The call finishes without `opened` | `openCable` or `subscribe` fails with `Invalid` (`open_cable finished without opened`, `subscribe finished without opened`) |
| The call fails after `opened` | The core releases the token with `cableClose` or `unsubscribe` and returns the error |
| `opened` with the token the lease holds or already released | Throws `Invalid`. The core does not release the token again |
| `opened` with another token while the lease holds a token or after it released one | Throws `Invalid`. The core releases the new token |
| `opened` after the core dropped the call | Returns normally. The core releases the token at once |

The core releases a token once per lease: after a release the lease refuses the token. A `cableClose` that the core cancels while it runs restores the lease, and the next release calls `cableClose` again with the same token. Make `cableClose` and `unsubscribe` safe for a token that is already gone. The order of `cableClose` against a pending `cableNext` of the same token is not defined, so a transport handles both.

```swift
final class FeedTransport: Transport, @unchecked Sendable {
  private let http: UrlSessionTransport
  private let feed: AppFeed

  init(http: UrlSessionTransport, feed: AppFeed) { self.http = http; self.feed = feed }

  func query(request: TransportRequest) async throws -> String { try await http.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await http.mutate(request: request) }
  func openCable(lease: CableLease) async throws { try await http.openCable(lease: lease) }
  func cableSend(token: String, text: String) async throws { try await http.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await http.cableNext(token: token) }
  func cableClose(token: String) async { await http.cableClose(token: token) }
  func put(url: String, headers: [String: String], bytes: Data, progress: ByteProgress) async throws -> PutReply {
    try await http.put(url: url, headers: headers, bytes: bytes, progress: progress)
  }

  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws {
    let token = UUID().uuidString
    let frames = try await feed.open(token: token, operation: request.operationName, variablesJson: request.variablesJson)
    try sink.opened(token: token)
    sink.ready()
    Task {
      do {
        for try await json in frames { try sink.event(json: json) }
      } catch {
        sink.failed(error: DbError.Network(detail: "\(error)"))
      }
      sink.closed()
    }
  }

  func unsubscribe(token: String) { feed.close(token: token) }
}
```

```kotlin
class FeedTransport(
    private val http: OkHttpTransport,
    private val feed: AppFeed,
    private val scope: CoroutineScope,
) : Transport {
    override suspend fun query(request: TransportRequest) = http.query(request)
    override suspend fun mutate(request: TransportRequest) = http.mutate(request)
    override suspend fun openCable(lease: CableLease) = http.openCable(lease)
    override suspend fun cableSend(token: String, text: String) = http.cableSend(token, text)
    override suspend fun cableNext(token: String) = http.cableNext(token)
    override suspend fun cableClose(token: String) = http.cableClose(token)
    override suspend fun put(url: String, headers: Map<String, String>, bytes: ByteArray, progress: ByteProgress) =
        http.put(url, headers, bytes, progress)

    override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink) {
        val token = UUID.randomUUID().toString()
        val frames = feed.open(token, request.operationName, request.variablesJson)
        sink.opened(token)
        sink.ready()
        scope.launch {
            try {
                frames.collect { json -> sink.event(json) }
            } catch (error: Exception) {
                sink.failed(DbException.Network(error.message ?: "feed failed"))
            }
            sink.closed()
        }
    }

    override fun unsubscribe(token: String) = feed.close(token)
}
```

`AppFeed` is app code: a stream of GraphQL response JSON per token, with `open` and `close`. A transport that does not open a cable socket throws `DbError.Invalid` (`DbException.Invalid` in Kotlin) from `openCable`, like `MockTransport`.

| Socket result | Result for Rust |
| --- | --- |
| Text | `EncryptedSocketEvent.Text` with unchanged text |
| Peer close | `EncryptedSocketEvent.Closed` with the peer code and reason |
| Binary frame | Close the socket and return `Closed` with code 1003 and a reason |
| Network failure | `Closed` with code 1006 and a reason |
| Open or send failure | Typed transport error from the platform classifier |

```swift
try await DB.open(encrypted: encrypted, credentials: credentials, modelsJSON: modelsJSON)
```

```kotlin
DBlayer.open(context, encrypted = encrypted, credentials = credentials, modelsJson = modelsJson, uploadNotification = notification)
```

## Subscriptions

`EncryptedTransport.subscribe` uses the shared `Transport.subscribe` and `SubscriptionEvent`, like a plain ActionCable transport.
Opening goes through the challenge, the channel confirmation, hello and an encrypted ready.
Each subscription gets its own string id. The `subscribed` confirmation yields `Ready`. A `next` message yields a GraphQL response.
Cancel sends `unsubscribe` for the chosen id only. The last cancel closes the socket.

### Server codes and subscription refusals

Copied server enumerations can reject authenticated refusals and cause repeated connections instead of stopping the affected subscription.
The client validates code shapes without copying the server's vocabulary.

| Field | Validation and meaning |
| --- | --- |
| `error.code` | 1-64 ASCII uppercase letters, digits or underscores. Preserve the string in the GraphQL failure |
| `error.retryable` | Boolean. This flag alone decides whether the refused subscription retries |
| `closing.reason` | The same string shape as `error.code`. Unknown reasons remain valid |
| `closing.code` | Integer from 1000 through 4999. Unknown codes remain valid |
| `t` and field names | Closed message grammar. Unknown message types and fields remain invalid |

An `error` for a subscription contains its `id`, `code` and `retryable`, without a message or reason field.
With `retryable=false`, the feed records `SubscriptionStatus.failure` and stops until explicit `resume` or the next committed credential.
Explicit resume restarts every stopped live subscription with a saved failure, including GraphQL refusals. The foreground event and the return of the network restart only stopped subscriptions whose saved failure is not a session refusal, and they send no cable resume command. A session refusal (`SessionCode` or a terminal close) stays stopped until explicit resume or `listen`.
With `retryable=true`, the feed retains the GraphQL error and retries after backoff.
Code spelling never selects retry behavior. For example, `REFUSED` can stop or retry according to the flag.
Removing the last subscription closes the socket without cable retry attempts, timers or a new connection.
Other active subscriptions keep using their connection when one subscription is refused.

An authenticated `closing` still closes the connection as requested by the peer.
The documented session-ending close values below keep their meaning. Other valid close values use normal reconnect backoff when subscriptions remain.
The code-shape check is shared by error codes and closing reasons, so new server vocabulary needs no client enumeration update.

| Subscription | Handling |
| --- | --- |
| `recordEvents` | Declared response paths select the reread or local removal from `changed_by` |
| `bulletinAdded` | The event creates a row of the memory model Notice without an HTTP read |
| `presenceSignal` | `apply` copies declared fields when `matches` accepts the event, `reset_after` resets the value |
| `recordSignal` | The event creates a row of the memory model InteractionNotice without an HTTP read |

After an event is applied, the core publishes a model event with the rows before and after. `via: :observed` rereads each observed read of the model once.
`recordEvents` does not replace row fields. A matching `removes_on` kind removes the cached row without HTTP or a mutation.
Other kinds use the HTTP reread through the encrypted client and the normal SQLite column write.
Memory model notices get a UUID when the server sends no id. Their rows live in memory only and `expires_in` deletes them.
A local scope with `subscribe` opens the subscription at the first reader. It releases the subscription after the last reader leaves, honoring `live_linger`.
Events are lost while no reader exists. The server does not replay them.
Undeclared areas increase `SubscriptionStatus.rejected`.
A repeated `Ready` rereads only expired observed selections of models with `changed_by`. Reopening a subscription uses the same freshness check.
The check uses each read's `freshness`, including its shorter `empty_in` duration. Missing answers and invalidated selections need a reread.
Node reads and remote relations default to `5.minutes`. A zero duration always expires. The first `Ready` starts no extra reread.
The server does not replay missed events. A fresh selection can keep old values until a read after `freshness` expires.
Expiration alone starts no request. Received events still trigger their normal immediate reread.
The REACTION area may point to several node reads. Then 1 HTTP request runs for each declared model.

While a live subscription exists, 25 seconds without traffic start an encrypted ping.
If no pong or next arrives within 20 seconds, the client closes the socket and reopens it.
Only a pong matching the pending ping's `ts` restarts the 25-second idle interval.
Each ping gets a new session counter value, so delayed pongs cannot confirm a later ping.
The pending socket receive survives timer, subscription and cancellation events until it returns a frame or the connection closes.
The client reads its credential before opening a socket. A handshake timeout closes the socket.
Without subscriptions there are no connections and no timers. Failed opens and closes before any server frame increase a cable backoff: 1 s doubling to 30 s with 20% jitter.
Each session owns its jitter seed, allocated from an atomic session counter without reading a clock.
Ready does not reset the counter. A known subscription's `subscribed` or parsed, nonempty `next` response resets it.
A pong resets it only when its `ts` matches the pending ping.
Unknown subscription ids, malformed or empty responses and unmatched pongs preserve the retry count and pending ping deadline.
Offline state and an empty subscription list use no attempts and add no retry delay.
Going offline drops a pending retry delay. Coming back online reopens at once while subscriptions remain, and the failure count stays.
The last cancellation, offline transition or transport drop interrupts a pending socket open or handshake receive.
`Sync.set_online` passes the connection state to the transport.

| Close | Result |
| --- | --- |
| HTTP 401 during open | `SessionCode::Unauthenticated`, stop until the next committed credential or explicit resume |
| HTTP 403 during open or channel `reject_subscription` | `SessionCode::AccountUnavailable`, stop until the next committed credential or explicit resume |
| 4401, including session_unknown | `SessionCode::Unauthenticated`, stop until the next committed credential or explicit resume |
| 4403 or ACCOUNT_LOCKED | `SessionCode::AccountUnavailable`, stop until the next committed credential or explicit resume |
| LOGGED_OUT | `SessionCode::SessionEnded`, stop until the next committed credential or explicit resume |
| 1003, 1006, 4400, 4408, 4409, 4429, 4430, 4500, 4503 | Reopen with backoff. A close before the first server frame after ready keeps growing the delay |

No close deletes the credential. Cable receives only the function that reads the credential.
Terminal refusals also reject later subscriptions without opening sockets or triggering rereads.
A committed credential or `resume()` clears the refusal and restarts stopped subscriptions.
Each watched query receives one `LiveChange.error` with the typed session failure until that explicit restart.
`SubscriptionStatus.failure` retains the typed subscription error. User-initiated configuration requests and queries remain available.
`DbError` keeps socket close codes and GraphQL extensions. Kotlin exposes the same data through `DbException`.
Swift `LiveChange.error` and `SubscriptionSink.failed` take `DbError`. Kotlin uses `DbException`.

The fixtures `fixtures/change-events/subscriptions.json` hold the requests and responses of all 4 subscriptions.
`fixtures/change-events/changes.dbl` shows the choice of HTTP reads. A fake socket checks the full exchange, the HTTP JWE and the row write.

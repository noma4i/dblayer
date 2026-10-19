# Cable protocol

`transport::CableLink` implements the cryptographic exchange for `/api/v3/cable`. It has no socket, timer, database or credential storage access.

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

Swift and Kotlin use the same `EncryptedConfig(origin, serverKey, pin)` for config and `open` calls.
Fetch config with `DB.serverConfig` or `DBlayer.serverConfig` before opening the database.
Config uses `POST /api/v3/bootstrap` without credentials, sequence or Cable connection. See [server sync](server-sync.md).
The platform passes WebSocket text through `openCable`, `cableSend`, `cableNext` and `cableClose`. Rust handles encryption, subscriptions, reopening and timers.

## Platform sockets

`DB.open` and `DBlayer.open` choose a platform transport when `transport` is omitted.
Swift uses `URLSessionWebSocketTask`. Kotlin uses OkHttp WebSocket.
Both derive `/api/v3/cable` from `EncryptedConfig.origin`: HTTPS becomes WSS, and development HTTP becomes WS.
Every open returns a new token. Calls for that token send and receive text without interpreting the encrypted payload.
`cableClose` removes the token and cancels its socket. Further reads return no event.

| Socket result | Result for Rust |
| --- | --- |
| Text | `EncryptedSocketEvent.Text` with unchanged text |
| Peer close | `EncryptedSocketEvent.Closed` with the peer code and reason |
| Binary frame | Close the socket and return `Closed` with code 1003 and a reason |
| Network failure | `Closed` with code 1006 and a reason |
| Open or send failure | Typed transport error from the platform classifier |

```swift
try DB.open(encrypted: encrypted, credentials: credentials, modelsJSON: modelsJSON)
```

```kotlin
DBlayer.open(encrypted = encrypted, credentials = credentials, modelsJson = modelsJson)
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
With `retryable=false`, the feed records `SubscriptionStatus.failure` and stops until explicit `resume` or `listen`.
Explicit resume restarts every stopped live subscription with a saved failure, including GraphQL refusals.
With `retryable=true`, the feed retains the GraphQL error and retries after backoff.
Code spelling never selects retry behavior. For example, `REFUSED` can stop or retry according to the flag.
Removing the last subscription closes the socket without cable retry attempts, timers or a new connection.
Other active subscriptions keep using their connection when one subscription is refused.

An authenticated `closing` still closes the connection as requested by the peer.
The documented session-ending close values below keep their meaning. Other valid close values use normal reconnect backoff when subscriptions remain.
The code-shape check is shared by error codes and closing reasons, so new server vocabulary needs no client enumeration update.

| Subscription | Handling |
| --- | --- |
| `recordEvents` | `area`, `kind` and `entityId` select the reread or local removal from `changed_by` |
| `viewerNotice` | The event creates a row of the memory model Notice without an HTTP read |
| `chatActivity` | `TYPING` updates the ephemeral `typing` of the user, `reset_after` resets the value |
| `interactionReceived` | The event creates a row of the memory model InteractionNotice without an HTTP read |

`recordEvents` does not replace row fields. A matching `removes_on` kind removes the cached row without HTTP or a mutation.
Other known kinds use the HTTP reread through the existing V3 JWE client and the normal SQLite column write.
Memory model notices get a UUID when the server sends no id. Their rows live in memory only and `expire_after` deletes them.
A local scope with `updates` opens the subscription at the first reader. It releases the subscription after the last reader leaves, honoring `live_linger`.
Events are lost while no reader exists. The server does not replay them.
Undeclared areas increase `SubscriptionStatus.rejected`.
A repeated `Ready` rereads only expired observed selections of models with `changed_by`. Reopening a subscription uses the same freshness check.
The check uses each read's `fresh_for`, including its shorter `empty` duration. Missing answers and invalidated selections need a reread.
Node reads and remote relations default to `5.minutes`. A zero duration always expires. The first `Ready` starts no extra reread.
The server does not replay missed events. A fresh selection can keep old values until a read after `fresh_for` expires.
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
The last cancellation, offline transition or transport drop interrupts a pending socket open or handshake receive.
`Sync.set_online` passes the connection state to the transport.

| Close | Result |
| --- | --- |
| HTTP 401 during open | `SessionCode::Unauthenticated`, stop until explicit listen or resume |
| HTTP 403 during open or channel `reject_subscription` | `SessionCode::AccountUnavailable`, stop until explicit listen or resume |
| 4401, including session_unknown | `SessionCode::Unauthenticated`, stop until explicit listen or resume |
| 4403 or ACCOUNT_LOCKED | `SessionCode::AccountUnavailable`, stop until explicit listen or resume |
| LOGGED_OUT | `SessionCode::SessionEnded`, stop until explicit listen or resume |
| 1003, 1006, 4400, 4408, 4409, 4429, 4430, 4500, 4503 | Reopen with backoff. A close before the first server frame after ready keeps growing the delay |

No close deletes the credential. Cable receives only the function that reads the credential.
Terminal refusals also reject later subscriptions without opening sockets or triggering rereads.
After sign-in, call `listen` or `resume` to clear the refusal and restart stopped subscriptions.
Each watched query receives one `LiveChange.error` with the typed session failure until that explicit restart.
`SubscriptionStatus.failure` retains the typed subscription error. User-initiated configuration requests and queries remain available.
`SubscriptionFailure.close` keeps the close code. GraphQL errors keep their messages and `GraphqlExtensions` in the shared error type.
Swift `LiveChange.error`, `SubscriptionSink.failed` and `GraphQLSubscriptionEvent.errors` take `DbError`. Kotlin uses `DbException`.

The fixtures `fixtures/change-events/subscriptions.json` hold the requests and responses of all 4 subscriptions.
`fixtures/change-events/changes.dbl` shows the choice of HTTP reads. A fake socket checks the full exchange, the HTTP JWE and the row write.

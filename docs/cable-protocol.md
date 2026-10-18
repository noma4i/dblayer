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
The public vectors in `fixtures/v3-cable/kat.json` contain test keys only. Tests compare hello plaintext and all four encrypted frames without printing key material.

Swift and Kotlin retain the same `EncryptedConfig(origin, serverKey, pin)` and `openEncrypted` calls.
The platform passes WebSocket text through `openCable`, `cableSend`, `cableNext` and `cableClose`. Rust handles encryption, subscriptions, reopening and timers.

## Platform sockets

`DB.openEncrypted` and `DBlayer.openEncrypted` choose a platform transport when `transport` is omitted.
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
try DB.openEncrypted(encrypted: encrypted, credentials: credentials, modelsJSON: modelsJSON)
```

```kotlin
DBlayer.openEncrypted(encrypted, credentials, modelsJson = modelsJson)
```

## Subscriptions

`EncryptedTransport.subscribe` uses the shared `Transport.subscribe` and `SubscriptionEvent`, like a plain ActionCable transport.
Opening goes through the challenge, the channel confirmation, hello and an encrypted ready.
Each subscription gets its own string id. The `subscribed` confirmation yields `Ready`. A `next` message yields a GraphQL response.
Cancel sends `unsubscribe` for the chosen id only. The last cancel closes the socket.

| Subscription | Handling |
| --- | --- |
| `viewerChanges` | `area` and `entityId` select the HTTP reread from `changed_by` |
| `viewerNotice` | The event creates a row of the memory model Notice without an HTTP read |
| `chatActivity` | `TYPING` updates the ephemeral `typing` of the user, `reset_after` resets the value |
| `interactionReceived` | The event creates a row of the memory model InteractionNotice without an HTTP read |

`viewerChanges` does not replace row fields. The HTTP reread uses the existing V3 JWE client and the normal SQLite column write.
Memory model notices get a UUID when the server sends no id. Their rows live in memory only and `expire_after` deletes them.
A local scope with `updates` opens the subscription at the first reader. It releases the subscription after the last reader leaves, honoring `live_linger`.
Events are lost while no reader exists. The server does not replay them.
Undeclared areas increase `SubscriptionStatus.rejected`. A repeated `Ready` rereads the observed selections of models with `changed_by`.
The REACTION area may point to several node reads. Then 1 HTTP request runs for each declared model.

While a live subscription exists, 25 seconds without traffic start an encrypted ping.
If no pong or next arrives within 20 seconds, the client closes the socket and reopens it.
Without subscriptions there are no connections and no timers. Failed opens increase `pacing::Backoff`. A successful ready resets the counter.
Offline state and an empty subscription list use no attempts and add no retry delay. This includes a cancel during the handshake.
`Sync.set_online` passes the connection state to the transport.

| Close | Result |
| --- | --- |
| 4401 | `SessionCode::Unauthenticated`, subscriptions end |
| 4403 or ACCOUNT_LOCKED | `SessionCode::AccountUnavailable`, subscriptions end |
| LOGGED_OUT | `SessionCode::SessionEnded`, subscriptions end |
| 1003, 1006, 4400, 4408, 4409, 4429, 4430, 4500, 4503 | Reopen with backoff |

No close deletes the credential. Cable receives only the function that reads the credential.
`SubscriptionFailure.close` keeps the close code. GraphQL errors keep their messages and `GraphqlExtensions` in the shared error type.
Swift `LiveChange.error`, `SubscriptionSink.failed` and `GraphQLSubscriptionEvent.errors` take `DbError`. Kotlin uses `DbException`.

The fixtures `fixtures/v3-cable/subscriptions.json` hold the requests and responses of all 4 subscriptions.
`fixtures/v3-cable/changes.dbl` shows the choice of HTTP reads. A fake socket checks the full exchange, the HTTP JWE and the row write.

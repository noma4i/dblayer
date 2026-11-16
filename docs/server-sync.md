# Server sync

DBlayer talks to a GraphQL endpoint. You describe operations and models once. Requests, pagination, retries, refetching and subscriptions are handled for you.

See [Retries](#retries) for request ownership, delays, budgets and cancellation.
HTTP subscription errors follow query retry classification: 5xx errors retry, while terminal errors including 401 and 403 stop.

## Connecting

1. Pass the endpoint and a `headers` closure to `open`, or create a transport with the endpoint and a token supplier. The closure runs for every request, so a refreshed token is sent without reopening the database.
2. Open the database with your models and the transport.
3. Pass your GraphQL operation types. The name and document come from the type, the response root is the first field of the document.

Use `DB.open` (Swift) or `DBlayer.open` (Kotlin) for every database.
Without `encrypted`, requests use ordinary GraphQL HTTP and the transport's existing subscriptions, without credentials, sequence numbers or APQ.

Opening is asynchronous: use `try await DB.open(...)` in Swift and suspend `DBlayer.open(context, ...)` in Kotlin.
The library opens files and recreates incompatible caches on its worker pool.
Open the database once at app start and use that instance everywhere. A second call to `open` while the database is open fails with `database is already open; open it once at app start`, whatever its parameters. An opening error permits retry. `close` ends the database, meant for tests and the end of the process, and the next `open` opens it again.

```swift
try await DB.open(endpoint: endpoint, headers: { ["Authorization": "Bearer \(session.token)"] }, modelsJSON: modelsJSON, operations: GraphQLOperations.all, uploadTaskTitle: "Uploading")
```

```kotlin
DBlayer.open(context, endpoint = endpoint, headers = { mapOf("Authorization" to "Bearer ${session.token}") }, modelsJson = modelsJson, operations = GraphQLOperations.all, uploadNotification = notification)
```

An `open` that passes at least one operation besides the upload steps fails with `Invalid` when the set leaves out an operation the library sends by itself: the subscription of the change feed, each `listens` and `updates` subscription, and the read of each relation a `changed_by` event starts. The error names every missing operation in alphabetical order, and no definition of that call is accepted. An `open` without operations or with the upload steps only passes, and the app registers the rest later. The generated `GraphQLOperations.all` holds them all.

```swift
// Invalid: the operations the library sends by itself are not registered: Changes, UserNode
try await DB.open(endpoint: endpoint, modelsJSON: modelsJSON, operations: [Greeting.self], uploadTaskTitle: "Uploading")
```

```kotlin
// Invalid: the operations the library sends by itself are not registered: Changes, UserNode
DBlayer.open(context, endpoint = endpoint, modelsJson = modelsJson, operations = listOf(Greeting), uploadNotification = notification)
```

`endpoint`, `encrypted`, `credentials`, `transport`, `path` and `http` default to nil or null.
`headers` is a closure that returns the request headers, `@Sendable () -> [String: String]` in Swift and `() -> Map<String, String>` in Kotlin. It defaults to an empty map and is called for each request and cable connection to the database origin. Presigned file uploads use their supplied headers and User-Agent. `operations` defaults to an empty list. `modelsJSON` or `modelsJson` is required.

The built-in clients send `User-Agent: DBlayer/<version>` on HTTP requests, file PUTs and WebSocket handshakes.
The core supplies the library version from `core/Cargo.toml`.
An application `User-Agent` passed through request headers retains its value, followed by a space and the library token.
For example, `Example/3` becomes `Example/3 DBlayer/<version>`.
Custom HTTP and transport implementations must set this header themselves using the core function.

```swift
let agent = userAgent(application: "Example/3")
```

```kotlin
val agent = userAgent("Example/3")
```

An omitted endpoint and transport opens a local database when `encrypted` is absent.
With `encrypted`, the default HTTP uses its origin. The default transport uses `endpoint`, or that origin when endpoint is absent.
`http` is the last optional argument and accepts custom encrypted HTTP replies.
The core rejects `encrypted` without credentials, credentials without `encrypted`, and `http` without `encrypted` as `DbError.Invalid` (`DbException.Invalid` in Kotlin).
Encrypted sessions require JWE requests.

An operation type declares `Data` (Swift) or a `dataSerializer` (Kotlin) when its generated call returns a decoded response.
Operations used by models need neither. Swift defaults `Data` to `Never`.
The library normalizes model responses into rows. Do not copy model fields into a response type.

```swift
enum ArchiveGroupMutation: GraphQLMutation {
  struct Variables: Encodable, Sendable { let id: String }
  static let operationName = "ArchiveGroup"
  static let document = "mutation ArchiveGroup($id: ID!) { archiveGroup(id: $id) { id archived } }"
}
```

```kotlin
object ArchiveGroupMutation : GraphQLOperation {
    override val operationName = "ArchiveGroup"
    override val document = "mutation ArchiveGroup(\$id: ID!) { archiveGroup(id: \$id) { id archived } }"
}
```

The transport is an interface with four calls: `appHeaders`, `send`, `openCable` and `put`. The core builds the URL, the headers (yours, its own and the `User-Agent`), the GraphQL body and the timeouts (connect 10 seconds, request 30 seconds) and classifies every raw outcome. On Apple platforms the applied bound is the idle timeout of the request: URLSession gives no connect bound and no total bound per request. Kotlin applies connect, read, write and call timeouts. Your app gets HTTP and WebSocket implementations for each platform (`UrlSessionTransport`, `OkHttpTransport`). Tests use a mock transport. A REST transport implements the same interface, see [your own transport](cable-protocol.md#your-own-transport).

No HTTP call of the library follows a redirect: a `3xx` answer to a query, an encrypted request or a file upload is reported with its status (`Http` for a query) and the server sees one request.

## Encrypted session

A server that takes only encrypted requests (JWE over HTTPS) needs no signing code in the app. The core builds and opens every request. The app gives it three things:

| Piece             | What it is                                                                                                                                                                                                                                                                                                                                         |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EncryptedConfig` | `origin`, `serverKey`, `pin`, `endpoints`, `protocolLabel`, `cablePurpose`, `cableChannel`. The key is base64 DER with at least 3072 bits. Its SHA-256 must match the lowercase hex pin. Ship both with the app. See Application protocol configuration below                                                                                      |
| Executor          | `send` runs the POST that the core built for `origin + path` with `application/jose` and returns the raw outcome. One class does it with the HTTP and WebSocket calls: `UrlSessionTransport` (Swift) and `OkHttpTransport` (Kotlin). `DB.open` and `DBlayer.open` pass it to the core, which picks it for the transport and for the encrypted HTTP |
| `CredentialStore` | `get`, `set` and `clear` keep one string. The library ships no store: your app implements it over the Keychain (iOS) or the Keystore (Android). The core creates the credential, calls `set` before the first request and never writes it to SQLite. The examples call that app code `AppCredentials`                                              |

The database uses encrypted JWE for queries and mutations, and encrypted Cable for subscriptions.
The passed transport offers a text WebSocket and file upload. The shared subscription interface is described in [Cable protocol](cable-protocol.md).

`HttpRequest.purpose` names the network of a request. `transfer` covers the upload steps, the content mutation and the abort. `standard` covers all other requests.

The Rust [cable protocol](cable-protocol.md) uses the same bundled key and pin for encrypted hello and encrypted frames.

```swift
let origin = URL(string: "https://api.example.com")!
try await DB.open(
  encrypted: EncryptedConfig(origin: origin.absoluteString, serverKey: bundledServerKey, pin: bundledServerPin, endpoints: endpoints, protocolLabel: protocolLabel, cablePurpose: cablePurpose, cableChannel: cableChannel),
  credentials: AppCredentials(),
  modelsJSON: modelsJSON,
  path: path, uploadTaskTitle: "Uploading"
)
try await DB.sessionCall("otp", body: PhoneBody(phone: "+61400000000"))
let answer = try await DB.sessionCall("confirm", body: CodeBody(code: "123456"), as: SignedIn.self)
```

```kotlin
val origin = "https://api.example.com"
DBlayer.open(
    context,
    encrypted = EncryptedConfig(origin, bundledServerKey, bundledServerPin, endpoints, protocolLabel, cablePurpose, cableChannel),
    credentials = AppCredentials(context),
    modelsJson = modelsJSON,
    path = path,
    uploadNotification = notification,
)
DBlayer.sessionCall<PhoneBody, JsonElement>("otp", PhoneBody("+61400000000"))
val answer: SignedIn = DBlayer.sessionCall("confirm", CodeBody("123456"))
```

`sessionCall` encodes an `Encodable` (`@Serializable` in Kotlin) body, sends a configured operation and decodes the answer into the type you name. Swift without an answer type returns nothing; in Kotlin name `JsonElement` as the answer type to ignore the answer. An answer with an `error` field throws `DbError.Session` (`DbException.Session` in Kotlin) with a typed `code`. See [Ending a session](#ending-a-session).

Each `EncryptedSessionOperation` declares `path` and `class`: `login` creates the credential and fails with `AlreadyAuthenticated` while one exists, `authenticated` signs with the credential, `sessionless` sends none.
For `class: .sessionless`, the request contains exactly `audience`, `kid`, `method`, `path`, `requestId`, `issuedAt`, `responseKey`, and `body`.
It omits `credential` and `sequence`, stores no pending request, and does not acquire the mutation lock.
A failed send is repeated by the library with the session retry policy, up to 40 sends and without a time limit, then the error returns to the caller.
Bootstrap keeps its separate envelope and shares only the JWE encryption primitive.

Only `logout` is a name the library reserves. The other names in the table, `otp`, `support`, `confirm` and `social`, are examples: the app sets its own names in `EncryptedConfig.endpoints.sessionOperations`.

| Operation | Body                                                                                                                                   | Answer                                                                |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `otp`     | `phone`                                                                                                                                | `nextStep`                                                            |
| `support` | `name`, `contact`, `message`, `step`, `device`, `os`, `appVersion`, `country`                                                          |                                                                       |
| `confirm` | `code`                                                                                                                                 | `nextStep`                                                            |
| `social`  | the Apple or Google sign-in fields of the server API: the source name (`apple` or `google`), `idToken`, optional `authCode` and `name` | `nextStep`                                                            |
| `logout`  | `{}`                                                                                                                                   | `revoked`. The core then deletes the credential and the session state |

| Session code                                                                                 | Meaning                                                                           | Credential and state                                         |
| -------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| `invalidCode`, `invalidPhone`, `invalidProvider`, `providerRejected`, `alreadyAuthenticated` | the server refused the step                                                       | kept, the sequence is confirmed                              |
| `rateLimited`, `accountUnavailable`                                                          | too many codes or a disabled account                                              | kept, the sequence is confirmed                              |
| `sequenceConflict`                                                                           | the server holds another request for this sequence                                | kept, the pending request stays                              |
| `unauthenticated`                                                                            | the server does not accept the session for this request                           | kept, the request is removed, the call throws `Unauthorized` |
| `sessionEnded`                                                                               | the server ended the session                                                      | erased with the account data                                 |
| `replayedRequest`                                                                            | a read arrived twice. The core already repeated it once with a new request id     | kept                                                         |
| `serverKeyMismatch`                                                                          | the session key differs from the pinned key                                       | kept                                                         |
| `stateLost`                                                                                  | a stored credential has no matching sequence state, final without automatic retry | kept, recovery belongs to the application                    |
| `noCredential`                                                                               | the call needs a session and the database has none                                | kept                                                         |
| `other`                                                                                      | any other `error` value, the text is in `detail`                                  | kept                                                         |

A sealed `RATE_LIMITED` answer with `retryAfter` of N seconds ends its request at once. No request of the session leaves before N seconds pass, the cable open included. A later `retryAfter` inside the pause moves the end of the pause.

### Ending a session

| Event                                            | What the app sees                                                                                                                                         | Credential and data                                                                                           |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `sessionReset()`                                 | Returns after the clearing. Old-session calls and receipts (`confirmed`) end with `NoSession`, or `Closed` after `close`; streams end with `SessionEnded` | The credential, the stored session state and the data of the account are cleared. The server is not contacted |
| The `logout` session operation, answer `revoked` | `sessionCall("logout", ...)` returns the answer. The session ends as after `sessionReset()`                                                               | The stored state is cleared first, the credential second                                                      |
| Any other call, answer `revoked`                 | The session ends and the call fails with `NoSession`                                                                                                      | Cleared as above                                                                                              |
| HTTP 401 on a call                               | The call fails with `Unauthorized`. HTTP 403 fails it with `Http(403)`. The session stays                                                                 | Kept                                                                                                          |

`Unauthorized` means the server refused this request. `Revoked` means the server ended the session. After a revoked answer the app signs in again with a new `AccountScope`.
Call `sessionReset()` for every sign-out of the app. The library clears the data itself.
If `CredentialStore.clear` fails during a revoked logout, the logout still succeeds and the next open clears the credential that has no stored state.

### Reading the session

`sessionStatus()` returns the session now without network or storage access: `epoch`, `identity` and `phase`. `phase` is `none` without a session, `active` for a signed-in one (also while it resets or its storage failed) and `ending` while it ends. `identity` is the digest of the credential and is set for `active` and `ending`. Equal identities in two readings mean the same session. It is not the server session id. The call throws `NotOpen` before `open` and `Closed` after `close`.

The app keeps no sign-in flag of its own. Read the state after a stream ends with `SessionEnded` and after `sessionCall` or `sessionReset`.

```swift
let state = try DB.sessionStatus()
if state.phase == .active { showMain() } else { showSignIn() }
```

```kotlin
val state = DBlayer.sessionStatus()
if (state.phase == SessionStanding.ACTIVE) showMain() else showSignIn()
```

### Session errors in loads and mutations

A sealed answer of the encrypted session that carries an `error` field with a session code throws `DbError.Session(code:detail:)` (`DbException.Session` with `code` and `detail` in Kotlin). It is the same case that `sessionCall` throws, with the same `SessionCode`. No `Graphql` error and no text is parsed. `detail` holds the code text of the server.

| Where                                                                                         | Result                                                                                                                                                                      |
| --------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Query.load()`, `loadMore()`, `refresh()`, a forced reload (`readNode(force: true)`, `nodes`) | the request of the load got the sealed refusal                                                                                                                              |
| `DB.call(_:variables:)` and generated `Operations.*` calls (`DBlayer.call` in Kotlin)         | the call got the sealed refusal                                                                                                                                             |
| Mutation receipt `confirmed()`                                                                | the queued mutation got the sealed refusal. `Rejected` carries the session code and detail. The action's `onFailure` declaration decides whether the optimistic value stays |
| streams (`scope.stream`)                                                                      | never. They read the local database. A load under them that gets the refusal fails quietly, the explicit `load()` shows it                                                  |

`retry(action)` does not throw for the refusal of one operation. A refusal that confirms the sequence spends the request and removes it. `sequenceConflict` does not confirm it and keeps the request pending. `unauthenticated` removes the request and keeps the session. `sessionEnded` ends the session. The credential stays for every other code. Sign-in state changes only through `sessionReset()`, `sessionEnded` and a `revoked` answer. Unknown refusal codes stay `Graphql` errors. Temporary codes follow the automatic retry rules below, including codes inside `errors[].extensions.code`.

The errors `UNAUTHENTICATED` and `ACCOUNT_UNAVAILABLE` inside `errors[].extensions.code` reach the rejected outcome with the same code. The action's declaration decides whether to keep its optimistic value.

### Recover a queued mutation

Recovery depends on the response and whether it confirms the sequence:

| The refusal                                                                      | The request                                                                          | Do                                                                             |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| confirmed the sequence (`accountUnavailable`, `rateLimited`, every sign-in code) | spent, nothing is pending                                                            | fix the cause, then `retry`. The core sends the mutation as a new request      |
| did not confirm it, and the code is `sequenceConflict`                           | pending, and it blocks every other mutation, sign-in too                             | `sessionReset()`, sign in again, then `retry`                                  |
| `unauthenticated` or HTTP 401                                                    | removed, nothing is pending, the session stays                                       | the call throws `Unauthorized`. Fix the cause, then `retry`                    |
| `sessionEnded`                                                                   | removed, the session ends and the account data is erased                             | sign in again                                                                  |
| HTTP 3xx or 4xx other than 401, 408, 425 and 429, without a sealed answer        | removed without advancing the sequence, the owner becomes `Failed` with `Http(code)` | Fix the cause before sending again                                             |
| lost connection, timeout, HTTP 408, 429 or 5xx                                   | pending                                                                              | the durable owner repeats the saved request under the [retry policy](#retries) |
| stored request whose path is now declared sessionless                            | removed without sending or advancing the sequence                                    | nothing to do                                                                  |

The permanent statuses apply to the first send and every repeat through the same settlement code.
The first send still throws its HTTP error. Opening the database again sends nothing after removal.
The [retry owner](#retries) preserves Pending across temporary failures.
HTTP 401 returns `Unauthorized` at once and removes the pending request. The session, data and credential stay.
HTTP 401 and the `UNAUTHENTICATED` code reach the caller as `Unauthorized`: the session, its data and the credential stay. HTTP 403 stays `Http(403)`.
Opening the database with a session sends the stored request. The application calls nothing.

The library closes the queued operation that owns the pending request with the answer, like a normal send. The server runs the mutation once, and `retry` after it finds nothing to send. After `sequenceConflict` every call, sign-in too, fails with `sequenceConflict` until `sessionReset()`, because the stored request is still pending.

For mutations with `clientMutationId` in the SDL input type, the generator sets `client_mutation_id` in the action spec.
The model needs no field declaration. Without SDL, automatic key passing is off.
SQLite gives each new operation a random 128-bit key and stores it in the column `op.client_mutation_id`.
The key survives restart, `retry` and a retry of the final upload mutation.
A new encrypted request gets the old key of the operation. A new operation gets a new key.
An app value in `input.clientMutationId` takes priority, including an explicit `null`.
Upload steps keep their saved request. A multipart abort uses its `uploadId` for request identity without the former transfer key.

A refusal with `on_failure: :keep`, for example the server codes `IN_PROGRESS` and `DUPLICATE_CLIENT_KEY`, leaves the row in `failed`. A manual `retry` repeats the same key.
The library does not declare success without a server row. It does not try to find the row through an incomplete list.
The app sets the keys `messageAlbumSend.items[].clientKey`. The library does not change them.

Uploads own their saved requests, including blob creation, multipart signing, completion and the final mutation.
After reopening the database, the library restores upload steps and unfinished multipart aborts. An abort remains owned by its upload ID after the transfer is removed.
The library passes an upload step's answer to that step and continues its remaining work.
The saved request identifies the exact part numbers when an upload URL needs renewal.
Upload failures retain their typed `SessionCode` in Swift and Kotlin.

A batch upload stores one `op` for the final mutation and one `sync_upload` row per file, including its source, progress, and saved steps.
The final mutation carries all remaining items in their original order and uses the operation's existing delivery key.
An uncertain final delivery keeps every member until explicit retry or discard.
A direct `call` mutation has no queued owner and creates no operation.

```swift
do {
  try await Moment.feed.load()
} catch DbError.Session(let code, _) where code == .unauthenticated {
  try await DB.sessionReset()
  try await DB.sessionCall("social", body: signIn)
}
```

```kotlin
try {
    Moment.feed.load()
} catch (error: DbException.Session) {
    if (error.code == SessionCode.UNAUTHENTICATED) {
        DBlayer.sessionReset()
        DBlayer.sessionCall<SignInBody, JsonElement>("social", signIn)
    }
}
```

A platform transport (`MockTransport` and your own `Transport`) throws `DbError.Session` to the same effect.

```swift
do {
  try await Moment.feed.load()
} catch DbError.Session(let code, _) where code == .accountUnavailable {
  showBlocked()
} catch DbError.Session(let code, _) where code == .unauthenticated {
  signOut()
} catch DbError.Offline {
  showOffline()
}
```

```kotlin
try {
    Moment.feed.load()
} catch (error: DbException.Session) {
    when (error.code) {
        SessionCode.ACCOUNT_UNAVAILABLE -> showBlocked()
        SessionCode.UNAUTHENTICATED -> signOut()
        else -> show(error.detail)
    }
} catch (error: DbException.Offline) {
    showOffline()
}
```

Fetch the encrypted config before opening a database. `DB.serverConfig` and `DBlayer.serverConfig` call the same Rust function without credentials or SQLite.
The first bootstrap call or opening starts one process network monitor. Offline bootstrap waits without HTTP attempts.
Android requires Context first and retains applicationContext. The same monitor feeds the database after opening.

```swift
let encrypted = EncryptedConfig(origin: origin, serverKey: bundledServerKey, pin: bundledServerPin, endpoints: endpoints, protocolLabel: protocolLabel, cablePurpose: cablePurpose, cableChannel: cableChannel)
let config = try await DB.serverConfig(encrypted: encrypted, platform: .ios, version: appVersion, build: appBuild)
if config.updateRequired { showUpdate(config.storeUrl) }
```

```kotlin
val encrypted = EncryptedConfig(origin, bundledServerKey, bundledServerPin, endpoints, protocolLabel, cablePurpose, cableChannel)
val config = DBlayer.serverConfig(context, encrypted, AppPlatform.ANDROID, appVersion, appBuild)
if (config.updateRequired) showUpdate(config.storeUrl)
```

`AppPlatform` accepts iOS and Android. Version contains 2 or 3 numeric segments separated by dots. Optional build contains at most 32 characters.
The core refuses invalid input and a mismatched pin before HTTP. The optional `http` argument defaults to the platform client on the origin.

The request uses `POST` at `encrypted.endpoints.bootstrap`, with `Content-Type: application/jose`, no query, cookies, credentials or old encryption headers.
Redirects are refused. HTTP implementations do not reuse added application headers, cookie storage or authenticators.
The core sets empty Accept and Accept-Language values to suppress system defaults, and both platforms send the headers as given.
Bootstrap sends exactly one User-Agent with the library token and no application products.
Each attempt creates a UUID, a response key and an issuedAt timestamp from the core clock. No sequence or pending request is stored.

The request JWE uses RSA-OAEP and A256GCM. Its plaintext contains audience, kid, method, path, requestId, issuedAt, responseKey and body.
The body contains platform, version and optional build.
The response JWE uses dir and A256GCM under responseKey. Its plaintext contains requestId, requestDigest, sequence=null and body.
requestDigest is lowercase SHA-256 of the request JSON without issuedAt, with top-level keys sorted.
Body retains the order platform, version, build. See `fixtures/bootstrap-config/bootstrap_kat.json`.
The client checks the response key, algorithms, requestId and requestDigest before reading config fields.

| Field                              | Meaning                                                                           |
| ---------------------------------- | --------------------------------------------------------------------------------- |
| `configVersion`                    | required structure version, currently 1                                           |
| `protocolVersion`, `minAppVersion` | protocol and minimum application versions                                         |
| `updateRequired`                   | a successful result requesting an app update                                      |
| `country`, `countryDisabled`       | nullable country code and phone sign-in restriction                               |
| `countries`                        | name, code, flag, phoneCode, phoneMask, phoneFormatter, enabled, phoneAuthEnabled |
| `trackingPromptEnabled`            | whether the app shows the tracking prompt                                         |
| `storeUrl`                         | nullable store link for the requested platform                                    |
| `flagsJson`                        | flags object encoded as JSON, including future values                             |

Country phoneFormatter maps strings to strings. Required booleans and lists never default from missing or null values.
Unknown fields are accepted. Unsupported configVersion and wrong field types have distinct typed config errors.

| Result                                            | Swift / Kotlin outcome                                  |
| ------------------------------------------------- | ------------------------------------------------------- |
| Offline or timeout                                | waits and retries until success or cancellation         |
| Task or coroutine cancellation                    | Swift CancellationError or Kotlin CancellationException |
| HTTP 408, 429 or 5xx                              | waits and retries until success or cancellation         |
| Other HTTP errors                                 | Http, without an automatic retry                        |
| Wrong bundled pin                                 | Security with ServerKeyMismatch, 0 HTTP                 |
| Invalid JWE, key, requestId or digest             | Security with ResponseAuthentication                    |
| TLS trust failure                                 | Security with TlsTrust                                  |
| Sealed INVALID_REQUEST or invalid local arguments | Config with InvalidRequest                              |
| Invalid config fields                             | Config with InvalidFormat                               |
| Unknown configVersion                             | Config with UnsupportedVersion                          |

The classifiers preserve typed errors. Config failures do not clear credentials, reset sessions or enable entry from cached data.

## Retries

Each request has one retry owner. Core `RetryPolicy` defines the common delay calculation and the budgets below.

| Request                                                              | Retry owner                   | Budget                                                                            |
| -------------------------------------------------------------------- | ----------------------------- | --------------------------------------------------------------------------------- |
| Encrypted read, bootstrap, standalone mutation, session call, logout | Encrypted HTTP                | Unlimited until success, terminal refusal or cancellation                         |
| Plain load                                                           | Query executor                | 3 sends for network failures, 2 for temporary server failures, the first included |
| Queued mutation, including encrypted and final upload mutations      | Durable outbox                | 40 undelivered attempts, then Failed with the edit and Pending owner preserved    |
| Upload transfer step or multipart part                               | Durable upload step           | 50 retries per step or part                                                       |
| Logical subscription and shared socket reconnect                     | Each connection independently | Unlimited while needed                                                            |

Encrypted HTTP retries network errors, timeouts, HTTP 408, 429 and 5xx.
The temporary GraphQL codes are `TIMEOUT`, `SERVICE_UNAVAILABLE`, `INTERNAL_SERVER_ERROR` and `DATABASE_ERROR`.
A queued mutation repeats these codes only when the answer does not confirm the sequence. An answer that confirms the sequence ends the mutation by its `on_failure`.
An attempt counts each send of the request: a network loss before the send spends no attempt.
Opening after a schema change preserves the attempt budget and the final Failed result of queued operations.
An owned encrypted mutation returns each temporary failure to its durable owner without starting another HTTP retry loop.
An unconfirmed encrypted HTTP failure retains Pending, allowing the owner to repeat the same request safely.
Plain mutations without a confirmed answer are sent again with the same request id after interrupted transfers, unreadable responses and HTTP 5xx.
The delay ceilings are 1, 2, 4, 8, 16 and 30 seconds, then 30 seconds for subsequent attempts.
Each request gets an independent random seed. Jitter subtracts up to 20% from each delay.
Upload steps cap delays at 15 seconds with 50% jitter and preserve their attempt count and deadline across restarts.
An explicit outbox backoff keeps its configured seed and delays.
Clock drives retry waits, upload deadlines, subscription debounce and socket deadlines.
Offline requests wait for online without making HTTP attempts or running retry timers.
`refresh()`, `loadMore()` and `readNode(force: true)` return `Offline` at once without the network. Before the first network value of the system they wait for it. Other loads wait for the network. When connectivity disappears during an in-flight request or retry wait, the request is cancelled and the load waits for the network and starts again when it returns.
Callers of the same load share one request. Cancelling the caller that runs it does not fail the others: the first waiting caller sends the request again and the rest keep waiting for it.
Bootstrap receives Connectivity before a database exists and waits for online before its first HTTP attempt.
No timer runs without a failed request awaiting another attempt.
Applications await the original call and use its cancellation instead of adding their own startup retry loop.
Retries cover HTTP exchange failures. Local Pending conflicts, credential storage failures and SQLite failures still return immediately without retrying.

Security, Config, unsupported versions, invalid origins, RequestTooLarge, ResponseTooLarge and Session refusals return immediately.
HTTP 400, 404, 405, 413, 415 and 422 remain permanent refusals.
Bare HTTP 401 and 403 keep their existing immediate errors.
Malformed or unauthenticated encrypted responses also return immediately.

Bootstrap seals a new requestId and response key on every attempt.
Numbered requests keep the same Pending requestId, response key, sequence, path, owner and body after an unconfirmed failure.
Each attempt refreshes issuedAt after waiting for online and seals the request again.
The mutation lock stays held across the current encrypted call.
A confirmed temporary refusal atomically commits the confirmed sequence and stores the next Pending with a new requestId and response key.
The next attempt uses that new sequence and the same operation body.
Cancellation preserves Pending and never advances an unconfirmed sequence.
Dropping the Rust future stops the request and its backoff.
Swift Task and Kotlin coroutine cancellation also stop bootstrap and `sessionCall`.

Create one `RetryProgress()` before the database exists. Pass it as `retries` to `DB.serverConfig` in Swift and `DBlayer.serverConfig` in Kotlin.
It is optional. All `RetryProgress` instances read the same process-wide source.
`RetryProgress.changes` in Swift (an `AsyncSequence` that you iterate with `for await`) and `RetryProgress.flow` in Kotlin deliver a `RetrySnapshot` with a `waiting` list.
Each `RetryEntry` contains `owner`, retry policy `kind`, schedule `id`, `attempt` and `delayMs` in milliseconds.
A reader gets the current snapshot first, then the newest one. A slow reader sees the newest snapshot and delays no request.
Cancelling a read ends only that reader. The calls and the other readers go on.
An entry appears when a retry is scheduled and leaves when its timer fires or is cancelled.
`waiting` is empty when no retry is waiting, even when requests are in flight.
There is no timer. The snapshot only follows the retry loops that `RetryPolicy` already runs.
`attempt` starts at 1 for each call. `delayMs` is the actual jittered delay until the next attempt.
The entry contains no request payload. Owners include bootstrap, session, window, calls, outbox, transfer and feed.
Durable outbox and upload owners also expose their saved attempts through operation state.

The list shows the timers that wait now. It is not a list of all calls.
`RetryProgress` is not a connection state: for the network of the phone read [`NetworkStatus`](sync-state.md#network-status).

```swift
let retries = RetryProgress()
for await snapshot in retries.changes {
  for entry in snapshot.waiting {
    print(entry.owner, entry.kind, entry.attempt, entry.delayMs)
  }
}
```

```kotlin
val retries = RetryProgress()
retries.flow.collect { snapshot ->
    snapshot.waiting.forEach { entry -> println("${entry.owner} ${entry.kind} ${entry.attempt} ${entry.delayMs}") }
}
```

The core puts the timeouts in every `HttpRequest`: connect 10 seconds, request 30 seconds. Kotlin applies them to the call. Swift applies the request timeout to the request, including when given a custom session.
URLSession does not expose an independent connect timeout, so Swift has no separate 10-second connect setting.

Origin rule: `https://host[:port]` is accepted for any host. `http://host[:port]` is accepted only for development hosts: the name `localhost`, a name that ends with the label `.local`, and the IP literals of loopback (`127.0.0.0/8`, `::1`), private networks (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `fc00::/7`) and link-local (`169.254.0.0/16`, `fe80::/10`). The core parses the host strictly: decimal, hex and octal address forms, trailing dots, user info, paths, queries and fragments are refused. The request audience stays the origin you gave. The JWE protects the payload over http, but the transport is not protected, so use http for development only. Production origins use https.

`DB.open` (Swift) and `DBlayer.open` (Kotlin) open the one database of the process. The calls of the library use it, and the raw bridge object is not public.
The core takes the transport and the encrypted HTTP from the executor of the platform, `UrlSessionTransport` or `OkHttpTransport`, on the same origin, including the WebSocket at `encrypted.endpoints.cable`. A database with neither `endpoint` nor `encrypted` is local and has no transport. The optional `transport` and `http` parameters replace the executor in tests.

```swift
try await DB.open(encrypted: EncryptedConfig(origin: origin, serverKey: bundledServerKey, pin: bundledServerPin, endpoints: endpoints, protocolLabel: protocolLabel, cablePurpose: cablePurpose, cableChannel: cableChannel), credentials: AppCredentials(), modelsJSON: modelsJSON, uploadTaskTitle: "Uploading")
```

```kotlin
DBlayer.open(context, encrypted = EncryptedConfig(origin, bundledServerKey, bundledServerPin, endpoints, protocolLabel, cablePurpose, cableChannel), credentials = AppCredentials(context), modelsJson = modelsJson, uploadNotification = notification)
```

Tests can pass their own transport and HTTP replies. A test that opens cable must also implement the 4 WebSocket calls.

```swift
try await DB.open(encrypted: encrypted, credentials: credentials, transport: testTransport, modelsJSON: modelsJSON, http: testHttp, uploadTaskTitle: "Uploading")
```

```kotlin
DBlayer.open(context, encrypted = encrypted, credentials = credentials, transport = testTransport, modelsJson = modelsJson, http = testHttp, uploadNotification = notification)
```

Rules of the session:

- Every mutation, including a credentialed sign-in operation, carries the next sequence number. A query carries none and gets a new request id each time.
- The core stores a mutation (request id, response key, sequence, path and body) before it sends it. If the answer is lost, the same call or the next open with a session sends the identical request again, including after restart. The server answers from its record. Only the time stamp changes.
- While a request is pending, a different mutation fails with a network error and sends nothing. The library sends the pending request when the database opens with a session. When a queued operation owns the request, the answer also closes that operation.
- A refused step such as `invalidCode` confirms the sequence. `sequenceConflict` does not: the pending request stays and every later call fails with `sequenceConflict` until `sessionReset()`. `unauthenticated` removes the request and keeps the session: the call throws `Unauthorized` and the next call goes out. `sessionEnded` ends the session and erases the account data. The core never clears the session for `sequenceConflict` or `unauthenticated`.
- Database format changes and reopening preserve the credential digest, confirmed sequence and complete pending request in SQLite.
- Changes to the session tables migrate their stored values within the same transaction that recreates the database tables.
- Missing sequence state for a stored credential returns the typed session error above before sending. Its detail remains `lost session sequence state for the stored credential`.
- This error preserves the credential and pending request. The application decides how to recover. The core never guesses sequence numbers.
- Only an absent credential creates a new credential. `sessionReset()` and a `revoked` answer clear the credential, session row and pending request.
- HTTP 503 and a lost connection wait for automatic retry. HTTP 400 returns immediately.

The JWE size is checked before the pending row is saved and before the HTTP send.
If the size exceeds 256 KiB, Swift gets `DbError.RequestTooLarge(size:limit:)` and Kotlin gets `DbException.RequestTooLarge` with the fields `size` and `limit`.
This local refusal does not change the sequence of the existing session and creates no pending row.
The next short request is sent without `sessionReset()`.
The check applies to reads, `sessionCall`, the `logout` call and every request with a sequence.
A local refusal of the full document does not use the next sequence.
A real HTTP 413 returns `Http` and removes Pending without spending its sequence.

### Upload failures

A transfer step retries network errors and the temporary GraphQL codes listed above under the upload budget. The final content mutation follows the [write rules](files.md#transfer-rules): a confirmed refusal keeps `Failed` without an automatic retry.

### Persisted queries

The core sends a GraphQL read as its SHA-256 hash first, without the document. This keeps the body small: the body loses the whole `query` field.

| Server answer                                                      | Core                                                                         |
| ------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| result                                                             | returns it                                                                   |
| `PERSISTED_QUERY_NOT_FOUND` (HTTP 200, `errors[].extensions.code`) | sends the call once more with the document and the hash, returns that answer |
| `INVALID_INPUT` or any other error                                 | returns it, no second send                                                   |

The second answer is final: a second `PERSISTED_QUERY_NOT_FOUND` goes to your app and the core sends no third request. Swift and Kotlin code does not change: the generated operations behave as before and return the answer of the last send.

- A query is sent again as a new request: new request id, new response key. The server reports the miss before it records the request id, so nothing is spent.
- A request with a sequence (mutation, call, the final mutation of an upload) goes first with the hash alone. If the server does not know the document, the core sends the full document under the same number and a new request id. Each pair keeps its bytes after a retry, a release and a restart.
- An answer without `sequence` (for example `PERSISTED_QUERY_NOT_FOUND` or `INVALID_INPUT`, which the server answers before it checks the sequence) did not spend the number: the core removes the pending request, returns the answer and sends the next call under the same number.

The server registry fills itself. The first read of a document after the app is installed (or after the server lost the registry) costs 2 requests: the miss and the send with the document, which the server stores. Every later read of that document, from any device, is one hash-only request. If the server registers the documents in advance through its deployment tooling, there are no misses and every read is one hash-only request.

## Responses become rows

Every response, whether from a query, a mutation or a subscription event, goes through the same rules:

| Rule              | Behavior                                                                                                                                                                                                                                      |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Identity          | the objects of a relation are rows of its model and need an `id`. A relation parameter can supply a missing id. Other objects without one are dropped                                                                                         |
| Nesting           | objects found at the `from:` path of an association (its name by default) become rows of the target model. `belongs_to` keeps the key (`userId`) on the outer row, `has_many` and `has_one` rows get the outer id, `references` keeps the ids |
| Lists and pages   | lists and `edges { node }` with `pageInfo` are recognized from the document and keep server order                                                                                                                                             |
| Partial responses | fields a query did not select are never erased, each field is updated on its own and the newer revision wins                                                                                                                                  |
| Unknown fields    | ignored                                                                                                                                                                                                                                       |
| One commit        | a whole response lands in one commit                                                                                                                                                                                                          |

## Loading

```swift
Moment.feed                      // live; loads if stale, nothing to call
try await Moment.feed.loadMore() // next page
try await Moment.feed.refresh()  // pull to refresh, replaces in one commit
```

```kotlin
Moment.feed                      // live; loads if stale, nothing to call
Moment.feed.loadMore()           // next page
Moment.feed.refresh()            // pull to refresh, replaces in one commit
```

- Fresh data is not requested again. `freshness expires_in:` sets how long data counts as fresh.
- Identical parallel loads become one request.
- Loads and mutations use the [retry policy](#retries). Terminal refusals return immediately.
- A response older than the data already stored never overwrites it.
- A row that another response stored while this one was in flight keeps its stored data and still joins the list this response names.
- `paginate max_pages:` limits how many pages `loadMore()` fetches from the network for one list. At the limit the list ends: `hasMore` is false and `loadMore()` answers no more pages. Only loading all pages fails. The count is stored with the cursor and starts again at 1 when the first page is fetched again.

## Refresh merges, it does not replace

`load()` reads the list only when it is stale and keeps no subscription, see [live data](live-data.md). `refresh()` works on one list with its parameters. Only that list changes: other lists keep their content, and nothing is deleted just to be written again.

```swift
try await Moment.feed.refresh()
try await Moment.byUser(userId: 5).refresh()      // only user 5's list
```

```kotlin
Moment.feed.refresh()
Moment.byUser(userId = 5).refresh()               // only user 5's list
```

What a refresh does, in one commit:

- Rows from the response are merged field by field. Rows that did not change keep their revision and are not reported as updated.
- Records that the server has not confirmed yet (a message being sent, a failed one waiting for retry) stay in the list where they are.
- For a paged list the response is the first page. The last row of the response is looked up in the old list. If it is found, the new first page is followed by the old rows that came after it, so a long list that the user scrolled does not jump. If it is not found, the old tail is dropped and `loadMore()` loads the next pages again.
- Rows that are missing from the response leave the list but stay in the database, because they may belong to other lists. A row is deleted when the server says so (`delete_if:`, a null or `NOT_FOUND` node answer, a delete event).
- A failed request changes nothing.
- Every read of one list, node or set of ids shares the request in flight. A request goes to the network at least 1 second after the previous answer of the same read landed: `refresh()` and forced reads inside that second get the previous answer, event rereads and the reread after a gap or a return of the network wait for the end of the second and go once. A failed request leaves no wait.

An event can carry identifiers or full model rows. Without `payload`, `changed_by` always forces its declared HTTP reread, regardless of freshness.
`changed_by "MESSAGE", via: :node, payload: "chat"` applies the full Chat at the event item's `chat` path.
Declare `payload: "message"` on Message too for zero node requests when both rows are present.
All payload targets commit before any declared HTTP target, including targets derived through `id_of`.
The core indexes targets by area when the models are registered, without scanning unrelated models for each event.
Missing or incomplete payloads are rejected as application errors. They never trigger an HTTP fallback.
An active model event reader receives a `lost` position when its payload cannot be applied.
`removes_on` handles terminal kinds before reading their payload, so a deleted Message can carry null while its Chat still updates.
Declare `server_revision :serverRevision` with a required integer attribute on a versioned model.
The core drops a server row whose revision is lower than its stored revision. Equal or higher revisions apply.
The same comparison covers payloads, node responses and pages. The integer remains exact through SQLite, Swift and Kotlin.
Payloads from a subscription's previous database generation are discarded after reset.
Remove manual Chat rereads and list refetches after MESSAGE when these payload declarations supply both rows.
Events of the plain transport with a full row save that row directly. `insert_on: :server` adds such a row to loaded selections that match its `where` columns, and does the same for rows from operation responses (`upsert`), including cached rows absent from matching loaded windows. Existing members keep their places.
A scope parameter without `where` needs explicit `membership` to accept event rows.
`matches mediaKind: { in: [...] }`, `matches kind: { equals: "GIFT" }` and `matches sequenceNumber: { greater_than: :afterSequence }` select loaded windows by row fields. A row from an event, an operation response or a node reread joins every loaded window whose `where` columns and `matches` tests it satisfies. A row with a null tested field joins none. A member that stops matching leaves the window.
Constant `variables` keep a scope out of automatic insertion unless a `matches` rule describes the answer.
`membership list: { contains: :lists }` selects windows whose list parameter occurs in the row array.
`insert_on: :server` also admits rows from `changed_by` node rereads when the model has a `node` scope that `changed_by` names with `via:`.
Missing members join loaded windows at the head. Existing members keep their places. Manual node reads do not insert members.
After a reconnect, Cable rereads the observed selections of models with `changed_by`.
`via: :observed` uses this reread for each event. Each event also reaches `scope.events(Model)` with the row before and after the reread, see [Models](models.md).

## Mutations

```ruby
operation :create do
  mutation "CreateMoment"
end
```

Fields of the kind `id` that hold a whole number are sent as JSON numbers. A GraphQL `ID` accepts a number and so does an `Int`, while a string only fits an `ID`.

Servers that take one input object and answer with a payload use `response from:`. The generator reads the operation document: when it has one variable, an input object that the root field takes, and no parameter has the name of that variable, the body goes under that variable. A parameter of the same name takes the whole input object instead (`parameter :input, StickerInput`), and the call keeps omitted, null and set nested fields:

```ruby
operation :update do
  mutation "UpdateGroup"
  response from: "groupUpdate.group"
end
operation :send_reward do
  mutation "RewardSend"
  parameter :rewardId, :string
  optimistic false
  upsert Note, from: "rewardSend.message"
  variables userId: :id
  response from: "rewardSend.user"
  refetch :current, model: CurrentUser
end
```

`response :ignore` skips reading the response, for a mutation whose answer is not the model's row; add `refetch :profile` to reload the scope instead.

The operation document with one input variable sends `{ input: { ...fields } }`. `response from:` is the path of the row inside the response, by default the first field of the operation. `upsert Model, from: "a.b"` writes an object or every object in an array at that path into the declared model. The model must exist, and a path that the response lacks writes nothing. `refetch :scope, model: Model` marks a scope of another model stale after the operation succeeds and reloads it when someone reads it, for counters that live on another row. `refetch` is a set: write the line again for each scope.

A named model operation without an `optimistic` line writes each parameter named like an attribute at once, `optimistic false` opts out, and a bare `returns` gives the call the answer type of the document (see [writing data](writing-data.md#what-the-server-answered)). `create`, `update` and `destroy` call the named mutation. Each field of the input takes the parameter, then the attribute (`create`, `update`), then the row `id` (the name `id` or the name of the model followed by `Id`) with the same name. A field that is not null, has no default and finds no source is an error, and a nullable one is not sent. `variables a: :b` only renames: the field `a` takes the source `b`. A `parameter` line in a model operation declares a value that is sent but not stored (see [writing data](writing-data.md)). Its types are the types of an operation without a model: a scalar type or the name of a schema enum or input object (see [models](models.md#check-against-the-server-schema)). The response is normalized like any other response and replaces the optimistic row. See [writing data](writing-data.md) for the queue, retries and rollback.

## Operations without a model

Sign-in, sign-out, a QR code or a price list belong to no row. Declare each one with an `operation` block. The generator writes a typed call for Swift and Kotlin, and the call sends the operation and reads the answer type of its document:

```ruby
operation :qr_code_create do
  mutation "QrCodeCreate"
  parameter :kind, :string
  parameter :size, :int, null: true
end
```

```swift
let code = try await Operations.qrCodeCreate(kind: "group", size: 3)
print(code.url)
```

```kotlin
val code = Operations.qrCodeCreate(kind = "group", size = 3)
println(code.url)
```

- `mutation "Name"` or `query "Name"` names the operation. Pass the operation type to `DB.open` (Swift) or `DBlayer.open` (Kotlin) like any other operation.
- `parameter` lines are the variables, with the same types as the parameters of `create`. `null: true` makes one optional and an omitted one is not sent. An operation document with one input variable sends them as `{ input: { ... } }`, and the parameters are the fields of that input.
- The answer type is the selection of the document under its root field, typed by the schema. A field is the selected field (an alias names it). Its type follows the schema type: a scalar (`Int`, `Float`, `Boolean`, `String`, `ID` or a custom scalar with a `scalar` line), a schema enum with its values, `JSON` for a value you read yourself (`JSONValue` in Swift, `JsonElement` in Kotlin), a list as the schema declares it, an object as a type named by the owner and the field (`PricingLoadResultVideoIntro`). Selected `... on T` fragments on a union or an interface make a union type, and the selection must include `__typename`. Every field is nullable. An `ID` field accepts a number or a string and arrives as a string. A document that selects nothing under its root field (`signOut`) returns nothing. An operation without a model takes no `returns` line, and `returns do ... end` is an error.
- `upsert Model, from: "path"` lands the rows of the answer under the path in one batch before the call returns, with the parsing of `upsert` in a model operation. The call then gives the typed answer, and a refused landing fails the call. An unknown model or a path outside the selection stops the generator.
- `parameter :owner, OwnerInput` names a schema input object. The generator reads it and nested inputs from `--schema`.
- Required inputs remain required. Nullable fields use `GraphQLNullable`: omitted by default, explicit null, or a supplied value. Lists preserve every dimension and element nullability.
- A required input field with a schema default can be omitted. Nullable fields with defaults still distinguish omission from explicit null.
- `parameter :kind, WidgetKind` names a schema enum, and an answer field of that schema type is the same enum. It becomes a typed enum with an `unknown` case, in inputs too (see [models](models.md#enums)). A custom scalar needs a `scalar` line (see [models](models.md#custom-scalars)). The `JSON` scalar uses `JSONValue` in Swift and `JsonElement` in Kotlin.
- The call does not enter the outbox and returns Offline when started offline. Before the first network value of the system the call waits for it and fails with a network error after 30 seconds.
- Encrypted calls follow the [retry policy](#retries). A numbered mutation stores Pending before HTTP, including for a standalone operation.
- Cancelling a standalone encrypted mutation preserves Pending and leaves its result unconfirmed.
- Plain standalone calls return transport failures and GraphQL errors directly.
- A failed call or load throws a typed error. `Http` carries the status and the body (503), `Graphql` carries every message of an answer with errors in order, and `Session` carries the session code of a refusal of the encrypted session (see [session errors in loads and mutations](#session-errors-in-loads-and-mutations)). `Network` reports a network failure. `Interrupted` reports a cancelled wait with its cause. `Offline` is a call that needs the network while it is away, `Dropped` a request that left and got no answer, `Unauthorized` a refusal of the credential by the encrypted session, `InFlight` a change refused because the operation is in flight. `Storage` is a failure of the local database, `Internal` a broken invariant of the library, `Copy` a failure to copy the source of a transfer. A thread or executor that cannot start has its own variant.

```swift
do {
  _ = try await Operations.pricingLoad()
} catch DbError.Unauthorized {
  signOut()
} catch let DbError.Graphql(messages, _) {
  show(messages)
}
```

```kotlin
try {
    Operations.pricingLoad()
} catch (error: DbException.Unauthorized) {
    signOut()
} catch (error: DbException.Graphql) {
    show(error.messages)
}
```

- Do not use it for a row of a model. The row is written by `create`, `update` or a model `operation`, which keep the queue, the retries and the rollback.

An operation that reads lists, objects and JSON from the answer:

```ruby
operation :pricing_load do
  query "Pricing"
end
```

```swift
let pricing = try await Operations.pricingLoad()
let names = pricing.rewards?.compactMap(\.name) ?? []
```

```kotlin
val pricing = Operations.pricingLoad()
val names = pricing.rewards?.mapNotNull { it.name }.orEmpty()
```

An operation whose root field is a list, such as a search that returns the matching items, declares `response shape: :list`. The selection then describes one element and the call returns a list of it:

```ruby
operation :giphy_load do
  query "Giphy"
  response shape: :list
  parameter :type, :string
  parameter :limit, :int, null: true
end
```

```swift
let found = try await Operations.giphyLoad(type: "gif", limit: 20)
let urls = found.compactMap(\.url)
```

```kotlin
val found = Operations.giphyLoad(type = "gif", limit = 20)
val urls = found.mapNotNull { it.url }
```

`response shape: :list` needs a document that selects the fields of one element.

An aggregate that is not a row of a model, such as the counters of a group library, is an operation without a model. The call returns the fields as a value and writes no row. When the answer nests the aggregate under `node(id:)`, the type of the nested selection is part of the answer type:

```ruby
operation :group_media_state do
  query "groupMediaState"
  parameter :groupId, :id
  parameter :upToSequence, :int, null: true
end
```

```swift
let state = try await Operations.groupMediaState(groupId: "c1", upToSequence: 120)
let visual = state.mediaLibrary?.visualCount
```

```kotlin
val state = Operations.groupMediaState(groupId = "c1", upToSequence = 120)
val visual = state.mediaLibrary?.visualCount
```

An operation that takes a schema input object:

```ruby
operation :sticker_send do
  mutation "StickerSend"
  parameter :input, StickerInput
end
```

```swift
let sent = try await Operations.stickerSend(input: StickerInput(name: "wave", tags: ["a"], pinned: true, owner: OwnerInput(id: "7")))
print(sent.id)
```

```kotlin
val sent = Operations.stickerSend(StickerInput(name = "wave", tags = listOf("a"), pinned = true, owner = OwnerInput(id = "7")))
println(sent.id)
```

A list of schema input objects or enums takes the name in brackets. `[WidgetInput]` is a list that holds no nil, `null_items: true` allows nil items, `null: true` makes the whole list optional and an omitted one is not sent. The check compares the list with the variable of the mutation: a parameter that may hold nil items does not fit `[WidgetInput!]`.

```ruby
operation :widgets_send do
  mutation "WidgetsSend"
  parameter :widgets, [StickerWidgetInput], null: true
end
```

```swift
let widget = StickerWidgetInput(kind: .emoji, props: .emoji(StickerEmojiInput(code: "fire")))
let answer = try await Operations.widgetsSend(widgets: [widget])
print(answer.id)
```

```kotlin
val widget = StickerWidgetInput(kind = WidgetKind.Emoji, props = StickerPropsInput.Emoji(StickerEmojiInput(code = "fire")))
val answer = Operations.widgetsSend(widgets = listOf(widget))
println(answer.id)
```

## Subscriptions

```ruby
scope :feed do
  query "MomentsFeed"
  subscribe "MomentEvents", apply: { effect: :upsert, delete_if: "destroyed" }
end
```

- `from: "a.b"` selects the row inside the event payload. Its default is the first field. `id:` selects the identifier and defaults to `id`.
- Events are normalized like responses. `id:` and `delete_if:` accept dotted paths. `delete_if: path` selects the value that marks a deleted row. `delete_if: { not: path }` deletes the row when its path holds a false value, so one event type can both set and clear a state.
- `id:` takes a list of paths for a row that a pair of values names. Events with the same pair are one row, and a later event updates it.

```ruby
model ChatActivity, storage: :memory, expires_in: 8.seconds do
  attribute :chatId, :id
  attribute :userId, :id
  attribute :kind, :string
  attribute :active, :bool
  scope :by_chat do
    parameter :chatId, :id
    where chatId: :chatId
    subscribe "ChatAction", id: ["chatId", "userId"], apply: { effect: :upsert, delete_if: { not: "active" } }
  end
end
```

- An event changes only the attributes it carries. Attributes it does not select keep their stored values. The read of the stored row and the write happen in one transaction, so a local edit made at the same moment is never overwritten by an older copy.
- A delete event removes the same rows as a local `destroy`: the row and its `dependent: :destroy` children, in one commit.
- Events preserve fields owned by unconfirmed local edits. Loaded windows with `insert_on: :server` accept matching rows.
- If the first connection fails, the library retries it with the same growing delay and the same offline rule as a reconnect. `subscribe` does not throw for a refused connection.
- The subscription starts when the first reader connects, is shared by all readers and closes after the last reader leaves.
- After a dropped connection, the library reconnects and rereads once the watched relations of the models that lost events, fresh or not. Offline, it waits without attempts. Returning online reconnects at once. The reconnect delay runs on the database clock: a test clock that does not move never reconnects an online subscription, so a test advances it. A scope declared with `keep_on_gap: true` is not reread after the break, see [models](models.md#scopes).
- A WebSocket close frame ends the current subscription even without a GraphQL `complete` message.
- Bursts of events are merged into one commit.
- The debounce of a model is the longest `debounce:` of its subscriptions: events of the model wait for it and land in one commit. Each declaration keeps its own row effects, regardless of which reader opened first.
- Declarations without readers receive no row effects, including while another declaration keeps the shared connection open.
- Shared commits merge partial fields of the same nested row before writing it.
- A batch that fails to land marks its events `lost` for `scope.events(Model)` readers and makes the observed reads of its models stale. It reports no `gap`.
- Change notifications and declared ephemeral or reread effects on the same operation all run.
- Model event readers receive before and after rows for ephemeral and reread effects too.

### Subscriptions with arguments

A scope with parameters sends the values of the read relation as the variables of its subscription. They are the same variables the load request sends for that read, without the page arguments:

```ruby
model Document do
  attribute :slug, :string
  scope :bySlug do
    parameter :slug, :string
    parameter :source, DocumentSource, default: "WEB"
    where slug: :slug
    query "documentBySlug"
    response from: "documentBySlug", shape: :list
    subscribe "documentUpdated"
  end
end
```

```swift
let query = Document.bySlug("intro", source: .web)   // a screen that reads it opens documentUpdated(slug: "intro", source: WEB)
```

```kotlin
val query = Document.bySlug("intro", DocumentSource.Web)   // a screen that reads it opens documentUpdated(slug: "intro", source: WEB)
```

- Each set of values has its own subscription. Readers with the same canonical values (the key of the scope window) share one. A reader with other values opens another.
- The subscription closes after the last reader of its set leaves. The other sets stay open.
- The application only reads the relation. It never opens or closes the subscription.
- With `--schema`, the generator compares the arguments of the subscription document with the scope parameters by name and type. A required argument without a scope parameter, or an argument of another type, stops the generator with the file and line of `subscribe`. A scope parameter without an argument is allowed. Without `--schema` the arguments are not checked.
- `subscribe` on a model takes no arguments.

### Model-level subscriptions

A `subscribe` inside a scope lives while a screen reads the scope. For events that should reach the database for the whole session (presence, counters), declare `subscribe` on the model. It takes the same options:

```ruby
model User do
  subscribe "userUpdated", debounce: 300.milliseconds
end
```

A model-level subscription opens when the first stream with live data of the model starts (a `scope.events(Model)` reader, a list, a count or an entity of the model) and closes in the same step when the last of them ends. A signed-in session alone opens no subscription. Preloads and reloads after a sign-in are requests, not subscriptions, and run either way. The `scope.events(Model)` stream of a scope ends when its session ends: its last item is `ended`. Subscriptions that a refusal stopped restart with the next committed credential.

## Request log

`DB.requests()` in Swift and `DBlayer.requests()` in Kotlin give a stream of `RequestLogItem` (`RequestLogStream` in Swift) for debugging: `ready` when the stream starts, `request` with the `operation`, the variables as a JSON string and a `RequestReason` for each request that leaves for the network, `lost` with the `first` and `last` positions when more than 256 items waited, and `ended` with an `EndCause` when the session ends. A network failure after the request left still counts as sent, its retry is a new item. While no stream is open, nothing is recorded.

| Reason      | Request                                                                                           |
| ----------- | ------------------------------------------------------------------------------------------------- |
| `load`      | first load of a window, a node without freshness, the next page                                   |
| `refresh`   | a call with `force`: `refresh`, `node` and `nodes`                                                |
| `event`     | reread after an event of `changed_by` or a stale mark                                             |
| `reconnect` | reread after a gap, when the network returns, when the app comes to the foreground                |
| `retry`     | resend after a failure                                                                            |
| `send`      | first send of a queued operation, a call or an upload step                                        |

```swift
for await item in try DB.requests() {
  switch item {
  case .ready: break
  case let .request(operation, variablesJson, reason): log(operation, variablesJson, reason)
  case let .lost(first, last): log(first, last)
  case let .ended(cause): log(cause)
  }
}
```

```kotlin
DBlayer.requests().collect { item ->
    when (item) {
        RequestLogItem.Ready -> Unit
        is RequestLogItem.Request -> log(item.operation, item.variablesJson, item.reason)
        is RequestLogItem.Lost -> log(item.first, item.last)
        is RequestLogItem.Ended -> log(item.cause)
    }
}
```

## Automatic refetching

| Event                                       | Result                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Successful `create` or `destroy` of a model | no refetch: the answer places or removes the row. Scopes are refetched only when the action declares `refetch`. A `destroy` answered with `NOT_FOUND` makes every relation of the model stale                                                                                                                                                                                                                                                                                     |
| Successful mutation with `refetch`          | the listed scopes become stale and are refetched once the loads of the same list that were in flight at the commit have finished, so the reply that lands last is always issued after the commit. A load that began before the commit does not settle the refetch, whatever it lands. The call that sent the mutation returns with its own answer and never waits for the refetch, and the next mutation is sent meanwhile. A session change drops a refetch the old session owed |
| Permanent refusal of a model operation      | after the rollback the receiver row is reread through the `node` scope (when the model has one) and the `refetch` scopes become stale, as after success. A network failure and `Unauthorized` do not trigger it. Single-flight keeps it to 1 request |
| `freshness` expired                         | checked on the next read or network return. Expiration alone sends no request                                                                                                                                                                                                                                                                                                                                                                                                     |
| App returns to the foreground               | an online idle window with a reader reloads when its scope declares `refresh_on_foreground: true`, whether fresh or stale. An offline foreground does nothing                                                                                                                                                                                                                                                                                                                     |
| Network returns after offline               | stale windows with readers reload in small groups, events that coincide share one run, close cancels the run. Live subscriptions with readers reconnect. Waiting changes and uploads resume                                                                                                                                                                                                                                                                                       |
| Network is back                             | the queue is sent and stale relations are refetched                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Subscription reconnects after lost events   | watched relations of the affected models are refetched once                                                                                                                                                                                                                                                                                                                                                                                                                       |

You never write "if the mutation worked, reload the list".

## State for the UI

The load state of a stream (`phase`, `error`, `hasMore`, `settled`, `total`, `loaded`) and the queue of operations (`scope.operations()`, pending writes and send errors) are live too.

### Application protocol configuration

`EncryptedConfig` requires `endpoints`, `protocolLabel`, `cablePurpose`, and `cableChannel` alongside `origin`, `serverKey`, and `pin`.
These arguments have no defaults. Paths start with one slash and contain no query or fragment.
Protocol labels are nonempty and contain no control characters.
The session operation dictionary maps application names to `EncryptedSessionOperation` records. The reserved `logout` operation uses `endpoints.logout` with a credential.

```swift
let endpoints = EncryptedEndpoints(
  graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout",
  bootstrap: "/api/bootstrap", sessionOperations: [
    "signIn": EncryptedSessionOperation(path: "/api/sign-in", class: .authenticated),
    "ticket": EncryptedSessionOperation(path: "/api/ticket", class: .sessionless)
  ]
)
let protocolLabel = "example-ws-v1"
let cablePurpose = "ws.hello"
let cableChannel = "EncryptedGraphqlChannel"
let encrypted = EncryptedConfig(
  origin: origin, serverKey: bundledServerKey, pin: bundledServerPin,
  endpoints: endpoints, protocolLabel: protocolLabel,
  cablePurpose: cablePurpose, cableChannel: cableChannel
)
let config = try await DB.serverConfig(encrypted: encrypted, platform: .ios, version: appVersion)
try await DB.open(encrypted: encrypted, credentials: credentials, modelsJSON: modelsJSON, uploadTaskTitle: "Uploading")
```

```kotlin
val endpoints = EncryptedEndpoints(
    graphql = "/api/graphql", cable = "/api/cable", logout = "/api/logout",
    bootstrap = "/api/bootstrap", sessionOperations = mapOf(
        "signIn" to EncryptedSessionOperation("/api/sign-in", EncryptedOperationClass.AUTHENTICATED),
        "ticket" to EncryptedSessionOperation("/api/ticket", EncryptedOperationClass.SESSIONLESS)
    )
)
val protocolLabel = "example-ws-v1"
val cablePurpose = "ws.hello"
val cableChannel = "EncryptedGraphqlChannel"
val encrypted = EncryptedConfig(
    origin, bundledServerKey, bundledServerPin, endpoints,
    protocolLabel, cablePurpose, cableChannel
)
val config = DBlayer.serverConfig(context, encrypted, AppPlatform.ANDROID, appVersion)
DBlayer.open(context, encrypted = encrypted, credentials = credentials, modelsJson = modelsJson, uploadNotification = notification)
```

`DBlayer.open` and `DBlayer.serverConfig` take the Android `Context` first. Applications supply every protocol value before opening or fetching config.
`UrlSessionTransport` and `OkHttpTransport` take no cable path. The core builds the WebSocket address from `EncryptedConfig` and passes it to `openCable`; the platform opens it and reports raw frames to `CableEvents`.

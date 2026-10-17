# Server sync

DBlayer talks to a GraphQL endpoint. You describe operations and models once. Requests, pagination, retries, refetching and subscriptions are handled for you.

## Connecting

1. Create a transport with the endpoint and a token supplier. The transport reads the current token once per request.
2. Open the database with your models and the transport.
3. Pass your GraphQL operation types. The name and document come from the type, the response root is the first field of the document.

An operation type declares `Data` (Swift) or a `dataSerializer` (Kotlin) only when your code reads the response itself through the typed transport calls. An operation that models and actions run needs neither: leave `Data` out and it is `Never`, and the library still normalizes the response into rows. Do not write a response type that copies a model.

```swift
enum ArchiveChatMutation: GraphQLMutation {
  struct Variables: Encodable, Sendable { let id: String }
  static let operationName = "ArchiveChat"
  static let document = "mutation ArchiveChat($id: ID!) { archiveChat(id: $id) { id archived } }"
}
```

```kotlin
object ArchiveChatMutation : GraphQLOperation {
    override val operationName = "ArchiveChat"
    override val document = "mutation ArchiveChat(\$id: ID!) { archiveChat(id: \$id) { id archived } }"
}
```

The transport is an interface with three calls: `query`, `mutate` and `subscribe`. Your app gets HTTP and WebSocket implementations for each platform. Tests use a mock transport. A REST transport implements the same interface.

## Encrypted session

A server that takes only encrypted requests (JWE over HTTPS) needs no signing code in the app. The core builds and opens every request. The app gives it three things:

| Piece | What it is |
| ----- | ---------- |
| `EncryptedConfig` | `origin` (`https://host[:port]`, no path; see the origin rule below), `serverKey` (the server public key, base64 DER) and `pin` (SHA-256 of that DER, lowercase hex). The key must have at least 3072 bits and match the pin. Ship both with the app. Do not download them |
| `EncryptedHttp` | `post` sends the request text to `origin + path` as `application/jose`. `get` reads the public config. Both return the status and the body. `UrlSessionEncryptedHttp` (Swift) and `OkHttpEncryptedHttp` (Kotlin) do it |
| `CredentialStore` | `get`, `set` and `clear` keep one string in the Keychain (iOS) or the Keystore (Android). The core creates the credential, calls `set` before the first request and never writes it to SQLite |

The database opens with its own transport for queries and mutations. The transport you pass serves subscriptions and file uploads.

```swift
let origin = URL(string: "https://api.example.com")!
let database = try Database.openEncrypted(
  config: Config(path: path, dataVersion: nil, cacheRows: 4096),
  modelsJson: modelsJSON,
  encrypted: EncryptedConfig(origin: origin.absoluteString, serverKey: bundledServerKey, pin: bundledServerPin),
  http: UrlSessionEncryptedHttp(origin: origin),
  credentials: KeychainCredentials(),   // your CredentialStore
  transport: DatabaseTransport(cable: { await cable.current })
)
_ = try await database.sessionCall(operation: "otp", bodyJson: #"{"phone":"+61400000000"}"#)
let answer = try await database.sessionCall(operation: "confirm", bodyJson: #"{"code":"123456"}"#)
```

```kotlin
val origin = "https://api.example.com"
val database = Database.openEncrypted(
    Config(path, null, 4096u),
    modelsJSON,
    EncryptedConfig(origin, bundledServerKey, bundledServerPin),
    OkHttpEncryptedHttp(origin),
    KeystoreCredentials(),   // your CredentialStore
    DatabaseTransport(cable = { cable.current }),
)
database.sessionCall("otp", """{"phone":"+61400000000"}""")
val answer = database.sessionCall("confirm", """{"code":"123456"}""")
```

`sessionCall` sends one of the sign-in operations and returns the answer body as JSON text. An answer with an `error` field throws `DbError.Session` (`DbException.Session` in Kotlin) with a typed `code`.

| Operation | Body | Answer |
| --------- | ---- | ------ |
| `otp` | `phone` | `nextStep` |
| `confirm` | `code` | `nextStep` |
| `social` | the Apple or Google sign-in fields of the server API: the source name (`apple` or `google`), `idToken`, optional `authCode` and `name` | `nextStep` |
| `logout` | `{}` | `revoked`. The core then deletes the credential and the session state |

| Session code | Meaning | Credential and state |
| ------------ | ------- | -------------------- |
| `invalidCode`, `invalidPhone`, `invalidProvider`, `providerRejected`, `alreadyAuthenticated` | the server refused the step | kept, the sequence is confirmed |
| `rateLimited`, `accountUnavailable` | too many codes or a disabled account | kept, the sequence is confirmed |
| `sequenceConflict`, `unauthenticated`, `sessionEnded` | the server holds another request for this sequence or no session | kept, the pending request stays |
| `replayedRequest` | a read arrived twice. The core already repeated it once with a new request id | kept |
| `serverKeyMismatch` | the key in the config differs from the pinned key | kept |
| `other` | any other `error` value, the text is in `detail` | kept |

### Session errors in loads and mutations

A sealed answer of the encrypted session that carries an `error` field with a session code throws `DbError.Session(code:detail:)` (`DbException.Session` with `code` and `detail` in Kotlin). It is the same case that `sessionCall` throws, with the same `SessionCode`. No `Graphql` error and no text is parsed. `detail` holds the code text of the server.

| Where | When it throws |
| ----- | -------------- |
| `Query.load()`, `loadMore()`, `refresh()`, a forced reload (`readNode(force: true)`, `nodes`) | the request of the load got the sealed refusal |
| `DB.run(_:variables:)` and generated `Operations.*` calls (`DBlayer.database.run` in Kotlin) | the call got the sealed refusal |
| `settled()`, `confirmedId()` and `answer()` of a row or an action that has a mutation | the queued mutation got the sealed refusal. The row rolls back or stays failed by its `onFailure` rule, as for any refusal |
| live reads (`live`, `LiveList`, `updates`) | never. They read the local database. A load under them that gets the refusal fails quietly, the explicit `load()` shows it |

`flush()`, `retry()` and `resume()` do not throw for the refusal of one operation. The core keeps the credential and the pending request for every code. Sign-in state changes only through `sessionReset()` and a `logout` answer with `revoked`. A refusal with a code outside the `SessionCode` table (for example `INTERNAL_SERVER_ERROR`) stays a `Graphql` error. Errors inside `data` and `errors` of a normal answer stay `Graphql` too.

A platform transport (`MockTransport` and your own `Transport`) throws `DbError.Session` to the same effect.

```swift
do {
  try await Moment.feed.load()
} catch DbError.Session(let code, _) where code == .accountUnavailable {
  showBlocked()
} catch DbError.Session(let code, _) where code == .unauthenticated {
  signOut()
} catch DbError.Network {
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
} catch (error: DbException.Network) {
    showOffline()
}
```

`serverConfig(platform:version:)` (`serverConfig(platform, version)` in Kotlin) reads `GET /api/v3/config`. Both arguments are optional. When set, the core sends them as the URL-encoded query `?platform=ios&version=1.2.3`; the server decides what an unknown platform means. Encrypted paths (`/api/v3/graphql`, sign-in) never carry a query: the core refuses them. The answer is not cached by the server and depends on the request address.

| Field | Meaning |
| ----- | ------- |
| `protocolVersion`, `minAppVersion`, `graphqlUrl`, `cableUrl` | versions and addresses. Take them from here |
| `updateRequired` | `true` when `version` is below the minimum for `platform`. `false` without both arguments |
| `country`, `countryDisabled` | the country of the device address, or `nil`. `countryDisabled` is `true` when phone sign-in is off there |
| `countries` | sign-up countries: `name`, `code`, `flag`, `phoneCode`, `phoneMask`, `phoneFormatter` (a string to string map, empty when absent), `enabled`, `phoneAuthEnabled` |
| `trackingPromptEnabled` | `true` when the app shows the tracking prompt |
| `iosStoreUrl`, `androidStoreUrl` | store links, or `nil` |
| `serverKey` | the announced key, compared with the pin |

A missing or `null` field reads as `false`, an empty list or `nil`. Unknown fields are ignored. The core compares the announced key id with the pin and the announced DER with `EncryptedConfig.serverKey`, and throws `serverKeyMismatch` when either differs or when the algorithms are not RSA-OAEP with A256GCM. Never take the key from the config.

```swift
let config = try await DB.database.serverConfig(platform: "ios", version: appVersion)
if config.updateRequired { showUpdate(config.iosStoreUrl) }
```

```kotlin
val config = DBlayer.database.serverConfig("android", appVersion)
if (config.updateRequired) showUpdate(config.androidStoreUrl)
```

Origin rule: `https://host[:port]` is accepted for any host. `http://host[:port]` is accepted only for development hosts: the name `localhost`, a name that ends with the label `.local`, and the IP literals of loopback (`127.0.0.0/8`, `::1`), private networks (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `fc00::/7`) and link-local (`169.254.0.0/16`, `fe80::/10`). The core parses the host strictly: decimal, hex and octal address forms, trailing dots, user info, paths, queries and fragments are refused. The request audience stays the origin you gave. The JWE protects the payload over http, but the transport is not protected, so use http for development only. Production origins use https.

`DB.openEncrypted` (Swift) and `DBlayer.openEncrypted` (Kotlin) open the encrypted database like `open` does and set `DB.database` and `DBlayer.database`. `http` defaults to `UrlSessionEncryptedHttp` or `OkHttpEncryptedHttp` on the origin.

```swift
try DB.openEncrypted(encrypted: EncryptedConfig(origin: origin, serverKey: bundledServerKey, pin: bundledServerPin), credentials: KeychainCredentials(), transport: transport, modelsJSON: modelsJSON)
```

```kotlin
DBlayer.openEncrypted(EncryptedConfig(origin, bundledServerKey, bundledServerPin), KeystoreCredentials(), transport, modelsJson)
```

Rules of the session:

- Every mutation, including a sign-in operation, carries the next sequence number. A query carries none and gets a new request id each time.
- The core stores a mutation (request id, response key, sequence, path and body) before it sends it. If the answer is lost, the same call, `sessionResume()` or a restart sends the identical request again and the server answers from its record. Only the time stamp changes.
- While a request is pending, a different mutation fails with a network error and sends nothing. `sessionResume()` sends the pending request and returns its answer.
- A refused step such as `invalidCode` confirms the sequence. `sequenceConflict`, `unauthenticated` and `sessionEnded` do not: the pending request stays. The core never clears the session for any code. Treat these three as a lost session: call `sessionReset()`, delete your signed-in state and sign in again.
- A credential with no matching session state (for example a restored Keychain with a new database file) is replaced by a new credential. The next call is unauthenticated.
- Errors: `503` is a retryable HTTP error, `400` means a bad request or a device clock more than 2 minutes behind or 30 seconds ahead of the server, `413` means more than 256 KiB. A lost connection is a network error.
- `sessionReset()` deletes the credential and the stored state without contacting the server.
- A `logout` answer with `revoked` clears the credential and the state in one step, without confirming the sequence first. If your `CredentialStore.clear` fails, the pending logout stays and `sessionResume()` finishes it with the answer the server stored.
- The core never clears the credential for a refusal code, an HTTP error, a key mismatch or a closed cable.

### Persisted queries

The core sends a GraphQL call as its SHA-256 hash first, without the document. This keeps the body small: the body loses the whole `query` field (for a 545 byte document the body drops from 724 to 162 bytes before encryption).

| Server answer | Core |
| ------------- | ---- |
| result | returns it |
| `PERSISTED_QUERY_NOT_FOUND` (HTTP 200, `errors[].extensions.code`) | sends the call once more with the document and the hash, returns that answer |
| `INVALID_INPUT` or any other error | returns it, no second send |

The second answer is final: a second `PERSISTED_QUERY_NOT_FOUND` goes to your app and the core sends no third request. Swift and Kotlin code does not change: the generated operations and `sessionResume()` behave as before and return the answer of the last send.

- A query is sent again as a new request: new request id, new response key. The server reports the miss before it records the request id, so nothing is spent.
- A mutation is sent again as a new mutation. The server has already taken the first sequence number `N` and answers the miss with it, so the core confirms `N` and removes the pending request. The second send takes `N+1`, new request id and response key, and the core stores it before it sends it, like any mutation. The same `N` with another body would get `SEQUENCE_CONFLICT`.
- After a restart `sessionResume()` sends the stored request unchanged. A stored hash-only request that is answered with a miss returns that answer, and your app sends the mutation again as usual.

The server registry fills itself. The first call of a document after the app is installed (or after the server lost the registry) costs 2 requests: the miss and the send with the document, which the server stores. Every later call of that document, from any device, is one hash-only request. If the server registers the documents in advance with `rake v3:persisted_queries:register[/path/to/GraphQL]`, there are no misses and every call is one hash-only request.

## Responses become rows

Every response, whether from a query, a mutation or a subscription event, goes through the same rules:

| Rule | Behavior |
| ---- | -------- |
| Identity | the objects of a relation are rows of its model and need an `id`. Objects without one are dropped |
| Nesting | objects found at the `from:` path of an association (its name by default) become rows of the target model. `belongs_to` keeps the key (`userId`) on the outer row, `has_many` and `has_one` rows get the outer id, `references` keeps the ids |
| Lists and pages | lists and `edges { node }` with `pageInfo` are recognized from the document and keep server order |
| Partial responses | fields a query did not select are never erased, each field is updated on its own and the newer revision wins |
| Unknown fields | ignored |
| One commit | a whole response lands in one commit |

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

- Fresh data is not requested again. `fresh_for` sets how long data counts as fresh.
- Identical parallel loads become one request.
- Network and server errors of a load are retried within a budget; fatal errors are returned. Mutations are never retried by themselves, see [sync state](sync-state.md).
- The built-in transports tell the core whether the server answered. A GraphQL error reaches the core with its messages and a 401 or a 5xx answer as an HTTP error: a mutation is rejected (answers below 500) or delivery unknown (5xx) and is never sent again by itself, a load retries a 5xx. A request that never left the device (offline, host not found, connection refused) is a network error and is retried. A timeout or a lost connection after the request was sent is a network error for a load and delivery unknown for a mutation.
- A response older than the data already stored never overwrites it.
- A row that another response stored while this one was in flight keeps its stored data and still joins the list this response names.
- `max_pages` limits how many pages one load takes.

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
- Rows that are missing from the response leave the list but stay in the database, because they may belong to other lists. A row is deleted when the server says so (`deleted_by`, a null node, a delete event).
- A failed request changes nothing.

An event never loads a list again. A row that arrives by an event is stored like any other row (V2 does the same: the event inserts the row and no list is fetched). A relation that lists every row of its params (a scope with `where :chatId` and `joins: :events`) also gets the new row appended, when that list is loaded, in the same commit as the event. Declare `joins: :events` only for a relation without server-side filters (`variables`), because the library cannot tell from the row whether the server would have listed it. Other lists show the row on their next load. A scope with `text:` params takes no event rows (`joins: :events` is refused there): its members are the server answer, and a changed chat shows in it after the window reloads (`refetch` on an action, or `load` once the window is stale). Only a row whose parent is not stored makes the library reload the lists of that parent model.

`refresh(replace: true)` replaces the whole list with the response (unconfirmed records still stay). It is meant for small lists where the server is the only authority.

## Mutations

```ruby
operation :create do
  mutation CreateMoment
  input text: :text
end
```

Fields of the kind `id` that hold a whole number are sent as JSON numbers. A GraphQL `ID` accepts a number and so does an `Int`, while a string only fits an `ID`.

Servers that take one input object and answer with a payload use `wrap` and `root`:

```ruby
operation :update do
  mutation UpdateChat
  wrap :input
  root "chatUpdate.chat"
end
operation :send_gift do
  mutation GiftSend
  args giftId: :string
  local_effect :none, reason: "Apply the server response"
  wrap     :input
  root     "giftSend.user"
  lands    Message, root: "giftSend.message"
  refetch_of CurrentUser, relation: :current
  input    userId: :id, giftId: :giftId
end
```

`response :ignore` skips reading the response, for a mutation whose answer is not the model's row; add `refetch :profile` (or a `refetch` line in an action) to reload the relation instead.

`wrap :input` sends `{ input: { ...variables } }`. `root` is the path of the row inside the response, by default the first field of the operation. `lands Model, root: "a.b"` writes the row at another path of the response into that model, once per line, so a payload that carries several rows lands all of them. The model must exist, and a path that the response lacks lands nothing. `refetch_of Model, relation: :name` marks a relation of another model stale after the action succeeds and reloads it when someone reads it, for counters that live on another row.

`create`, `update` and `destroy` call the named mutation. The `input` line maps row fields to variables (same names by default). The `args` line in a model operation declares values that are sent but not stored (see [writing data](writing-data.md)). Its kinds are the kinds of an operation without a model: a scalar kind or the name of a schema enum or input object (see [models](models.md#check-against-the-server-schema)). The response is normalized like any other response and replaces the optimistic row. See [writing data](writing-data.md) for the queue, retries and rollback.

## Operations without a model

Sign-in, sign-out, a QR code or a price list belong to no row. Declare each one with an `operation` block. The generator writes a typed call for Swift and Kotlin, and the call sends the operation and reads the fields you name in `result`:

```ruby
operation :qr_code_create do
  mutation QrCodeCreate
  wrap     :input
  args     kind: :string, size: :int?
  result   url: :string
end
```

```swift
let code = try await Operations.qrCodeCreate(kind: "chat", size: 3)
print(code.url)
```

```kotlin
val code = Operations.qrCodeCreate(kind = "chat", size = 3)
println(code.url)
```

- `mutation` or `query` names the operation. Pass the operation type to `DB.open` (Swift) or `DBlayer.open` (Kotlin) like any other operation.
- `args` are the variables, with the same kinds as the args of `create`. A trailing `?` makes one optional and an omitted one is not sent. `wrap :input` sends them as `{ input: { ... } }`.
- `result` lists the fields to read from the root field of the answer, all optional. Without `result` the call returns nothing. A field is a scalar (`:bool`, `:int`, `:num`, `:string`, `:id`), a schema enum name (`:WidgetKind`), `:ids` or `[:string]` for a list of scalars, `:json` for a value you read yourself (`JSONValue` in Swift, `JsonElement` in Kotlin), `{ a: :int }` for an object, or `[{ a: :int }]` for a list of objects. Each object becomes its own type named after the call and the field (`PricingLoadResultGifts`). An `:id` field accepts a number or a string and arrives as a string.
- `args input: :StickerInput` names a schema input object. The generator reads it and nested inputs from `--schema`.
- Required inputs remain required. Nullable fields use `GraphQLNullable`: omitted by default, explicit null, or a supplied value. Lists preserve every dimension and element nullability.
- A required field with a schema default can be omitted. Nullable fields with defaults still distinguish omission from explicit null.
- `args kind: :WidgetKind` and `result kind: :WidgetKind` name a schema enum. It becomes a typed enum with an `unknown` case, in inputs too (see [models](models.md#enums)). A custom scalar needs a `scalar` line (see [models](models.md#custom-scalars)). The `JSON` scalar uses `JSONValue` in Swift and `JsonElement` in Kotlin.
- The call is sent at once. It is not queued, stored or retried, and it fails when the device is offline, when the transport fails or when the answer carries errors. The error text holds every message.
- A failed call or load throws a typed error. `Http` carries the status and the body (401, 503), `Graphql` carries every message of an answer with errors in order, `Session` carries the session code of a refusal of the encrypted session (see [session errors in loads and mutations](#session-errors-in-loads-and-mutations)), `Network` and `Interrupted` tell whether the request left the device. Any other failure is a `Sync` error with its text.

```swift
do {
  _ = try await Operations.pricingLoad()
} catch DbError.Http(let status, _) where status == 401 {
  signOut()
} catch DbError.Graphql(let messages) {
  show(messages)
}
```

```kotlin
try {
    Operations.pricingLoad()
} catch (error: DbException.Http) {
    if (error.status == 401.toUShort()) signOut()
} catch (error: DbException.Graphql) {
    show(error.messages)
}
```
- Do not use it for a row of a model. The row is written by `create`, `update` or a model `operation`, which keep the queue, the retries and the rollback.

An operation that reads lists, objects and raw JSON from the answer:

```ruby
operation :pricing_load do
  query  Pricing
  result gifts: [{ id: :id, name: :string, price: :int }], videoIntro: { enabled: :bool }, tags: [:string], spec: :json
end
```

```swift
let pricing = try await Operations.pricingLoad()
let names = pricing.gifts?.compactMap(\.name) ?? []
```

```kotlin
val pricing = Operations.pricingLoad()
val names = pricing.gifts?.mapNotNull { it.name }.orEmpty()
```

An operation whose root field is a list, such as a search that returns the matching items, declares `list`. `result` then describes one element and the call returns a list of it:

```ruby
operation :giphy_load do
  query  Giphy
  list
  args   type: :string, limit: :int?
  result id: :id, url: :string, isSticker: :bool
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

`list` needs a `result` line.

An operation that takes a schema input object:

```ruby
operation :sticker_send do
  mutation StickerSend
  args     input: :StickerInput
  result   id: :string
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

A list of schema input objects or enums takes the name in brackets. `[:WidgetInput]` is a list that holds no nil, `[:WidgetInput?]` may hold nil items, a `?` after the bracket (`[:WidgetInput]?`) makes the whole list optional and an omitted one is not sent. The check compares the list with the variable of the mutation: an arg that may hold nil items does not fit `[WidgetInput!]`.

```ruby
operation :widgets_send do
  mutation WidgetsSend
  args     widgets: [:StickerWidgetInput]?
  result   id: :string
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
  fetch MomentsFeed
  updates MomentEvents, deleted_by: :destroyed
end
```

- `root: "a.b"` is the path of the row inside the event payload (default: the first field), `id: "messageId"` is the key that holds its id (default: `id`). Use them when the server wraps the row or sends only an id for a delete.
- Events are normalized like responses. `deleted_by:` names the field that marks a deleted row.
- An event changes only the fields it carries. Fields it does not select keep their stored values. The read of the stored row and the write happen in one transaction, so a local edit made at the same moment is never overwritten by an older copy.
- A delete event removes the same rows as a local `destroy`: the row and its `dependent: :destroy` children, in one commit.
- An event that creates a row the library did not have makes the relations of that model stale, like a successful `create`, and refetches the ones someone is reading.
- If the first connection fails, the library retries it with the same growing delay and the same offline rule as a reconnect. `subscribe` does not throw for a refused connection.
- The subscription starts when the first reader connects, is shared by all readers and closes some time after the last reader leaves (30 seconds by default).
- After a dropped connection the library reconnects with a growing delay and refetches the relation, because events may have been missed. Offline (`setOnline(false)`) it does not reconnect and spends no attempts. `setOnline(true)` reconnects at once.
- Bursts of events are merged into one commit.

### Model-level subscriptions

`updates` lives while a screen reads the relation. For events that should reach the database for the whole session (presence, counters), declare `listen` on the model. It takes the same options as `updates`:

```ruby
model User do
  listen "userUpdated", debounce: 300.milliseconds
end
```

```swift
try DB.listen()      // after sign-in
try DB.unlisten()    // on sign-out
```

```kotlin
DBlayer.listen()
DBlayer.unlisten()
```

## Automatic refetching

| Event | Result |
| ----- | ------ |
| Successful `create` or `destroy` of a model, or a subscription event that creates a row | relations rooted in that model become stale and are refetched at once if someone is reading them |
| Successful mutation with `refetch` | the listed relations become stale and are refetched once the loads of the same list that were in flight at the commit have finished, so the reply that lands last is always issued after the commit |
| `fresh_for` expired | refetched when next read or when the app resumes |
| App resumes | the app calls `DB.resume()` (Swift) or `DBlayer.resume()` (Kotlin), stale relations with readers are refetched in small groups |
| Network is back | the queue is sent and stale relations are refetched |
| Subscription reconnects | the relation is refetched |

You never write "if the mutation worked, reload the list".

## State for the UI

Loading state (idle, loading, error with its kind, has more pages, last loaded time) and the outbox (pending writes, send errors) are live too.

# Errors

Every call of the library fails with one typed error: `DbError` in Swift and `DbException` in Kotlin. The cases and their fields are the same on both platforms.

| Case | Meaning |
| ---- | ------- |
| `NotOpen` | A call before the first successful `open`. |
| `Closed` | A call on a database that was opened and closed. Every call answers `Closed` before it reads its arguments. A second `close` succeeds. |
| `NoSession` | Server work without a session or transport, such as a send, a load or a create of a server model on a database opened without a session. |
| `Session` | A sealed answer of the encrypted session carried a session code (`code`, `detail`). See [ending a session](server-sync.md#ending-a-session). |
| `Unauthorized` | The server refused the request with HTTP 401 or 403. The session stays. |
| `Security` | The server key or the response did not pass the check (`code`: `ServerKeyMismatch`, `ResponseAuthentication`, `TlsTrust`). |
| `Config` | The open or the server config was refused (`code`: `InvalidFormat`, `UnsupportedVersion`, `InvalidRequest`). `open` also fails with `Config` on Android below API 34 when the manifest lacks `FOREGROUND_SERVICE_DATA_SYNC`. |
| `Invalid` | An argument or a declaration is wrong: a `limit` of 0, a load call without a relation, a second reader of a stream, an unreadable upload file, `encrypted` without credentials. |
| `NotFound` | The row, the operation or the model does not exist. `upload` on a missing row ends with `NotFound`. |
| `AlreadyExists` | A `create` with an id that exists (`model`, `id`). |
| `Offline` | The network is away and the call fails at once instead of waiting. |
| `Network`, `Http` | The request failed on the network, or the server answered with a status (`Http` carries `status`). A redirect is never followed and is reported as `Http`. |
| `RequestTooLarge` | The request exceeds the local limit (`size`, `limit`). |
| `ResponseTooLarge` | The answer exceeds the limit (`limit`): 16 MiB for a GraphQL answer, 64 KiB for the answer of a file upload. The platform stops reading at the limit. A read or an upload fails without a retry. A sent mutation counts as an unknown outcome and repeats with the same request id. When the retries end, the operation fails with `kind` `response_too_large` and `responseLimit`. |
| `Graphql` | The server answered with GraphQL errors (`messages`, `extensions`). |
| `Rejected` | The server refused the operation of a receipt `confirmed()` (`failure`: an `OperationFailure`, `kept`). `kept` is true when the failed change stays for `retry` or `discard`. |
| `Interrupted` | The wait was cancelled. The detail identifies the cause. A call deadline returns its last error or `Network`. |
| `Dropped` | The request left and its answer did not come. |
| `InFlight` | The operation is on its way and cannot be dropped or cancelled now. |
| `Storage` | The database could not save its state. |
| `Copy` | A copy step failed (`detail`). |
| `Runtime`, `Internal` | A task of the library failed, or it met a state it cannot serve, such as a damaged model event (`detail`). |
| `CorruptManifest` | The manifest of the database file cannot be read. |

A call that fails in order reports the first failing check. For `upload` the order is `Closed` or `NoSession`, then `Invalid`, then `NotFound`.

## Where an error arrives

| Where | Result |
| ----- | ------ |
| A call (`create`, `load`, `read`, `sessionCall`) | It throws the error. |
| A receipt `confirmed()` | The value, or `Rejected`, or the error of the end cause. |
| A receipt `confirmed()` | the value, `Rejected` with an `OperationFailure` and `kept`, or the error of the end (`NoSession`, `Closed`). See [writing data](writing-data.md#what-the-server-answered). |
| `sync.error` and `uploads[action].error` | An `OperationFailure`: `kind`, `sessionCode`, `requestSize`, `requestLimit`, `message`, `extensions` and `responseLimit`, in this order. |
| A stream | It never throws. The state carries the last load `error`. The last value is `ended` with a cause: `SessionEnded`, `Closed`, `Cancelled`, `Invalid`, `Storage` or `Internal`. |
| A transfer or an operations stream | The last item is `ended` with the cause. |
| `RetryProgress` | It reports waiting retries as `RetrySnapshot` values and no error. |

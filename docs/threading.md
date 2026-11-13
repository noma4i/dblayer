# Threading and performance

Every call is asynchronous and returns without waiting for SQLite or the network. Values reach you from library threads as snapshots and deltas of a stream. The Swift and Kotlin bindings hold no lock and keep no registry.

## Rules

Each piece of mutable state in the core has one owner with one queue. The owner handles one input at a time and talks to other owners with values only. A call into the app (a transport request, a credential store call, a stream wake-up) never runs under a library lock: a state change collects its effects and publishes them in order after the guards are released. Debug builds check tracked guards before effects and selected callbacks. A `lock-ok: reason` comment exempts one line from the static check and is inventoried in the lock audit report.

## What you can rely on

- Record writes use `async` in Swift and `suspend` in Kotlin. Rust runs SQLite writes, argument decoding and result conversion off the caller thread.
- Database calls prepare their JSON and decode their answers off the caller thread: Kotlin on `Dispatchers.Default`, Swift off the actor of the caller. A call runs on the thread of the caller (Kotlin) or on the actor of the caller (Swift) only until the library takes the place of the call in the order of the database; the rest of the work runs off it. A projection decodes rows off the main thread and assigns ready values on it. Generated `create`, `update`, `set`, model actions that are not uploads, node reads, edge reads and operation calls follow the same rule. Upload calls keep their own paths.
- A stream delivers values from library threads. A Kotlin `Projected` publishes its `StateFlow` from `Dispatchers.Default`, and a Swift `Projected` applies values on the main actor.
- You can call the library from the main thread: the body of every call runs on library threads. Do not wrap a call in `flowOn` or `withContext`, and do not move it off the main actor in Swift.
- A delivery touches only the rows it carries. The bindings hold no lock for it.
- A session end or reset ends a waiting call with `NoSession`. Close gives `Closed`. Cancellation ends a wait with its cause. A call deadline returns its last error or `Network`.
- Await `discard(action)` and `sessionReset()` to finish removing queued changes or clearing account data before starting dependent work.
- Opening a database is asynchronous (`await DB.open` in Swift, suspend `DBlayer.open` in Kotlin). Registering operations is asynchronous. A live query attaches off the caller thread when it is collected.
- Calls that read or write files (`attach`, `file`, `url`, `detach`) run on a background thread of the library, never on the caller thread.
- Upload metadata reads, checksums and byte-range reads use the blocking pool. Cancelling a transfer does not wait for queued file work.
- `cachedRow` and the edge value reads answer without the caller thread waiting for the database lock and decode their rows off the caller thread. They are `async` in Swift and `suspend` in Kotlin, as is `Query.edgeValues`. See [live data](live-data.md#reading-before-the-first-delivery).
- The transports on both platforms do the whole network exchange, including reading the response body, off the caller thread: Kotlin on `Dispatchers.IO`, Swift through the async `URLSession` API. You can call them from the main thread.
- Disk databases use WAL with 2 read-only connections and 1 writer. Each read holds a transaction across rows, nested values and synchronization metadata. Reading the current database generation does not wait for the writer. A read waits when both read connections are occupied.
- Reads involving temporary models use their owning connection. Entirely in-memory databases also share their connection with the writer.
- Parsing and normalization are done before the writer runs. Only the writer commits, one batch per transaction and one revision per batch.
- A stream is a journal of at most 256 values in the core. A reader that is slower than the changes gets a restoring snapshot for row changes and a `lost` range for model events. Platforms keep no buffer. See [live data](live-data.md).
- `scope.entity(Model, id)` opens a stream whose first value comes from the cache of the core.
- Cancelling or dropping a stream stops its work at once without leaking tasks. Cancelling a pending `pull` cancels the stream: its next value is `Ended(Cancelled)` and no delivery follows.
- A stream whose window loads from the server returns its handle at once. Its first value carries the rows and the state together.
- When the app is in the background the core keeps working until the OS stops the process. Returning to the foreground is an input of the network owner.
- A request, a load, an upload and a subscription hold a lease that a session end, a reset or a close ends. `close` returns after every lease ended and frees the open slot. A stream opened after the close ends at once. A reopened database has no old work left.
- A pending native call resumes your coroutine or Swift task from a library lane, never from the thread that produced the result or released the call. Cancelling a call hands the cancel and the free to that lane, so neither runs while the library holds a lock.
- Load, send and upload code plans and commits SQLite work as whole units. With a multi-thread Tokio scheduler a unit hands its worker thread over while it waits for a reader slot or the writer, so a busy store cannot stop unrelated tasks, timers or cancellation. A current-thread scheduler runs the unit inline.
- A waiter may be woken by a foreign task that calls the library again. The owner collects the wake, the drop of a sender and the abort of a task as owned effects and runs them in order after it releases its state, so the nested call returns and a blocked wake does not stop later calls.
- An idle database has no timer wake-ups and sends no requests. A deadline (memory row expiry, ephemeral reset, a retry) is one scheduled input.
- Queues are bounded. Producers wait asynchronously where work cannot be combined. Data is never dropped.

## Consistency under load

Speed never relaxes the guarantees of [live data](live-data.md): ordered deliveries, atomic transitions and read-your-writes hold with many threads.

## Reads

A read never runs on the thread of an owner. A stream asks for its read, the pool of 2 reader connections runs it, and the result returns to the stream owner as a value with its revision. A commit that arrives while a read runs makes the stream read again from the revision it holds. A read that fails ends in the state of the stream as an error with its last rows.

## Diagnostics

Tests count work (reads, calls, commits) instead of wall-clock time. `subscriptionCensus` reports the streams and channels the owners hold.

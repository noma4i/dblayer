# Threading and performance

Live results arrive from library threads. Record writes run on background threads. Disk reads use separate connections from the writer.

## Rules

Foreign execution must not run under a library lock. State changes return owned effects and publish them through `OrderedEffects` after guards are released. Debug builds check tracked guards before effects and selected callbacks. A `lock-ok: reason` comment exempts one line from the static check and is inventoried in the lock audit report. The debug tracker covers the core `locked` and dispatch `held` functions and the observer guard. Other raw locks and async gates are outside its current coverage.

## What you can rely on

- Record writes use `async` in Swift and `suspend` in Kotlin. Rust runs SQLite writes, argument decoding and result conversion off the caller thread.
- Database calls prepare their JSON and decode their answers off the caller thread: Kotlin on `Dispatchers.Default`, Swift through `@concurrent`. A projection decodes rows off the main thread and assigns ready values on it. Generated `create`, `update`, `set`, model actions that are not uploads, node reads, edge reads and operation calls follow the same rule. Upload calls keep their own paths.
- A stream delivers values from library threads. A Kotlin `Projected` publishes its `StateFlow` from `Dispatchers.Default`, and a Swift `Projected` applies values on the main actor.
- A delivery touches only the rows it carries. The wrappers hold no lock for it.
- Await `discard(action)` and `sessionReset()` to finish removing queued changes or clearing account data before starting dependent work.
- Opening a database is asynchronous (`await DB.open` in Swift, suspend `DBlayer.open` in Kotlin). Registering operations is synchronous. A live query attaches off the caller thread when it is collected.
- Calls that read or write files (`attach`, `file`, `read`, `url`, `detach`) run on a background thread of the library, never on the caller thread.
- Upload metadata reads, checksums and byte-range reads use the blocking pool. Cancelling a transfer does not wait for queued file work.
- `cached()`, `cachedRow` and the edge value reads answer without the caller thread waiting for the database lock and decode their rows off the caller thread. They are `async` in Swift and `suspend` in Kotlin, as are the bridge calls `peekQuery` and `edgeValues` underneath. See [live data](live-data.md#reading-before-the-first-delivery).
- The transports on both platforms do the whole network exchange, including reading the response body, off the caller thread: Kotlin on `Dispatchers.IO`, Swift through the async `URLSession` API. You can call them from the main thread.
- Disk databases use WAL with 2 read-only connections and 1 writer. Each read holds a transaction across rows, nested values and synchronization metadata.
- Reading the current database generation does not wait for the writer.
- A read waits when both read connections are occupied. Boot and reset wait for active reads before changing tables or clearing data.
- Reads involving temporary models use their owning connection. Entirely in-memory databases also share their connection with the writer.
- A disk-only read uses the pool even when the database contains temporary models. Included associations participate in connection selection.
- Parsing and normalization are done before the writer runs. On a disk database every commit plans on a read connection. The writer only checks that no other commit landed and applies the plan. When one did, the plan runs again off the writer. Databases with temporary models and in-memory databases plan under the writer.
- Listener callbacks never run under the store lock.
- Listeners are called from library threads, never from the main thread. Move to the main thread in your UI code.
- Each listener has its own queue where the latest state wins. A slow listener does not delay others and does not build up a backlog.
- `scope.entity(Model, id)` opens a stream whose first value comes from the cache of the core.
- Frequent changes are merged: a listener gets one change per dispatch cycle, and the last state always arrives.
- Cancelling or dropping a subscription stops its work at once without leaking tasks.
- Cancelling a subscription returns without waiting for the observer registry. When the dispatch thread holds it, the request is queued and applied before the next registry access.
- A subscription to a remote relation returns its handle at once. The stored window state is read on a blocking task. The subscription delivers nothing until then and keeps only the latest merged snapshot, so its first value carries the rows and the state together.
- Releasing the last owning synchronization handle stops background subscriptions, uploads and scheduled retries. Background tasks never keep that handle alive.
- Cloned database handles keep synchronization alive until they are released. Explicitly awaited operations retain their handle until they finish. A connectivity sink does not: it refuses calls once the database is closed or released.
- `close` (`closeDatabase` on the native handle) closes the database and waits until every lease ended, then frees the open slot. Every live query, changes queue, entity source, model event feed, blob stream opened from the database ends with its defined terminal result: a waiting `next` returns the end, `current` before a first value returns a `Closed` change with an interrupted error, the initial-load wait fails as interrupted. A stream opened after the close ends at once. Synchronization stops its background tasks as if the last owner had been released, and a reopened database has no old work left. Cloud and entity refresh tasks hold only their inner state, so dropping the last handle stops them.
- A pending native call resumes your coroutine or Swift task from a library lane, never from the thread that produced the result or released the call. Cancelling a call hands the cancel and the free to that lane, so neither runs while the library holds a lock.
- An entity listener may call `retain`, `release`, `reset`, `shutdown` and `decodeFailed` from inside `changed`. The source queues its deliveries and sends them in order after it releases its state, so a call from the listener never waits on the delivery that made it.
- A reset listener may write or reset again. `Store` copies the listener list and returns the call as an owned effect. `reset` and `reset_all` run it after the file lifecycle lock, the writer and the announcement turn are released, so the cache clear and the reset event are already published and a nested commit takes the next turn.
- A live query listener may call `cancel` and any operation that changes the online state from inside `changed`. Related queries release their target observers, projection and state snapshots before they call the listener.
- A listener may own an object that queries or cancels another subscription when the library releases it. The observer registry hands every removed subscriber to the end of its own operation, so the listener and what it captured are destroyed after the registry lock is released.
- When the library cannot start a listener thread, it never calls the listener on the thread that asked. The refused start is counted (`listener_spawn_failures`). The subscriber keeps its latest state, stays idle, and starts again with its next change. A feed subscriber or an entity subscriber gets the refusal as `failure` with its next delivery, a rows or count subscriber gets its latest state.
- Load, send and upload code plans and commits SQLite work as whole units. With a multi-thread Tokio scheduler a unit hands its worker thread over while it waits for a reader slot or the writer, so a busy store cannot stop unrelated tasks, timers or cancellation. A current-thread scheduler runs the unit inline. A commit made under a state guard returns its announcement to the caller, who publishes it after the guard is released. The announcement order is the commit order, and an unpublished announcement publishes when it is dropped.
- A relation state waiter may be woken by a foreign task that calls the library again. The load owner queues each state change as an owned effect and sends the watch updates, drops senders and aborts tasks in order after it releases its state, so the nested call returns and its update follows the first one. Last-reader cancellation, flight completion and feed readiness wake their waiters the same way.
- A resume waiter, a preload wait and a live readiness waiter may be woken by a foreign task that calls the library again. The resume owner, the preload owner and the live subscription table collect their wakes, listen and unlisten calls, subscription drops and aborts as owned effects. One ordered queue runs them after the lock is released, so a nested call returns and a blocked wake does not stop later calls.
- Queues are bounded. Producers wait asynchronously where work cannot be combined. Observation requests combine within each live group. Data is never dropped.

## Consistency under load

Speed never relaxes the guarantees of [live data](live-data.md): ordered deliveries, atomic transitions and read-your-writes hold with many threads.

## Reads

Observers never read SQLite on the thread that routes events, and never under the lock that holds the observer registry. A pool of 2 reader threads (`dblayer-read`) runs every read of a subscription, of a group refresh after a commit, of a window growing and of a retained entity. The result returns to the routing thread as an event: `ReadOk` with the rows, or `ReadFailed` with the error. A subscription returns its handle at once and the first value arrives after its read. `Db::await_revision(r)` waits for these reads too, up to revision `r`. A group has at most one read in flight. A commit that arrives meanwhile marks the group to read again.

At most 2 reads can be queued, running or awaiting application of their results. A slot stays occupied until the routing thread applies its result. When both slots are occupied, live groups retain their pending requests in FIFO order without blocking the caller. Each group occupies at most one position in that queue.

Repeated entity refreshes combine their retained ids into one pending read. Repeated window expansions add their requested row counts into one pending expansion. Releasing ids or cancelling an observation removes its pending work.

A read that fails is counted (`query_reads_failed`, `entity_reads_failed`). A failed entity read reaches the subscriber of those ids as a delivery with `failure` set and no rows. The group keeps the error and its last rows, and the next batch of events of any model reads the group again. A panic while the routing thread handles one group is counted (`owner_panics`), fails that group in the same way and does not stop the other groups of the batch. The counter `sql_under_owner` counts SQL run on the routing thread or under the registry lock. It stays 0.

## Caching

A row cache keeps decoded rows keyed by model and id with their revision. Commits invalidate only the rows they touched. Repeated reads of unchanged rows do not build their cells again. Many observers of one query share a single read per commit.

A read started before a commit retains its SQLite snapshot. Its rows cannot repopulate the cache after a newer commit.

The row cache applies both a row-count limit and a 16 MiB estimated memory limit. It evicts the least recently used rows. A record larger than the memory limit bypasses the cache without evicting other records. Estimates include nested values, string capacities, collection storage and synchronization metadata. They do not measure allocator overhead or process memory.

Released native entities share a separate 8 MiB estimated budget. Rust selects retained values using row-size estimates carried with deliveries. No second serialization measures them.

Temporary values and memory records use an ordered set of deadlines. Expiration visits due entries, and deletion checks only those candidates. Rewriting a value replaces its deadline. An idle database waits for an event or the next deadline without polling.

## Diagnostics

Counters report cache hits, misses, evictions, estimated retained bytes, observation reads, observer calls and transport calls. Tests use work counters instead of wall-clock time.

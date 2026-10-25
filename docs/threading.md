# Threading and performance

Live results arrive from library threads. Record writes run on background threads. Disk reads use separate connections from the writer.

## What you can rely on

- Record writes use `async` in Swift and `suspend` in Kotlin. Rust runs SQLite writes, argument decoding and result conversion off the caller thread.
- Database calls prepare their JSON and decode their answers off the caller thread: Kotlin on `Dispatchers.Default`, Swift through `@concurrent`. `LiveList` and `LiveWindow` build `ids` off the main thread and only assign ready values on it. Generated `create`, `update`, `set`, model actions that are not uploads, node reads, edge reads and operation calls follow the same rule. Upload calls keep their own paths.
- A Kotlin live list or window publishes `rows`, `ids`, `snapshot` and `replaced` from `Dispatchers.Default`, so the comparison of rows that `StateFlow` makes never runs on the main thread. The owner of what a consumer takes is one immutable bundle of rows, edits and revision that the publisher and `takeUpdate()` swap by compare-and-set, so `takeUpdate()` never waits for a publication and always returns rows and edits of one revision. `rows` of an update never change after `takeUpdate()` returns them. Swift prepares `ids` and composes the edits off the main thread, keeps the same bundle, and assigns the prepared values on the main actor. Swift `takeUpdate()` holds a lock only to swap the bundle, never while edits are composed, and Swift observation never compares rows.
- A delivery to entities touches only the live entities whose ids it carries. Kotlin applies the values after it releases the registry lock, so `entity(id)` never waits for a delivery.
- Await `discard()` and `logout()` to finish removing queued changes or clearing account data before starting dependent work.
- Opening a database is asynchronous (`await DB.open` in Swift, suspend `DBlayer.open` in Kotlin). Registering operations and starting live queries execute synchronously.
- Calls that read or write files (`attach`, `file`, `read`, `url`, `detach`, `maintain`) run on a background thread of the library, never on the caller thread.
- Upload metadata reads, checksums and byte-range reads use the blocking pool. Cancelling a transfer does not wait for queued file work.
- `cached()` and `cachedRow` answer from memory, never wait for the database lock and decode their rows off the caller thread. They are `async` in Swift and `suspend` in Kotlin. See [live data](live-data.md#reading-before-the-first-delivery).
- The transports on both platforms do the whole network exchange, including reading the response body, off the caller thread: Kotlin on `Dispatchers.IO`, Swift through the async `URLSession` API. You can call them from the main thread.
- Disk databases use WAL with 2 read-only connections and 1 writer. Each read holds a transaction across rows, nested values and synchronization metadata.
- A read waits when both read connections are occupied. Boot and reset wait for active reads before changing tables or clearing data.
- Reads involving temporary models use their owning connection. Entirely in-memory databases also share their connection with the writer.
- A disk-only read uses the pool even when the database contains temporary models. Included associations participate in connection selection.
- Parsing and normalization are done before the writer runs. Landing server data plans its commit on a read connection and writes only when no other commit landed in between, otherwise it plans again under the writer. In-memory databases, databases with temporary models, local writes, event batches and operation updates plan under the writer.
- Listener callbacks never run under the store lock.
- Listeners are called from library threads, never from the main thread. Move to the main thread in your UI code.
- Each listener has its own queue where the latest state wins. A slow listener does not delay others and does not build up a backlog.
- `entity(id)` returns from memory. Swift and Kotlin submit batched retain/release calls in order on background threads. Rust reads the rows in its reader pool.
- Frequent changes are merged: a listener gets one change per dispatch cycle, and the last state always arrives.
- Cancelling or dropping a subscription stops its work at once without leaking tasks.
- Cancelling a subscription or expanding a window returns without waiting for the observer registry. When the dispatch thread holds it, the request is queued and applied before the next registry access.
- A subscription to a remote relation returns its handle at once. The stored window state is read on a blocking task, and the first value is delivered after it arrives.
- Releasing the last owning synchronization handle stops background subscriptions, uploads and scheduled retries. Background tasks never keep that handle alive.
- Cloned database handles and connectivity sinks keep synchronization alive until they are released. Explicitly awaited operations retain their handle until they finish.
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

# Threading and performance

Live results arrive from library threads. Record writes run on background threads. Disk reads use separate connections from the writer.

## What you can rely on

- Record writes use `async` in Swift and `suspend` in Kotlin. Rust runs SQLite writes, argument decoding and result conversion off the caller thread.
- Await `discard()` and `logout()` to finish removing queued changes or clearing account data before starting dependent work.
- Opening a database, registering operations and starting live queries still execute synchronously.
- Calls that read or write files (`attach`, `file`, `read`, `url`, `detach`, `maintain`) run on a background thread of the library, never on the caller thread.
- `cached()` and `cachedRow` answer from memory and never wait for the database lock, so they are safe on the main thread. See [live data](live-data.md#reading-before-the-first-delivery).
- The transports on both platforms do the whole network exchange, including reading the response body, off the caller thread: Kotlin on `Dispatchers.IO`, Swift through the async `URLSession` API. You can call them from the main thread.
- Disk databases use WAL with 2 read-only connections and 1 writer. Each read holds a transaction across rows, nested values and synchronization metadata.
- A read waits when both read connections are occupied. Boot and reset wait for active reads before changing tables or clearing data.
- Reads involving temporary models use their owning connection. Entirely in-memory databases also share their connection with the writer.
- A disk-only read uses the pool even when the database contains temporary models. Included associations participate in connection selection.
- Parsing, normalization and planning are done before the writer runs, so the writer only executes ready statements.
- Listener callbacks never run under the store lock.
- Listeners are called from library threads, never from the main thread. Move to the main thread in your UI code.
- Each listener has its own queue where the latest state wins. A slow listener does not delay others and does not build up a backlog.
- Entities never block either: `entity(id)` answers at once from memory, and the calls into the database for a batch of new ids run on one background queue (Swift) or thread (Kotlin), in order.
- Frequent changes are merged: a listener gets one change per dispatch cycle, and the last state always arrives.
- Cancelling or dropping a subscription stops its work at once without leaking tasks.
- Queues are bounded. A full queue makes the caller wait asynchronously. Data is never dropped.

## Consistency under load

Speed never relaxes the guarantees of [live data](live-data.md): ordered deliveries, atomic transitions and read-your-writes hold with many threads.

## Caching

A row cache keeps decoded rows keyed by model and id with their revision. Commits invalidate only the rows they touched. Repeated reads of unchanged rows do not build their cells again. Many observers of one query share a single read per commit.

A read started before a commit retains its SQLite snapshot. Its rows cannot repopulate the cache after a newer commit.

The row cache applies both a row-count limit and a 16 MiB estimated memory limit. It evicts the least recently used rows. A record larger than the memory limit bypasses the cache without evicting other records. Estimates include nested values, string capacities, collection storage and synchronization metadata. They do not measure allocator overhead or process memory.

Released native entities share a separate 8 MiB estimated budget. Rust selects retained values using row-size estimates carried with deliveries. No second serialization measures them.

Temporary values and memory records use an ordered set of deadlines. Expiration visits due entries, and deletion checks only those candidates. Rewriting a value replaces its deadline. An idle database waits for an event or the next deadline without polling.

## Diagnostics

Counters report cache hits, misses, evictions, estimated retained bytes, observation reads, observer calls and transport calls. Tests use work counters instead of wall-clock time.

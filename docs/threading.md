# Threading and performance

DBlayer never makes your threads wait. Loading data, writing, parsing responses and recomputing live results happen in the background, so a list that is loading or syncing does not cost frames.

## What you can rely on

- Public calls are asynchronous (`async` in Swift, `suspend` in Kotlin) and cheap: they enqueue work and return.
- Calls that read or write files (`attach`, `file`, `read`, `url`, `detach`, `maintain`) run on a background thread of the library, never on the caller thread.
- `cached()` and `cachedRow` answer from memory and never wait for the database lock, so they are safe on the main thread. See [live data](live-data.md#reading-before-the-first-delivery).
- The transports on both platforms do the whole network exchange, including reading the response body, off the caller thread: Kotlin on `Dispatchers.IO`, Swift through the async `URLSession` API. You can call them from the main thread.
- The database runs in WAL mode. Reads and the single writer share one SQLite connection under one lock, so a read waits while a commit runs. There is no reader pool.
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

A row cache keeps decoded rows keyed by model and id with their revision. Commits invalidate only the rows they touched. Repeated reads of unchanged rows do not parse JSON again. Many observers of one query share a single read per commit.

## Diagnostics

Counters count cache hits, misses and evictions, observation reads, observer calls and transport calls. Tests use these counters instead of wall-clock time.

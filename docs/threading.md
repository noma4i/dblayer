# Threading and performance

DBlayer never makes your threads wait. Loading data, writing, parsing responses and recomputing live results happen in the background, so a list that is loading or syncing does not cost frames.

## What you can rely on

- Public calls are asynchronous (`async` in Swift, `suspend` in Kotlin) and cheap: they enqueue work and return.
- Reads never wait for writes. The database runs in WAL mode and reads use their own connections.
- There is one writer. Parsing, normalization and planning are done before the writer runs, so the writer only executes ready statements.
- Locks are held briefly: no disk access, JSON parsing or callbacks under a lock.
- Listeners are called from library threads, never from the main thread. Move to the main thread in your UI code.
- Each listener has its own queue where the latest state wins. A slow listener does not delay others and does not build up a backlog.
- Frequent changes are merged: a listener gets one change per dispatch cycle, and the last state always arrives.
- Cancelling or dropping a subscription stops its work at once without leaking tasks.
- Queues are bounded. A full queue makes the caller wait asynchronously. Data is never dropped.

## Consistency under load

Speed never relaxes the guarantees of [live data](live-data.md): ordered deliveries, atomic transitions and read-your-writes hold with many threads.

## Caching

A row cache keeps decoded rows keyed by model and id with their revision. Commits invalidate only the rows they touched. Repeated reads of unchanged rows do not parse JSON again. Many observers of one query share a single read per commit.

## Diagnostics

Counters expose cache hits and misses, reads per commit, queue lengths, longest lock hold and transport calls. Tests use these counters instead of wall-clock time.

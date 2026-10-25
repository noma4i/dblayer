package com.noma4i.dblayer

import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.launch

internal class ChangeListener(private val body: (LiveChange) -> Unit) : QueryListener {
    override fun changed(change: LiveChange) = body(change)
}

/** The newest value waits for a slow reader. Values that arrive meanwhile are merged into it, so nothing queues up. */
internal class Latest<Value>(private val merge: ((Value, Value) -> Value)?) {
    private val lock = Any()
    private var pending: Value? = null
    private var has = false
    private val wake = kotlinx.coroutines.channels.Channel<Unit>(kotlinx.coroutines.channels.Channel.CONFLATED)

    val hasPending: Boolean get() = synchronized(lock) { has }

    fun put(value: Value) {
        synchronized(lock) {
            @Suppress("UNCHECKED_CAST")
            pending = if (has && merge != null) merge.invoke(pending as Value, value) else value
            has = true
        }
        wake.trySend(Unit)
    }

    suspend fun next(): Value {
        while (true) {
            synchronized(lock) {
                if (has) {
                    has = false
                    @Suppress("UNCHECKED_CAST")
                    return (pending as Value).also { pending = null }
                }
            }
            wake.receive()
        }
    }
}

class LiveSource<Value>(
    val initial: Value,
    private val merge: ((Value, Value) -> Value)? = null,
    internal val subscribe: ((Value) -> Unit) -> () -> Unit,
) {
    constructor(initial: Value, attach: (QueryListener) -> LiveSubscription, map: (LiveChange) -> Value) : this(initial, null, { emit ->
        val subscription = attach(ChangeListener { emit(map(it)) })
        val cancel: () -> Unit = { subscription.cancel() }
        cancel
    })

    /** The one way to read a live value. It attaches off the caller's thread when collected, and leaves no subscription when the collector is cancelled before, during or after the attach. A slow collector gets the newest value, merged with the ones it missed. */
    val flow: Flow<Value>
        get() = flow {
            val latest = Latest(merge)
            val cancel = subscribe(latest::put)
            try {
                while (true) emit(latest.next())
            } finally {
                cancel()
            }
        }.flowOn(kotlinx.coroutines.Dispatchers.Default)

    suspend fun current(): Value = flow.firstOrNull() ?: initial
}

enum class LoadPhase { Opening, NotLoaded, Loading, Loaded, Empty, Failed, Closed }

/** One delivery of a live list: the rows with the ids it replaced, the load phase of the relation and the error, as the core saw them at [seq]. */
data class Snapshot<Model>(
    val rows: List<Model>,
    /** The ids that this delivery replaced: the real id of a row that replaced a temporary one, to the temporary id. */
    val replaced: Map<String, String>,
    val reordered: Boolean = false,
    val edits: List<ListEdit<Model>>? = null,
    val window: WindowEdges? = null,
    val seq: ULong = 0uL,
    val phase: LoadPhase = LoadPhase.Opening,
    /** The running load is the first one of a list that has no rows yet. */
    val isInitialLoading: Boolean = false,
    /** The running load reloads a list that already has rows. */
    val isRefreshing: Boolean = false,
    /** The running load fetches the next page. */
    val isFetchingMore: Boolean = false,
    val hasMore: Boolean = false,
    val total: Long? = null,
    val loadedAt: Long? = null,
    val error: String? = null,
    val errorKind: String? = null,
    /** The ids of the rows that this build cannot decode. They are not in [rows]. */
    val undecodable: List<String> = emptyList(),
) {
    val isLoading get() = phase == LoadPhase.Loading
    val failed get() = phase == LoadPhase.Failed

    internal companion object {
        fun <Model> of(applied: ListState<Model>, change: LiveChange) = Snapshot(
            applied.rows, change.replacedIds(), applied.reordered, applied.edits, change.window?.let(::WindowEdges), change.seq,
            when (change.phase) {
                LivePhase.NOT_LOADED -> LoadPhase.NotLoaded
                LivePhase.LOADING -> LoadPhase.Loading
                LivePhase.LOADED -> LoadPhase.Loaded
                LivePhase.EMPTY -> LoadPhase.Empty
                LivePhase.FAILED -> LoadPhase.Failed
                LivePhase.CLOSED -> LoadPhase.Closed
                null -> LoadPhase.Opening
            },
            change.initial, change.refreshing, change.fetchingMore, change.hasMore, change.total, change.loadedAt,
            applied.failure ?: change.error?.toString(), if (applied.failure == null) change.errorKind else "invalid", applied.undecodable,
        )

        /** The newer delivery with the replacements of both: a chain A to B to C reads as C replaced A, and no pair is lost. */
        fun <Model> merging(older: Snapshot<Model>, newer: Snapshot<Model>): Snapshot<Model> {
            val replaced = older.replaced.toMutableMap()
            for ((real, temporary) in newer.replaced) {
                replaced[real] = older.replaced[temporary] ?: temporary
                if (older.replaced.containsKey(temporary)) replaced.remove(temporary)
            }
            return newer.copy(replaced = replaced, reordered = older.reordered || newer.reordered, edits = ListEdit.composing(older.edits, newer.edits, newer.rows.size), window = newer.window ?: older.window)
        }
    }
}

/** A live query for view models: the newest snapshot of the list and the calls that move it. The scope of the owner ends it. */
class LiveList<Model : DBModel>(private val query: Query<Model>, scope: kotlinx.coroutines.CoroutineScope, refreshOnConnect: Boolean = false) {
    private val initialSubscription = kotlinx.coroutines.CompletableDeferred<LiveSubscription>(scope.coroutineContext[kotlinx.coroutines.Job])
    private val mutableSnapshot = kotlinx.coroutines.flow.MutableStateFlow(Snapshot<Model>(emptyList(), emptyMap()))
    private val mutableRows = kotlinx.coroutines.flow.MutableStateFlow<List<Model>>(emptyList())
    private val mutableIds = kotlinx.coroutines.flow.MutableStateFlow<List<String>>(emptyList())
    private val mutableReplaced = kotlinx.coroutines.flow.MutableStateFlow<Map<String, String>>(emptyMap())
    private val updates = ListUpdates<Model>()

    init {
        val snapshots = query.snapshots(refreshOnConnect) { result ->
            result.fold({ initialSubscription.complete(it) }, { initialSubscription.completeExceptionally(it) })
        }
        scope.launch(kotlinx.coroutines.Dispatchers.Default) {
            try {
                snapshots.flow.collect { snapshot ->
                    updates.publish(snapshot)
                    if (snapshot.reordered) mutableIds.value = snapshot.rows.map { row -> row.id }
                    mutableReplaced.value = snapshot.replaced
                    mutableRows.value = snapshot.rows
                    mutableSnapshot.value = snapshot
                }
            } catch (error: kotlinx.coroutines.CancellationException) {
                throw error
            } catch (error: Exception) {
                mutableSnapshot.value = Snapshot(emptyList(), emptyMap(), phase = LoadPhase.Failed, error = error.message ?: error.toString(), errorKind = "subscription")
            }
        }
    }

    val snapshot: kotlinx.coroutines.flow.StateFlow<Snapshot<Model>> get() = mutableSnapshot
    val rows: kotlinx.coroutines.flow.StateFlow<List<Model>> get() = mutableRows
    val ids: kotlinx.coroutines.flow.StateFlow<List<String>> get() = mutableIds
    /** The ids that the last delivery of `rows` replaced: real id to temporary id. */
    val replaced: kotlinx.coroutines.flow.StateFlow<Map<String, String>> get() = mutableReplaced

    /** The newest rows with the edits since the last call, from one revision. The call never waits for a delivery that is being published. */
    fun takeUpdate(): ListUpdate<Model> = updates.take()

    suspend fun refresh() = query.refresh()
    suspend fun waitForInitialLoad() = initialSubscription.await().waitForInitialLoad()
    suspend fun loadMore(): LoadMoreResult = query.loadMore()
}

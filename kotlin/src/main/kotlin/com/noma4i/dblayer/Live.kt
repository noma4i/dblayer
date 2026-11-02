package com.noma4i.dblayer

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.launch

internal class ChangeListener(private val body: (LiveChange) -> Unit) : QueryListener {
    override fun changed(change: LiveChange) = body(change)
}

internal class Latest<Value>(private val merge: ((Value, Value) -> Value)?) {
    private val lock = Any()
    private var pending: Value? = null
    private var has = false
    private var version = 0
    private val wake = kotlinx.coroutines.channels.Channel<Unit>(kotlinx.coroutines.channels.Channel.CONFLATED)

    val hasPending: Boolean get() = synchronized(lock) { has }

    fun put(value: Value) {
        while (true) {
            var older: Value? = null
            var seen = 0
            var merging = false
            synchronized(lock) {
                if (has && merge != null) { merging = true; older = pending; seen = version }
                else { pending = value; has = true; version++ }
            }
            if (!merging) break
            @Suppress("UNCHECKED_CAST")
            val combined = merge!!.invoke(older as Value, value)
            val landed = synchronized(lock) {
                (version == seen).also { if (it) { pending = combined; has = true; version++ } }
            }
            if (landed) break
        }
        wake.trySend(Unit)
    }

    suspend fun next(): Value {
        while (true) {
            synchronized(lock) {
                if (has) {
                    has = false
                    version++
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

data class Snapshot<Model>(
    val rows: List<Model>,
    val replaced: Map<String, String>,
    val reordered: Boolean = false,
    val edits: List<ListEdit<Model>>? = null,
    val window: WindowEdges? = null,
    val seq: ULong = 0uL,
    val phase: LoadPhase = LoadPhase.Opening,
    val isInitialLoading: Boolean = false,
    val isRefreshing: Boolean = false,
    val isFetchingMore: Boolean = false,
    val hasMore: Boolean = false,
    val total: Long? = null,
    val loadedAt: Long? = null,
    val error: Throwable? = null,
    val errorKind: String? = null,
    val undecodable: List<String> = emptyList(),
    val complete: Boolean = false,
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
            applied.failure ?: change.error, if (applied.failure == null) change.errorKind else "invalid", applied.undecodable, change.complete && applied.failure == null,
        )

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

class LiveList<Model : DBModel> private constructor(
    private val reload: suspend () -> Unit,
    private val more: suspend () -> LoadMoreResult,
    scope: kotlinx.coroutines.CoroutineScope,
    open: ((Result<LiveSubscription>) -> Unit) -> LiveSource<Snapshot<Model>>,
) {
    constructor(query: Query<Model>, scope: kotlinx.coroutines.CoroutineScope, refreshOnConnect: Boolean = false) :
        this({ query.refresh() }, { query.loadMore() }, scope, { connected -> query.snapshots(refreshOnConnect, connected) })

    constructor(related: RelatedQuery<Model>, scope: kotlinx.coroutines.CoroutineScope, refreshOnConnect: Boolean = false) :
        this(related.refresh, related.loadMore, scope, { connected -> related.snapshots(refreshOnConnect, connected) })

    private val initialSubscription = kotlinx.coroutines.CompletableDeferred<LiveSubscription>(scope.coroutineContext[kotlinx.coroutines.Job])
    private val mutableSnapshot = kotlinx.coroutines.flow.MutableStateFlow(Snapshot<Model>(emptyList(), emptyMap()))
    private val mutableRows = kotlinx.coroutines.flow.MutableStateFlow<List<Model>>(emptyList())
    private val mutableIds = kotlinx.coroutines.flow.MutableStateFlow<List<String>>(emptyList())
    private val mutableReplaced = kotlinx.coroutines.flow.MutableStateFlow<Map<String, String>>(emptyMap())
    private val updates = ListUpdates<Model>()

    init {
        val snapshots = open { result ->
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
                mutableSnapshot.value = Snapshot(emptyList(), emptyMap(), phase = LoadPhase.Failed, error = error, errorKind = "subscription")
            }
        }
    }

    val snapshot: kotlinx.coroutines.flow.StateFlow<Snapshot<Model>> get() = mutableSnapshot
    val rows: kotlinx.coroutines.flow.StateFlow<List<Model>> get() = mutableRows
    val ids: kotlinx.coroutines.flow.StateFlow<List<String>> get() = mutableIds
    val replaced: kotlinx.coroutines.flow.StateFlow<Map<String, String>> get() = mutableReplaced

    fun takeUpdate(): ListUpdate<Model> = updates.take()

    suspend fun refresh() = reload()
    suspend fun waitForInitialLoad() = initialSubscription.await().waitForInitialLoad()
    suspend fun loadMore(): LoadMoreResult = more()
}

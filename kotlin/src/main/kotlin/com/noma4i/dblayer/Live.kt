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

    suspend fun current(): Value = runCatching { flow.firstOrNull() }.getOrNull() ?: initial
}

/** The rows of a delivery with the ids it replaced: the real id of a row that replaced a temporary one, to the temporary id. */
data class Snapshot<Model>(
    val rows: List<Model>,
    val replaced: Map<String, String>,
    val reordered: Boolean = false,
    val failure: String? = null,
    val edits: List<ListEdit<Model>>? = null,
    val window: WindowEdges? = null,
) {
    internal companion object {
        /** The newer rows with the replacements of both deliveries: a chain A to B to C reads as C replaced A, and no pair is lost. */
        fun <Model> merging(older: Snapshot<Model>, newer: Snapshot<Model>): Snapshot<Model> {
            val replaced = older.replaced.toMutableMap()
            for ((real, temporary) in newer.replaced) {
                replaced[real] = older.replaced[temporary] ?: temporary
                if (older.replaced.containsKey(temporary)) replaced.remove(temporary)
            }
            return Snapshot(newer.rows, replaced, older.reordered || newer.reordered, newer.failure ?: older.failure, ListEdit.composing(older.edits, newer.edits, newer.rows.size), newer.window ?: older.window)
        }
    }
}

enum class LoadPhase { Idle, Loading, Failed }

data class LoadState(
    val phase: LoadPhase = LoadPhase.Idle,
    /** The running load is the first one of a list that has no rows yet. */
    val isInitialLoading: Boolean = false,
    /** The running load reloads a list that already has rows. */
    val isRefreshing: Boolean = false,
    /** The running load fetches the next page. */
    val isFetchingMore: Boolean = false,
    val hasMore: Boolean = false,
    val total: Long? = null,
    val errorKind: String? = null,
    val error: String? = null,
    val loadedAt: Long? = null,
) {
    val isLoading get() = phase == LoadPhase.Loading
    val failed get() = phase == LoadPhase.Failed

    internal constructor(change: RelationStateChange) : this(
        when (change.phase) { "loading" -> LoadPhase.Loading; "failed" -> LoadPhase.Failed; else -> LoadPhase.Idle },
        change.initial, change.refreshing, change.fetchingMore,
        change.hasMore, change.total, change.errorKind, change.error, change.loadedAt,
    )
}

private class StateObserver(private val body: (RelationStateChange) -> Unit) : StateListener {
    override fun changed(state: RelationStateChange) = body(state)
}

internal fun stateSource(model: String, relation: String, paramsJson: String): LiveSource<LoadState> =
    LiveSource(LoadState()) { emit ->
        val subscription = DBlayer.database.liveState(model, relation, paramsJson, StateObserver { emit(LoadState(it)) })
        val cancel: () -> Unit = { subscription.cancel() }
        cancel
    }

internal class ListUpdate<Model : DBModel>(val snapshot: Snapshot<Model>? = null, val state: LoadState? = null) {
    companion object {
        fun <Model : DBModel> merging(older: ListUpdate<Model>, newer: ListUpdate<Model>) = ListUpdate(
            older.snapshot?.let { old -> newer.snapshot?.let { Snapshot.merging(old, it) } ?: old } ?: newer.snapshot,
            newer.state ?: older.state,
        )
    }
}

/** A live query for view models: the rows, the load state of the relation, and the calls that move them. The scope of the owner ends it. */
class LiveList<Model : DBModel>(private val query: Query<Model>, scope: kotlinx.coroutines.CoroutineScope, refreshOnConnect: Boolean = false) {
    private val initialSubscription = kotlinx.coroutines.CompletableDeferred<LiveSubscription>(scope.coroutineContext[kotlinx.coroutines.Job])
    private val mutableRows = kotlinx.coroutines.flow.MutableStateFlow<List<Model>>(emptyList())
    private val mutableIds = kotlinx.coroutines.flow.MutableStateFlow<List<String>>(emptyList())
    private val mutableFailure = kotlinx.coroutines.flow.MutableStateFlow<String?>(null)
    private val mutableReplaced = kotlinx.coroutines.flow.MutableStateFlow<Map<String, String>>(emptyMap())
    private val mutableState = kotlinx.coroutines.flow.MutableStateFlow(LoadState())
    private var pending: List<ListEdit<Model>>? = null

    init {
        val snapshots = query.snapshots(refreshOnConnect) { result ->
            result.fold({ initialSubscription.complete(it) }, { initialSubscription.completeExceptionally(it) })
        }
        val state = query.state
        val source = LiveSource(ListUpdate<Model>(), { older, newer -> ListUpdate.merging(older, newer) }) { emit ->
            val rows = snapshots.subscribe { emit(ListUpdate(snapshot = it)) }
            val load = runCatching { state.subscribe { emit(ListUpdate(state = it)) } }.getOrNull()
            val cancel: () -> Unit = { rows(); load?.invoke() }
            cancel
        }
        scope.launch {
            try {
                source.flow.collect { update ->
                    update.snapshot?.let { synchronized(this@LiveList) { pending = ListEdit.composing(pending, it.edits, it.rows.size) }; mutableReplaced.value = it.replaced; mutableFailure.value = it.failure; mutableRows.value = it.rows; if (it.reordered) mutableIds.value = it.rows.map { row -> row.id } }
                    update.state?.let { mutableState.value = it }
                }
            } catch (error: IllegalStateException) {
                mutableFailure.value = error.message
            }
        }
    }

    val rows: kotlinx.coroutines.flow.StateFlow<List<Model>> get() = mutableRows
    val ids: kotlinx.coroutines.flow.StateFlow<List<String>> get() = mutableIds
    /** The ids that the last delivery of `rows` replaced: real id to temporary id. */
    val replaced: kotlinx.coroutines.flow.StateFlow<Map<String, String>> get() = mutableReplaced
    val failure: kotlinx.coroutines.flow.StateFlow<String?> get() = mutableFailure
    val state: kotlinx.coroutines.flow.StateFlow<LoadState> get() = mutableState

    fun takeEdits(): List<ListEdit<Model>>? = synchronized(this) { pending.also { pending = emptyList() } }

    suspend fun refresh() = query.refresh()
    suspend fun waitForInitialLoad() = initialSubscription.await().waitForInitialLoad()
    suspend fun loadMore(): Boolean = query.loadMore()
}

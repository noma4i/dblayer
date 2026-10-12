package com.noma4i.dblayer

import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.launch
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

private class ChangeListener(private val body: (LiveChange) -> Unit) : QueryListener {
    override fun changed(change: LiveChange) = body(change)
}

private val decodeLog = java.util.logging.Logger.getLogger("DBlayer")

internal fun <Model> LiveChange.decode(serializer: KSerializer<Model>): List<Model> =
    rows.mapNotNull { row ->
        runCatching { dbJson.decodeFromString(serializer, row) }
            .onFailure { decodeLog.severe("Row of ${serializer.descriptor.serialName} failed to decode: $it") }
            .getOrNull()
    }

class LiveSource<Value>(
    val initial: Value,
    private val subscribe: ((Value) -> Unit) -> () -> Unit,
) {
    constructor(initial: Value, attach: (QueryListener) -> LiveSubscription, map: (LiveChange) -> Value) : this(initial, { emit ->
        val subscription = attach(ChangeListener { emit(map(it)) })
        val cancel: () -> Unit = { subscription.cancel() }
        cancel
    })

    val flow: Flow<Value>
        get() = callbackFlow {
            val cancel = runCatching { subscribe { trySend(it) } }.getOrNull()
            if (cancel == null) close()
            awaitClose { cancel?.invoke() }
        }.flowOn(kotlinx.coroutines.Dispatchers.Default)

    suspend fun current(): Value = flow.firstOrNull() ?: initial
}

/** The rows of a delivery with the ids it replaced: the real id of a row that replaced a temporary one, to the temporary id. */
data class Snapshot<Model>(val rows: List<Model>, val replaced: Map<String, String>)

internal fun LiveChange.replacedIds(): Map<String, String> =
    runCatching {
        dbJson.parseToJsonElement(diffJson).jsonObject["replaced"]?.jsonArray
            ?.associate { pair -> pair.jsonArray[1].jsonPrimitive.content to pair.jsonArray[0].jsonPrimitive.content }
    }.getOrNull() ?: emptyMap()

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

/** A live query for view models: the rows, the load state of the relation, and the calls that move them. */
class LiveList<Model : DBModel>(private val query: Query<Model>, scope: kotlinx.coroutines.CoroutineScope) {
    private val mutableRows = kotlinx.coroutines.flow.MutableStateFlow<List<Model>>(emptyList())
    private val mutableReplaced = kotlinx.coroutines.flow.MutableStateFlow<Map<String, String>>(emptyMap())
    private val mutableState = kotlinx.coroutines.flow.MutableStateFlow(LoadState())
    private val jobs = listOf(
        scope.launch { query.snapshots.flow.collect { mutableReplaced.value = it.replaced; mutableRows.value = it.rows } },
        scope.launch { query.state.flow.collect { mutableState.value = it } },
    )

    val rows: kotlinx.coroutines.flow.StateFlow<List<Model>> get() = mutableRows
    /** The ids that the last delivery of `rows` replaced: real id to temporary id. */
    val replaced: kotlinx.coroutines.flow.StateFlow<Map<String, String>> get() = mutableReplaced
    val state: kotlinx.coroutines.flow.StateFlow<LoadState> get() = mutableState

    suspend fun refresh() = query.refresh()
    suspend fun loadMore(): Boolean = query.loadMore()
    fun stop() = jobs.forEach { it.cancel() }
}

/** Live rows of one model looked up by id, for the records a list refers to (authors, last messages). */
class LiveRows<Model : DBModel>(private val idsQuery: (List<String>) -> Query<Model>, private val scope: kotlinx.coroutines.CoroutineScope) {
    private val mutableById = kotlinx.coroutines.flow.MutableStateFlow<Map<String, Model>>(emptyMap())
    private var ids: List<String> = emptyList()
    private var job: kotlinx.coroutines.Job? = null

    val byId: kotlinx.coroutines.flow.StateFlow<Map<String, Model>> get() = mutableById

    fun track(nextIds: List<String>) {
        val unique = nextIds.toSet().sorted()
        if (unique == ids) return
        ids = unique
        job?.cancel()
        if (unique.isEmpty()) {
            mutableById.value = emptyMap()
            return
        }
        job = scope.launch { idsQuery(unique).live.flow.collect { rows -> mutableById.value = rows.associateBy { it.id } } }
    }

    fun stop() {
        job?.cancel()
        job = null
    }
}

package com.noma4i.dblayer

import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.serialization.KSerializer

private class ChangeListener(private val body: (LiveChange) -> Unit) : QueryListener {
    override fun changed(change: LiveChange) = body(change)
}

internal fun <Model> LiveChange.decode(serializer: KSerializer<Model>): List<Model> =
    rows.mapNotNull { runCatching { dbJson.decodeFromString(serializer, it) }.getOrNull() }

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
            val cancel = subscribe { trySend(it) }
            awaitClose { cancel() }
        }

    suspend fun current(): Value = flow.first()
}

enum class LoadPhase { Idle, Loading, Failed }

data class LoadState(
    val phase: LoadPhase = LoadPhase.Idle,
    val hasMore: Boolean = false,
    val errorKind: String? = null,
    val error: String? = null,
    val loadedAt: Long? = null,
) {
    val isLoading get() = phase == LoadPhase.Loading
    val failed get() = phase == LoadPhase.Failed

    internal constructor(change: RelationStateChange) : this(
        when (change.phase) { "loading" -> LoadPhase.Loading; "failed" -> LoadPhase.Failed; else -> LoadPhase.Idle },
        change.hasMore, change.errorKind, change.error, change.loadedAt,
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
    private val mutableState = kotlinx.coroutines.flow.MutableStateFlow(LoadState())
    private val jobs = listOf(
        scope.launch { query.live.flow.collect { mutableRows.value = it } },
        scope.launch { query.state.flow.collect { mutableState.value = it } },
    )

    val rows: kotlinx.coroutines.flow.StateFlow<List<Model>> get() = mutableRows
    val state: kotlinx.coroutines.flow.StateFlow<LoadState> get() = mutableState

    suspend fun refresh() = query.refresh()
    suspend fun loadMore(): Boolean = query.loadMore()
    fun stop() = jobs.forEach { it.cancel() }
}

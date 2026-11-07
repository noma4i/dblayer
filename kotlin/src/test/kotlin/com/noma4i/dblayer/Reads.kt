package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn

object LoadPhase {
    val Opening = ReadPhase.IDLE
    val Loaded = ReadPhase.LOADED
    val Failed = ReadPhase.FAILED
}

/** The first load ended and the relation has no next page. */
val ReadState.complete: Boolean get() = settled && !hasMore && error == null

class Snapshot<M : DBModel>(
    val rows: List<M>,
    val phase: ReadPhase?,
    val error: DbException?,
    val total: Long?,
    val hasMore: Boolean,
    val complete: Boolean,
    val loaded: Long?,
)

suspend fun <M : DBModel> Query<M>.current(): List<M> = read()

class TestRow<M : DBModel>(private val decoder: ModelDecoder<M>, private val id: String) {
    suspend fun current(): M? = Query(decoder.modelName, decoder).readRow(id)
}

fun <M : DBModel> ModelDecoder<M>.find(id: String) = TestRow(this, id)

class TestCount<M : DBModel>(private val query: Query<M>) {
    suspend fun current(): Long = query.current().size.toLong()
}

val <M : DBModel> Query<M>.count: TestCount<M> get() = TestCount(this)

class TestSnapshots<M : DBModel>(private val query: Query<M>) {
    val flow: Flow<Snapshot<M>> = flow {
        val stream = AccountScope.current().stream(query)
        var projection = ListProjection<M>()
        stream.values.collect { value ->
            val next = projection.apply(value, query.decoder)
            if (next != null) {
                projection = next
                val state = projection.state
                emit(Snapshot(projection.rows, projection.phase, state?.error, state?.total?.toLong(), state?.hasMore ?: false, state?.complete ?: false, state?.loaded))
            }
        }
    }
}

class TestLive<M : DBModel>(private val query: Query<M>) {
    val flow: Flow<List<M>> = TestSnapshots(query).flow.map { it.rows }
    suspend fun current(): List<M> = query.current()
}

val <M : DBModel> Query<M>.live: TestLive<M> get() = TestLive(this)
val <M : DBModel> Query<M>.snapshots: TestSnapshots<M> get() = TestSnapshots(this)

class ListReader<M : DBModel>(private val query: Query<M>, scope: CoroutineScope, refreshOnConnect: Boolean = false) {
    private val projected = Projected(AccountScope.current().stream(query, refreshOnConnect), query.decoder, scope)
    val rows: StateFlow<List<M>> = projected.projection.map { it.rows }.stateIn(scope, SharingStarted.Eagerly, emptyList())
    val ids: Flow<List<String>> = rows.map { list -> list.map { it.id } }
    val snapshot: StateFlow<Snapshot<M>> = projected.projection.map { projection ->
        val state = projection.state
        Snapshot(projection.rows, projection.phase, state?.error, state?.total?.toLong(), state?.hasMore ?: false, state?.complete ?: false, state?.loaded)
    }.stateIn(scope, SharingStarted.Eagerly, Snapshot(emptyList(), null, null, null, false, false, null))

    suspend fun waitForInitialLoad() = projected.loaded()
    suspend fun refresh() = projected.refresh()
    suspend fun loadMore(): LoadMoreResult = projected.loadMore()
    suspend fun loadAll(force: Boolean = false) = query.loadAll(force)
}

fun heldUser(): Projected<User> = Projected(AccountScope.current().stream(User.profile), User, CoroutineScope(SupervisorJob()))

/** Returns when the projection shows a settled state, throws the failed state's error or `Interrupted` when the stream ended. Bounded by the hang guard. */
suspend fun <M : DBModel> Projected<M>.loaded() {
    val shown = awaitWithin("the projection shows a settled state") { projection.first { it.state?.settled == true || it.ended != null } }
    if (shown.ended != null && shown.ended != ReadEnd.Invalid) throw DbException.Interrupted("the stream ended before its rows were shown")
    val state = shown.state
    if (state?.settled == true) {
        val error = state.error ?: return
        if (state.phase == ReadPhase.FAILED) throw error
        return
    }
    throw DbException.Interrupted("the stream ended before its rows were shown")
}

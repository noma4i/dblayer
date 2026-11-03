package com.noma4i.dblayer

import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.remember
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonObject

/** The deliveries of one stream in order. Collecting it reads the stream, and a collector that stops or is cancelled ends the stream in the core. A stream that ends by itself delivers one `StreamValue.Ended` last. */
class ValueStream internal constructor(val id: ULong, val epoch: ULong, private val database: Database, private val channel: Channel<StreamDelivery>) {
    val deliveries: Flow<StreamDelivery> = flow {
        try {
            for (delivery in channel) {
                emit(delivery)
                if (delivery.value is StreamValue.Ended) break
            }
        } finally {
            close()
        }
    }

    /** The projection could not apply a delta. The core sends one new snapshot. */
    fun resync() = database.resyncStream(id)

    /** Loads the relation of the stream again. Throws `Interrupted` when the session of the stream ended. */
    suspend fun refresh() = offCaller { database.refreshStream(id) }

    suspend fun loadMore(): LoadMoreResult = offCaller { database.loadMoreStream(id) }

    /** Returns when the first load of the stream settled. */
    suspend fun loaded() = offCaller { database.streamLoaded(id) }

    /** Stops reading: the stream ends in the core. */
    fun close() {
        channel.close()
        database.cancelStream(id)
    }
}

private class Forward(private val channel: Channel<StreamDelivery>) : ValueSink {
    override fun delivered(delivery: StreamDelivery) {
        channel.trySend(delivery)
        if (delivery.value is StreamValue.Ended) channel.close()
    }
}

internal fun Database.openValueStream(epoch: ULong, request: StreamRequest): ValueStream {
    val channel = Channel<StreamDelivery>(Channel.UNLIMITED)
    val id = subscribe(epoch, request, Forward(channel))
    return ValueStream(id, epoch, this, channel)
}

enum class Applied { Changed, Resync, Ignored }

/** What a screen shows of a stream: an immutable value built only from deliveries. It holds no lock and no reference to the core. */
class ListProjection<Model : DBModel> private constructor(
    val rows: List<Model>,
    val state: StreamState?,
    val revision: Long?,
    val epoch: ULong?,
    val ended: StreamEnd?,
    val failure: String?,
    /** How many rows were decoded so far. A change of one row decodes one row. */
    val decoded: Int,
    /** How many deltas were applied. A delta decodes the rows it carries and no other. */
    val deltas: Int,
) {
    constructor() : this(emptyList(), null, null, null, null, null, 0, 0)

    val phase: LivePhase? get() = state?.phase

    class Step<Model : DBModel>(val projection: ListProjection<Model>, val applied: Applied)

    fun apply(delivery: StreamDelivery, decoder: ModelDecoder<Model>): Step<Model> {
        if (epoch != null && delivery.epoch != epoch) return Step(this, Applied.Ignored)
        val held = delivery.epoch
        var decoded = this.decoded
        var failure = this.failure
        fun decode(wire: List<WireRow>): List<Model?> = wire.map { row ->
            try {
                decoder.read(row).also { decoded += 1 }
            } catch (error: Exception) {
                failure = error.toString()
                null
            }
        }
        return when (val value = delivery.value) {
            is StreamValue.Snapshot -> Step(ListProjection(decode(value.rows).filterNotNull(), value.state, value.revision, held, ended, failure, decoded, deltas), Applied.Changed)
            is StreamValue.State -> Step(ListProjection(rows, value.state, revision, held, ended, this.failure, decoded, deltas), Applied.Changed)
            is StreamValue.Ended -> Step(ListProjection(rows, state, revision, held, value.reason, this.failure, decoded, deltas), Applied.Changed)
            is StreamValue.Delta -> {
                if (value.base != revision) return Step(this, Applied.Resync)
                val carried = decode(value.rows)
                if (carried.any { it == null }) return Step(ListProjection(rows, state, revision, held, ended, failure, decoded, deltas), Applied.Resync)
                val next = rebuild(rows, carried.filterNotNull().zip(value.rowIndexes.map { it.toInt() }), value.removed, value.placed, value.replaced, value.size.toInt())
                    ?: return Step(ListProjection(rows, state, revision, held, ended, this.failure, decoded, deltas), Applied.Resync)
                Step(ListProjection(next, value.state, value.revision, held, ended, this.failure, decoded, deltas + 1), Applied.Changed)
            }
        }
    }

    private fun rebuild(current: List<Model>, carried: List<Pair<Model, Int>>, removed: List<String>, placed: List<Placement>, replaced: List<Replacement>, size: Int): List<Model>? {
        val work = current.toMutableList()
        val byId = carried.associate { (model, _) -> model.id to model }
        val landed = HashSet<String>()
        for (pair in replaced) {
            val at = work.indexOfFirst { it.id == pair.from }
            val model = byId[pair.to]
            if (at < 0 || model == null) return null
            work[at] = model
            landed.add(pair.to)
        }
        if (removed.isNotEmpty()) {
            val gone = removed.toSet()
            if (work.count { it.id in gone } != gone.size) return null
            work.removeAll { it.id in gone }
        }
        val taken = HashMap<String, Model>()
        for (place in placed) {
            val at = work.indexOfFirst { it.id == place.id }
            if (at >= 0) taken[place.id] = work.removeAt(at)
        }
        for (place in placed.sortedBy { it.index }) {
            val model = taken[place.id] ?: byId[place.id]
            if (model == null || place.index.toInt() > work.size) return null
            work.add(place.index.toInt(), model)
            if (taken[place.id] == null) landed.add(place.id)
        }
        for ((model, index) in carried) {
            if (index >= work.size) return null
            if (model.id in landed && work[index].id == model.id) continue
            work[index] = model
        }
        return if (work.size == size) work else null
    }
}

/** The screen model of a stream: a StateFlow over a projection. It reads the stream until its scope completes, and the completion ends the stream in the core. */
class Projected<Model : DBModel>(val stream: ValueStream, decoder: ModelDecoder<Model>, scope: CoroutineScope) {
    private val held = MutableStateFlow(ListProjection<Model>())
    val projection: StateFlow<ListProjection<Model>> = held.asStateFlow()
    private val reading: Job = scope.launch {
        stream.deliveries.collect { delivery ->
            val step = held.value.apply(delivery, decoder)
            held.value = step.projection
            if (step.applied == Applied.Resync) stream.resync()
        }
    }

    val value: Model? get() = held.value.rows.firstOrNull()

    suspend fun refresh() = stream.refresh()

    suspend fun loadMore(): LoadMoreResult = stream.loadMore()

    suspend fun loaded() = stream.loaded()

    fun close() {
        reading.cancel()
        stream.close()
    }
}

/** The outcome of a durable command, or the row it wrote. */
class Sent<Model : DBModel> internal constructor(val row: Model, private val receipt: CommandReceipt, private val database: Database) {
    fun status(): CommandStatus = database.commandStatus(receipt)

    /** Waits for the one outcome: delivered, failed, or ended with the session. */
    suspend fun outcome(): CommandStatus = offCaller { database.commandOutcome(receipt) }
}

/** The streams and commands of one signed-in account. The root of the app makes one after the sign-in and recreates its screens when the account changes: a scope whose session ended opens streams that end at once. */
class AccountScope private constructor(val epoch: ULong, private val database: Database) {
    fun <Model : DBModel> stream(query: Query<Model>): ValueStream = database.openValueStream(epoch, StreamRequest.Query(query.json))

    fun <Model : DBModel> entity(decoder: ModelDecoder<Model>, id: String): ValueStream = database.openValueStream(epoch, StreamRequest.Entity(decoder.modelName, id))

    /** Writes the model locally and queues it for the server. It works without a network and after the app closed. Throws `Interrupted` when the session of this scope ended. */
    suspend fun <Model : DBModel> send(decoder: ModelDecoder<Model>, values: JsonObject, args: JsonObject = JsonObject(emptyMap())): Sent<Model> {
        val receipt = offCaller { database.send(CommandRequest(epoch, decoder.modelName, values.jsonText(), args.jsonText())) }
        return Sent(decoder.read(receipt.row), receipt, database)
    }

    companion object {
        fun current(): AccountScope {
            val database = DBlayer.database
            return AccountScope(database.sessionEpoch(), database)
        }
    }
}

@Composable
fun <Model : DBModel> rememberProjected(account: AccountScope, query: Query<Model>): Projected<Model> {
    val scope = remember(account.epoch, query.json) { CoroutineScope(kotlinx.coroutines.SupervisorJob() + kotlinx.coroutines.Dispatchers.Default) }
    val projected = remember(scope) { Projected(account.stream(query), query.decoder, scope) }
    DisposableEffect(projected) { onDispose { projected.close(); scope.coroutineContext[Job]?.cancel() } }
    return projected
}

fun streamCensus(): StreamCensus = DBlayer.database.streamCensus()

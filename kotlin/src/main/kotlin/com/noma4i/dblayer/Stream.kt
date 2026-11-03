package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
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

/** What a screen shows of a stream of rows: an immutable value built only from deliveries. It holds no lock and no reference to the core. */
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
    /** The steps of the last delivery from the rows before to the rows after. `null` after a snapshot: the screen reads the whole list. */
    val lastEdits: List<ListEdit<Model>>?,
    /** Server id to the temporary id the row had when the screen first showed it. */
    val temporaryIds: Map<String, String>,
    private val awaitingSnapshot: Boolean,
) {
    constructor() : this(emptyList(), null, null, null, null, null, 0, 0, emptyList(), emptyMap(), false)

    val phase: LivePhase? get() = state?.phase

    class Step<Model : DBModel>(val projection: ListProjection<Model>, val applied: Applied)

    private fun copy(
        rows: List<Model> = this.rows, state: StreamState? = this.state, revision: Long? = this.revision, epoch: ULong? = this.epoch, ended: StreamEnd? = this.ended, failure: String? = this.failure,
        decoded: Int = this.decoded, deltas: Int = this.deltas, lastEdits: List<ListEdit<Model>>? = this.lastEdits, temporaryIds: Map<String, String> = this.temporaryIds, awaitingSnapshot: Boolean = this.awaitingSnapshot,
    ) = ListProjection(rows, state, revision, epoch, ended, failure, decoded, deltas, lastEdits, temporaryIds, awaitingSnapshot)

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
            is StreamValue.Snapshot -> Step(copy(rows = decode(value.rows).filterNotNull(), state = value.state, revision = value.revision, epoch = held, failure = failure, decoded = decoded, lastEdits = null, awaitingSnapshot = false), Applied.Changed)
            is StreamValue.State -> Step(copy(state = value.state, epoch = held, lastEdits = emptyList()), Applied.Changed)
            is StreamValue.Ended -> Step(copy(ended = value.reason, epoch = held, lastEdits = emptyList()), Applied.Changed)
            is StreamValue.Delta -> {
                if (awaitingSnapshot) return Step(this, Applied.Ignored)
                val gap = Step(copy(epoch = held, failure = failure, decoded = decoded, awaitingSnapshot = true), Applied.Resync)
                if (value.base != revision) return gap
                val carried = decode(value.rows)
                if (carried.any { it == null }) return gap
                val result = rebuild(carried.filterNotNull().zip(value.rowIndexes.map { it.toInt() }), value.removed, value.placed, value.replaced, value.size.toInt()) ?: return gap
                val ids = HashMap(temporaryIds)
                for (pair in value.renamed) { ids[pair.to] = ids[pair.from] ?: pair.from; ids.remove(pair.from) }
                Step(copy(rows = result.first, state = value.state, revision = value.revision, epoch = held, failure = failure, decoded = decoded, deltas = deltas + 1, lastEdits = result.second.takeIf { it.size <= result.first.size }, temporaryIds = ids), Applied.Changed)
            }
            else -> Step(this, Applied.Ignored)
        }
    }

    private fun rebuild(carried: List<Pair<Model, Int>>, removed: List<String>, placed: List<Placement>, replaced: List<Replacement>, size: Int): Pair<List<Model>, List<ListEdit<Model>>>? {
        if (removed.isEmpty() && placed.isEmpty() && replaced.isEmpty()) {
            if (size != rows.size || carried.any { (model, index) -> index >= rows.size || rows[index].id != model.id }) return null
            val next = rows.toMutableList()
            for ((model, index) in carried) next[index] = model
            return next to carried.map { (model, index) -> ListEdit.Rewrite(index, model) }
        }
        val work = rows.toMutableList()
        val edits = ArrayList<ListEdit<Model>>()
        val byId = carried.associate { (model, _) -> model.id to model }
        val landed = HashSet<String>()
        for (pair in replaced) {
            val at = work.indexOfFirst { it.id == pair.from }
            val model = byId[pair.to]
            if (at < 0 || model == null) return null
            work[at] = model
            edits.add(ListEdit.Rewrite(at, model))
            landed.add(pair.to)
        }
        if (removed.isNotEmpty()) {
            val gone = removed.toSet()
            val found = work.indices.filter { work[it].id in gone }
            if (found.size != gone.size) return null
            for (index in found.reversed()) {
                edits.add(ListEdit.Remove(index, work[index].id))
                work.removeAt(index)
            }
        }
        val taken = HashMap<String, Model>()
        for (place in placed) {
            val at = work.indexOfFirst { it.id == place.id }
            if (at >= 0) {
                taken[place.id] = work[at]
                edits.add(ListEdit.Remove(at, place.id))
                work.removeAt(at)
            }
        }
        for (place in placed.sortedBy { it.index }) {
            val model = taken[place.id] ?: byId[place.id]
            if (model == null || place.index.toInt() > work.size) return null
            work.add(place.index.toInt(), model)
            edits.add(ListEdit.Insert(place.index.toInt(), model))
            if (taken[place.id] == null) landed.add(place.id)
        }
        for ((model, index) in carried) {
            if (index >= work.size) return null
            if (model.id in landed && work[index].id == model.id) continue
            work[index] = model
            edits.add(ListEdit.Rewrite(index, model))
        }
        return if (work.size == size) work to edits else null
    }
}

/** The screen model of a stream of rows: a StateFlow over a projection. It reads the stream until its scope completes, and the completion of the reading job ends the stream in the core, also when the scope was cancelled before the first delivery. */
class Projected<Model : DBModel>(val stream: ValueStream, decoder: ModelDecoder<Model>, scope: CoroutineScope) {
    private val held = MutableStateFlow(ListProjection<Model>())
    val projection: StateFlow<ListProjection<Model>> = held.asStateFlow()
    private val pending = java.util.concurrent.atomic.AtomicReference<List<ListEdit<Model>>?>(emptyList())
    private val reading: Job = scope.launch(start = CoroutineStart.ATOMIC) {
        stream.deliveries.collect { delivery ->
            val step = held.value.apply(delivery, decoder)
            held.value = step.projection
            if (step.applied == Applied.Resync) stream.resync()
            if (step.applied == Applied.Changed) pending.updateAndGet { ListEdit.composing(it, step.projection.lastEdits, step.projection.rows.size) }
        }
    }.also { it.invokeOnCompletion { stream.close() } }

    val value: Model? get() = held.value.rows.firstOrNull()

    /** The steps since the last call, to apply to a list the screen keeps. `null`: the screen reads the rows again. */
    fun takeEdits(): List<ListEdit<Model>>? = pending.getAndSet(emptyList())

    suspend fun refresh() = stream.refresh()

    suspend fun loadMore(): LoadMoreResult = stream.loadMore()

    suspend fun loaded() = stream.loaded()

    fun close() {
        reading.cancel()
        stream.close()
    }
}

/** The screen model of a count stream. */
class ProjectedCount(val stream: ValueStream, scope: CoroutineScope) {
    private val held = MutableStateFlow<Long?>(null)
    val count: StateFlow<Long?> = held.asStateFlow()
    private val reading: Job = scope.launch(start = CoroutineStart.ATOMIC) {
        stream.deliveries.collect { delivery -> (delivery.value as? StreamValue.Count)?.let { held.value = it.count } }
    }.also { it.invokeOnCompletion { stream.close() } }

    fun close() {
        reading.cancel()
        stream.close()
    }
}

/** The events of a model after the stream opened. The flow ends when the collector stops or the session of the account ends. */
fun <Model : DBModel> ValueStream.events(decoder: ModelDecoder<Model>): Flow<ModelEventItem<Model>> = flow {
    deliveries.collect { delivery ->
        when (val value = delivery.value) {
            is StreamValue.Missed -> emit(ModelEventItem.Missed(value.count))
            is StreamValue.Event -> try {
                emit(ModelEventItem.Event(ModelEvent(value.event.area, value.event.kind, value.event.id, value.event.before?.let(decoder::read), value.event.after?.let(decoder::read))))
            } catch (error: kotlinx.coroutines.CancellationException) { throw error } catch (error: Throwable) {
                emit(ModelEventItem.Failed(error))
                close()
            }
            else -> {}
        }
    }
}

/** The status of a media upload while it runs. */
val ValueStream.uploadStatus: Flow<WireBlobSnapshot>
    get() = flow { deliveries.collect { delivery -> (delivery.value as? StreamValue.Status)?.let { emit(it.blob) } } }

/** The file of a media command and the declared upload action that sends it. */
class UploadCommand(val action: String, val request: UploadRequest)

/** The outcome of a durable command, or the row it wrote. */
class Sent<Model : DBModel> internal constructor(val row: Model, private val receipt: CommandReceipt, private val database: Database) {
    fun status(): CommandStatus = database.commandStatus(receipt)

    /** Waits for the one outcome: delivered, failed, or ended with the session. It waits while the network is away. */
    suspend fun outcome(): CommandStatus = offCaller { database.commandOutcome(receipt) }
}

/** The streams and commands of one signed-in account. The root of the app makes one after the sign-in and recreates its screens when the account changes: a scope whose session ended opens streams that end at once. */
class AccountScope private constructor(val epoch: ULong, private val database: Database) {
    /** [refreshOnConnect] loads the relation again when the stream opens, also when it is fresh. The first rows stay visible. */
    fun <Model : DBModel> stream(query: Query<Model>, refreshOnConnect: Boolean = false): ValueStream = database.openValueStream(epoch, StreamRequest.Query(query.json(refreshOnConnect)))

    fun <Model : DBModel> count(query: Query<Model>): ValueStream = database.openValueStream(epoch, StreamRequest.Count(query.json))

    fun <Model : DBModel, Target : DBModel> related(query: Query<Model>, association: Association<Model, Target>): ValueStream = database.openValueStream(epoch, StreamRequest.Related(query.json, association.name))

    /** One row by id. [fetch] tells when the library loads it from the server. */
    fun <Model : DBModel> entity(decoder: ModelDecoder<Model>, id: String, fetch: EntityFetch = EntityFetch.NONE): ValueStream = database.openValueStream(epoch, StreamRequest.Entity(decoder.modelName, id, fetch))

    fun <Model : DBModel> events(decoder: ModelDecoder<Model>): ValueStream = database.openValueStream(epoch, StreamRequest.Events(decoder.modelName))

    fun uploadStatus(id: String): ValueStream = database.openValueStream(epoch, StreamRequest.Blob(id))

    /** Writes the model locally and queues it for the server. It works without a network and after the app closed. Only a durable create action (sending a message) and a declared upload action work this way; another model is refused. Throws `Interrupted` when the session of this scope ended. */
    suspend fun <Model : DBModel> send(decoder: ModelDecoder<Model>, values: JsonObject, args: JsonObject = JsonObject(emptyMap()), upload: UploadCommand? = null): Sent<Model> {
        val receipt = offCaller { database.send(CommandRequest(epoch, decoder.modelName, values.jsonText(), args.jsonText(), upload?.let { CommandUpload(it.action, it.request.wire()) })) }
        return Sent(decoder.read(receipt.row), receipt, database)
    }

    /** Sends the file of an existing record through its declared upload action. */
    suspend fun <Model : DBModel, Answer> upload(row: Model, action: String, request: UploadRequest, answerSerializer: kotlinx.serialization.KSerializer<Answer>): ActionAnswer<Model, Answer> {
        val operation = offCaller { database.upload(epoch, row.modelName, row.id, action, request.wire()) }
        return ActionAnswer(row, operation, answerSerializer)
    }

    suspend fun <Model : DBModel> uploadBatch(decoder: ModelDecoder<Model>, items: List<UploadItem>, request: UploadBatchRequest): List<Model> =
        offCaller { database.uploadCreateBatch(epoch, decoder.modelName, items.map { it.wire() }, request.wire()).map { decoder.read(it) } }

    companion object {
        fun current(): AccountScope {
            val database = DBlayer.database
            return AccountScope(database.sessionEpoch(), database)
        }
    }
}

fun streamCensus(): StreamCensus = DBlayer.database.streamCensus()

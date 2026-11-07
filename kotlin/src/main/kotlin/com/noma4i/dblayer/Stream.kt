package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonObject

/** The values of one stream in order, taken from the core one by one. Collecting it reads the stream, and a collector that stops or is cancelled ends the stream in the core. A stream that ends by itself delivers one `ReadValue.Ended` last. */
class ValueStream internal constructor(val epoch: ULong, private val reader: ReadStream, private val database: Database, private val load: LoadTarget?) {
    internal class LoadTarget(val model: String, val relation: String, val paramsJson: String)

    val values: Flow<ReadValue> = flow {
        while (true) {
            val value = reader.pull() ?: break
            emit(value)
            if (value is ReadValue.Ended) break
        }
    }

    /** Loads the relation of the stream again. Throws `Interrupted` when the session of the stream ended. */
    suspend fun refresh() {
        val target = requireTarget()
        offCaller { database.refresh(epoch, target.model, target.relation, target.paramsJson) }
    }

    suspend fun loadMore(): LoadMoreResult {
        val target = requireTarget()
        return offCaller { database.loadMore(epoch, target.model, target.relation, target.paramsJson) }
    }

    /** Stops reading: the stream ends in the core and nothing is delivered after this returns. */
    suspend fun cancel() = reader.cancel()

    fun close() = reader.close()

    private fun requireTarget(): LoadTarget = load ?: throw DbException.Invalid("the stream has no relation to load")
}

internal fun Database.openValueStream(epoch: ULong, request: ReadRequest, load: ValueStream.LoadTarget? = null): ValueStream =
    ValueStream(epoch, subscribeValues(epoch, request), this, load)

enum class Applied { Changed, Resync, Ignored }

/** What a screen shows of a stream of rows: an immutable value built only from deliveries. It holds no lock and no reference to the core. */
class ListProjection<Model : DBModel> private constructor(
    val rows: List<Model>,
    val state: ReadState?,
    val revision: Long?,
    val ended: ReadEnd?,
    val failure: String?,
    /** How many rows were decoded so far. A change of one row decodes one row. */
    val decoded: Int,
    /** How many deltas were applied. A delta decodes the rows it carries and no other. */
    val deltas: Int,
    /** The steps of the last delivery from the rows before to the rows after. `null` after a snapshot: the screen reads the whole list. */
    val lastEdits: List<ListEdit<Model>>?,
    /** Server id to the temporary id the row had when the screen first showed it. */
    val temporaryIds: Map<String, String>,
) {
    constructor() : this(emptyList(), null, null, null, null, 0, 0, emptyList(), emptyMap())

    val phase: ReadPhase? get() = state?.phase

    /** The error the screen shows: the one the core reports for the stream, else a row the platform could not decode. */
    val error: DbException? get() = state?.error ?: failure?.let { DbException.Invalid(it) }

    class Step<Model : DBModel>(val projection: ListProjection<Model>, val applied: Applied)

    private fun copy(
        rows: List<Model> = this.rows, state: ReadState? = this.state, revision: Long? = this.revision, ended: ReadEnd? = this.ended, failure: String? = this.failure,
        decoded: Int = this.decoded, deltas: Int = this.deltas, lastEdits: List<ListEdit<Model>>? = this.lastEdits, temporaryIds: Map<String, String> = this.temporaryIds,
    ) = ListProjection(rows, state, revision, ended, failure, decoded, deltas, lastEdits, temporaryIds)

    fun apply(value: ReadValue, decoder: ModelDecoder<Model>): Step<Model> {
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
        return when (value) {
            is ReadValue.Snapshot -> {
                failure = null
                val whole = decode(value.rows).filterNotNull()
                Step(copy(rows = whole, state = value.state, revision = value.revision, failure = failure, decoded = decoded, lastEdits = null), Applied.Changed)
            }
            is ReadValue.State -> Step(copy(state = value.state, lastEdits = emptyList()), Applied.Changed)
            is ReadValue.Ended -> Step(copy(ended = value.cause, lastEdits = emptyList()), Applied.Changed)
            is ReadValue.Delta -> {
                val held = revision ?: return Step(this, Applied.Ignored)
                fun gap() = Step(ListProjection(rows, state, null, ended, failure, decoded, deltas, lastEdits, temporaryIds), Applied.Resync)
                if (value.base != held) return gap()
                val steps: List<ListEdit<Model>?> = value.edits.map { edit ->
                    when (edit) {
                        is RowEdit.Insert -> decode(listOf(edit.row))[0]?.let { ListEdit.Insert(edit.index.toInt(), it) }
                        is RowEdit.Rewrite -> decode(listOf(edit.row))[0]?.let { ListEdit.Rewrite(edit.index.toInt(), it) }
                        is RowEdit.Remove -> ListEdit.Remove(edit.index.toInt(), edit.id)
                        is RowEdit.Move -> ListEdit.Move(edit.from.toInt(), edit.to.toInt())
                    }
                }
                if (steps.any { it == null }) return gap()
                val applied = steps.filterNotNull()
                val next = rows.toMutableList().also { it.replay(applied) }
                val ids = HashMap(temporaryIds)
                value.temporary.forEach { ids[it.id] = it.temporary }
                value.forgotten.forEach { ids.remove(it) }
                Step(copy(rows = next, state = value.state, revision = value.revision, failure = failure, decoded = decoded, deltas = deltas + 1, lastEdits = applied, temporaryIds = ids), Applied.Changed)
            }
            else -> Step(this, Applied.Ignored)
        }
    }
}

/** The screen model of a stream of rows: a StateFlow over a projection. It reads the stream until its scope completes, and the completion of the reading job ends the stream in the core, also when the scope was cancelled before the first delivery. */
class Projected<Model : DBModel>(val stream: ValueStream, decoder: ModelDecoder<Model>, scope: CoroutineScope) {
    private val held = MutableStateFlow(ListProjection<Model>())
    val projection: StateFlow<ListProjection<Model>> = held.asStateFlow()
    private val pending = java.util.concurrent.atomic.AtomicReference<List<ListEdit<Model>>?>(null)
    private val reading: Job = scope.launch(start = CoroutineStart.ATOMIC) {
        stream.values.collect { value ->
            val step = held.value.apply(value, decoder)
            held.value = step.projection
            if (step.applied == Applied.Changed) pending.updateAndGet { ListEdit.composing(it, step.projection.lastEdits, step.projection.rows.size) }
        }
    }.also { job -> job.invokeOnCompletion { stream.close() } }

    val value: Model? get() = held.value.rows.firstOrNull()

    /** The steps since the last call, to apply to a list the screen keeps. `null`: the screen reads the rows again. */
    fun takeEdits(): List<ListEdit<Model>>? = pending.getAndSet(emptyList())

    suspend fun refresh() = stream.refresh()

    suspend fun loadMore(): LoadMoreResult = stream.loadMore()

    fun close() {
        reading.cancel()
        stream.close()
    }
}

/** The screen model of a count stream. */
class ProjectedCount(val stream: ValueStream, scope: CoroutineScope) {
    private val held = MutableStateFlow<Long?>(null)
    val count: StateFlow<Long?> = held.asStateFlow()
    private val shown = MutableStateFlow<ReadState?>(null)
    /** The state of the stream with its error. */
    val state: StateFlow<ReadState?> = shown.asStateFlow()
    private val over = MutableStateFlow<ReadEnd?>(null)
    /** Why the stream ended by itself. */
    val ended: StateFlow<ReadEnd?> = over.asStateFlow()
    private val reading: Job = scope.launch(start = CoroutineStart.ATOMIC) {
        stream.values.collect { value ->
            when (value) {
                is ReadValue.Count -> { held.value = value.count.toLong(); shown.value = value.state }
                is ReadValue.State -> shown.value = value.state
                is ReadValue.Ended -> over.value = value.cause
                else -> {}
            }
        }
    }.also { it.invokeOnCompletion { stream.close() } }

    fun close() {
        reading.cancel()
        stream.close()
    }
}

/** The events of a model after the stream opened. The flow ends when the collector stops or the session of the account ends. */
fun <Model : DBModel> ValueStream.events(decoder: ModelDecoder<Model>): Flow<ModelEventItem<Model>> = flow {
    values.collect { value ->
        when (value) {
            is ReadValue.Ready -> emit(ModelEventItem.Ready)
            is ReadValue.Lost -> emit(ModelEventItem.Lost(value.first, value.last))
            is ReadValue.Gap -> emit(ModelEventItem.Gap)
            is ReadValue.Event -> try {
                emit(ModelEventItem.Event(ModelEvent(value.position, value.area, value.kind, value.id, value.before?.let(decoder::read), value.after?.let(decoder::read))))
            } catch (error: kotlinx.coroutines.CancellationException) { throw error } catch (error: Throwable) {
                emit(ModelEventItem.Failed(error))
                close()
            }
            is ReadValue.State -> value.state.error?.let { emit(ModelEventItem.Failed(it)) }
            is ReadValue.Ended -> if (value.cause == ReadEnd.Invalid) emit(ModelEventItem.Failed(DbException.Invalid("the events stream could not open")))
            else -> {}
        }
    }
}

/** The status of a transfer at one change. */
data class TransferProgress(val revision: Long, val state: String, val progress: ULong, val error: String?)

/** The status of a transfer while it runs, by the key `model:row:action`. */
val ValueStream.uploadStatus: Flow<TransferProgress>
    get() = flow { values.collect { value -> (value as? ReadValue.Status)?.let { emit(TransferProgress(it.revision, it.state, it.progress, it.error)) } } }

/** The file of a media command and the declared upload action that sends it. */
class UploadCommand(val action: String, val request: UploadRequest)

/** The streams and commands of one signed-in account. The root of the app makes one after the sign-in and recreates its screens when the account changes: a scope whose session ended opens streams that end at once. */
class AccountScope private constructor(val epoch: ULong, internal val database: Database) {
    /** [refreshOnConnect] loads the relation again when the stream opens, also when it is fresh. The first rows stay visible. */
    fun <Model : DBModel> stream(query: Query<Model>, refreshOnConnect: Boolean = false): ValueStream = database.openValueStream(epoch, ReadRequest.List(query.json(refreshOnConnect)), query.loadTarget)

    fun <Model : DBModel> count(query: Query<Model>): ValueStream = database.openValueStream(epoch, ReadRequest.Count(query.json))

    /** One row by id. [fetch] tells when the library loads it from the server. */
    fun <Model : DBModel> entity(decoder: ModelDecoder<Model>, id: String, fetch: ReadFetch = ReadFetch.NEVER): ValueStream = database.openValueStream(epoch, ReadRequest.Entity(decoder.modelName, id, fetch))

    fun <Model : DBModel> events(decoder: ModelDecoder<Model>): ValueStream = database.openValueStream(epoch, ReadRequest.Events(decoder.modelName))

    /** The status of the transfer of `uploads[action]` of a row. */
    fun uploadStatus(row: DBModel, action: String): ValueStream = database.openValueStream(epoch, ReadRequest.Blob("${row.modelName}:${row.id}:$action"))

    /** Writes the model locally and queues it for the server. It works without a network and after the app closed. Only a durable create action (sending a message) and a declared upload action work this way; another model is refused. Throws `Interrupted` when the session of this scope ended. */
    suspend fun <Model : DBModel> send(decoder: ModelDecoder<Model>, values: JsonObject, args: JsonObject = JsonObject(emptyMap()), upload: UploadCommand? = null): MutationReceipt<Model> {
        val receipt = offCaller { database.send(CommandRequest(epoch, decoder.modelName, values.jsonText(), args.jsonText(), upload?.let { CommandUpload(it.action, it.request.wire()) })) }
        return MutationReceipt(database, decoder, receipt)
    }

    /** Sends the file of an existing record through its declared upload action. */
    suspend fun <Model : DBModel> upload(row: Model, action: String, request: UploadRequest, decoder: ModelDecoder<Model>): MutationReceipt<Model> {
        val receipt = offCaller { database.upload(epoch, row.modelName, row.id, action, request.wire()) }
        return MutationReceipt(database, decoder, receipt)
    }

    suspend fun <Model : DBModel> uploadBatch(decoder: ModelDecoder<Model>, items: List<UploadItem>, request: UploadBatchRequest): OperationReceipt<Model> =
        offCaller { OperationReceipt(database, decoder, database.uploadCreateBatch(epoch, decoder.modelName, items.map { it.wire() }, request.wire())) }

    companion object {
        fun current(): AccountScope {
            val database = DBlayer.database
            return AccountScope(database.sessionEpoch(), database)
        }
    }
}

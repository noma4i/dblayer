package com.noma4i.dblayer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonObject

/** The values of one stream in order, taken from the core one by one. Collecting it reads the stream, and a collector that is cancelled cancels the stream in the core and leaves the object alive. A stream that ends by itself delivers one `ReadValue.Ended` last. */
class ValueStream internal constructor(val epoch: ULong, private val reader: ReadStream) {
    val values: Flow<ReadValue> = flow {
        val claim = when (val result = reader.claimReader()) {
            is ReadClaim.Claimed -> result.reader
            is ReadClaim.Ended -> { emit(ReadValue.Ended(result.cause)); return@flow }
        }
        try {
            while (true) emit(reader.pull(claim) ?: break)
        } catch (cancelled: CancellationException) {
            if (!currentCoroutineContext().isActive) withContext(NonCancellable) { reader.cancel() }
            throw cancelled
        } finally {
            reader.releaseReader(claim)
        }
    }.flowOn(Dispatchers.Default)

    /** Loads the relation of the stream again. Throws `NoSession` when the session ended and `Invalid` when the stream has no relation. */
    suspend fun refresh() = offCaller { reader.refresh() }

    suspend fun loadMore(): LoadMoreResult = offCaller { reader.loadMore() }

    /** Stops reading: the stream ends in the core and nothing is delivered after this returns. */
    suspend fun cancel() {
        offCaller { reader.cancel() }
    }

    fun close() {
        reader.close()
    }

    /** Asks the core for a snapshot: the one call a reader makes when a delivery cannot be decoded. */
    internal fun resync() = reader.resync()
}

internal fun Database.openValueStream(epoch: ULong, request: ReadRequest): ValueStream =
    ValueStream(epoch, subscribeValues(epoch, request))

/** One delivery of a stream of rows with its rows decoded: the platform value that [ListProjection] mirrors. */
sealed interface ListDelivery<out Model> {
    class Snapshot<Model>(val revision: Long, val rows: List<Model>, val edits: List<ListEdit<Model>>, val state: ReadState) : ListDelivery<Model>
    class Delta<Model>(val base: Long, val revision: Long, val edits: List<ListEdit<Model>>, val state: ReadState) : ListDelivery<Model>
    class State(val revision: Long, val state: ReadState) : ListDelivery<Nothing>
    class Ended(val cause: EndCause) : ListDelivery<Nothing>
}

/** Decodes the rows of a value of a stream of rows. `null` for a value that carries no list. Throws when a row cannot be decoded: the reader then asks the core for a snapshot. */
fun <Model : DBModel> ReadValue.listDelivery(decoder: ModelDecoder<Model>): ListDelivery<Model>? = when (this) {
    is ReadValue.Snapshot -> ListDelivery.Snapshot(revision, rows.map(decoder::read), edits.map { listEdit(it, decoder) }, state)
    is ReadValue.Delta -> ListDelivery.Delta(base, revision, edits.map { listEdit(it, decoder) }, state)
    is ReadValue.State -> ListDelivery.State(revision, state)
    is ReadValue.Ended -> ListDelivery.Ended(cause)
    else -> null
}

private fun <Model : DBModel> listEdit(edit: RowEdit, decoder: ModelDecoder<Model>): ListEdit<Model> = when (edit) {
    is RowEdit.Insert -> ListEdit.Insert(edit.index.toInt(), decoder.read(edit.row))
    is RowEdit.Rewrite -> ListEdit.Rewrite(edit.index.toInt(), decoder.read(edit.row), edit.temporary)
    is RowEdit.Remove -> ListEdit.Remove(edit.index.toInt(), edit.id)
    is RowEdit.Move -> ListEdit.Move(edit.from.toInt(), edit.to.toInt())
}

/** What a screen shows of a stream of rows: an immutable value mirrored from the deliveries of the core. It holds no lock and no reference to the core. */
class ListProjection<Model : DBModel> private constructor(
    val rows: List<Model>,
    val state: ReadState?,
    val revision: Long?,
    /** The revision the last delta continues from. */
    val base: Long?,
    val ended: EndCause?,
    /** The steps of the last delivery from the rows before to the rows after, as the core gives them for a snapshot and a delta alike. */
    val lastEdits: List<ListEdit<Model>>,
) {
    constructor() : this(emptyList(), null, null, null, null, emptyList())

    val phase: ReadPhase? get() = state?.phase

    /** The error the core reports for the stream. */
    val error: DbException? get() = state?.error

    fun apply(delivery: ListDelivery<Model>): ListProjection<Model> = when (delivery) {
        is ListDelivery.Snapshot -> ListProjection(delivery.rows, delivery.state, delivery.revision, null, ended, delivery.edits)
        is ListDelivery.Delta -> ListProjection(rows.toMutableList().also { it.replay(delivery.edits) }, delivery.state, delivery.revision, delivery.base, ended, delivery.edits)
        is ListDelivery.State -> ListProjection(rows, delivery.state, delivery.revision, base, ended, emptyList())
        is ListDelivery.Ended -> ListProjection(rows, state, revision, base, delivery.cause, emptyList())
    }

    /** Decodes and applies a value. `null` for a value that carries no list. Throws when a row cannot be decoded. */
    fun apply(value: ReadValue, decoder: ModelDecoder<Model>): ListProjection<Model>? = value.listDelivery(decoder)?.let(::apply)
}

/** The screen model of a stream of rows: a StateFlow over a projection. It reads the stream until its scope completes, and the completion of the reading job ends the stream in the core, also when the scope was cancelled before the first delivery. Rows are decoded and applied on [Dispatchers.Default]. */
class Projected<Model : DBModel>(val stream: ValueStream, decoder: ModelDecoder<Model>, scope: CoroutineScope) {
    private val held = MutableStateFlow(ListProjection<Model>())
    val projection: StateFlow<ListProjection<Model>> = held.asStateFlow()
    internal val reading: Job = scope.launch(start = CoroutineStart.ATOMIC) {
        try {
            stream.values.mapNotNull { value ->
                try {
                    value.listDelivery(decoder)
                } catch (error: kotlinx.coroutines.CancellationException) { throw error } catch (error: Exception) {
                    stream.resync()
                    null
                }
            }.onEach {
                held.value = held.value.apply(it)
            }.flowOn(Dispatchers.Default).collect()
        } finally {
            stream.close()
        }
    }

    val value: Model? get() = held.value.rows.firstOrNull()

    /** The steps of the last delivery, to apply to a list the screen keeps. */
    val edits: List<ListEdit<Model>> get() = held.value.lastEdits

    suspend fun refresh() = stream.refresh()

    suspend fun loadMore(): LoadMoreResult = stream.loadMore()

    fun close() {
        reading.cancel()
    }
}

/** What a screen shows of a count stream: the count, its revision and its state are one value of one delivery. */
class CountProjection(val count: ULong?, val revision: Long?, val state: ReadState?, val ended: EndCause?) {
    val error: DbException? get() = state?.error

    internal fun apply(value: ReadValue): CountProjection = when (value) {
        is ReadValue.Count -> CountProjection(value.count, value.revision, value.state, ended)
        is ReadValue.State -> CountProjection(count, value.revision, value.state, ended)
        is ReadValue.Ended -> CountProjection(count, revision, state, value.cause)
        else -> this
    }
}

/** The screen model of a count stream: a StateFlow over a projection. */
class ProjectedCount(val stream: ValueStream, scope: CoroutineScope) {
    private val held = MutableStateFlow(CountProjection(null, null, null, null))
    val projection: StateFlow<CountProjection> = held.asStateFlow()
    internal val reading: Job = scope.launch(start = CoroutineStart.ATOMIC) {
        try {
            stream.values.collect { held.value = held.value.apply(it) }
        } finally {
            stream.close()
        }
    }

    fun close() {
        reading.cancel()
    }
}

/** The events of a model after the stream opened. The flow ends when the collector stops or the stream ends: the last item is [ModelEventItem.Ended] with the cause of the core. */
fun <Model : DBModel> ValueStream.events(decoder: ModelDecoder<Model>): Flow<ModelEventItem<Model>> = flow {
    values.collect { value ->
        when (value) {
            is ReadValue.Ready -> emit(ModelEventItem.Ready)
            is ReadValue.Lost -> emit(ModelEventItem.Lost(value.first, value.last))
            is ReadValue.Gap -> emit(ModelEventItem.Gap)
            is ReadValue.Event -> emit(ModelEventItem.Event(ModelEvent(value.position, value.area, value.kind, value.id, value.before?.let(decoder::read), value.after?.let(decoder::read))))
            is ReadValue.Ended -> emit(ModelEventItem.Ended(value.cause))
            else -> {}
        }
    }
}.flowOn(Dispatchers.Default)

sealed interface ChangeItem {
    data object Ready : ChangeItem

    class Changed(val revision: Long, val rows: List<ChangedRow>) : ChangeItem

    class Ended(val cause: EndCause) : ChangeItem
}

internal fun ValueStream.changeItems(): Flow<ChangeItem> = flow {
    values.collect { value ->
        when (value) {
            is ReadValue.Ready -> emit(ChangeItem.Ready)
            is ReadValue.Changes -> emit(ChangeItem.Changed(value.revision, value.rows))
            is ReadValue.Ended -> emit(ChangeItem.Ended(value.cause))
            else -> {}
        }
    }
}.flowOn(Dispatchers.Default)

/** The status of a transfer at one change. */
data class TransferProgress(val revision: Long, val state: String, val waitReason: String?, val progress: Double, val error: String?, val localUrl: String?)

/** One item of the status of a transfer: a change, or the end of the stream with the cause of the core. */
sealed interface TransferItem {
    class Progress(val progress: TransferProgress) : TransferItem
    class Ended(val cause: EndCause) : TransferItem
}

/** The status of a transfer while it runs, by the key `model:row:action`. The last item is [TransferItem.Ended]. */
val ValueStream.uploadStatus: Flow<TransferItem>
    get() = flow {
        values.collect { value ->
            when (value) {
                is ReadValue.Status -> emit(TransferItem.Progress(TransferProgress(value.revision, value.state, value.waitReason, value.progress, value.error, value.localUrl)))
                is ReadValue.Ended -> emit(TransferItem.Ended(value.cause))
                else -> {}
            }
        }
    }.flowOn(Dispatchers.Default)

/** One item of the queue of operations: the whole queue, or the end of the stream with the cause of the core. */
sealed interface OperationQueueItem {
    class Queue(val revision: Long, val items: List<OperationInfo>) : OperationQueueItem
    class Ended(val cause: EndCause) : OperationQueueItem
}

/** The queue of operations of the account: the whole queue at open and after every change, then [OperationQueueItem.Ended]. Each [OperationInfo] names the arguments of [AccountScope.retry] and [AccountScope.discard]. */
internal fun ValueStream.operationLists(): Flow<OperationQueueItem> =
    flow {
        values.collect { value ->
            when (value) {
                is ReadValue.Operations -> emit(OperationQueueItem.Queue(value.revision, value.items))
                is ReadValue.Ended -> emit(OperationQueueItem.Ended(value.cause))
                else -> {}
            }
        }
    }.flowOn(Dispatchers.Default)

/** The file of a media command and the declared upload action that sends it. */
class UploadCommand(val action: String, val request: UploadRequest)

/** The streams and commands of one signed-in account. The root of the app makes one after the sign-in and recreates its screens when the account changes: a scope whose session ended opens streams that end at once. */
class AccountScope private constructor(val epoch: ULong, internal val database: Database) {
    /** [refreshOnConnect] loads the relation again when the stream opens, also when it is fresh. The first rows stay visible. */
    fun <Model : DBModel> stream(query: Query<Model>, refreshOnConnect: Boolean = false): ValueStream = database.openValueStream(epoch, ReadRequest.List(query.spec(refreshOnConnect)))

    fun <Model : DBModel> count(query: Query<Model>): ValueStream = database.openValueStream(epoch, ReadRequest.Count(query.spec()))

    /** One row by id. [fetch] tells when the library loads it from the server. */
    fun <Model : DBModel> entity(decoder: ModelDecoder<Model>, id: String, fetch: ReadFetch = ReadFetch.NEVER): ValueStream = database.openValueStream(epoch, ReadRequest.Entity(decoder.modelName, id, fetch))

    fun <Model : DBModel> events(decoder: ModelDecoder<Model>): ValueStream = database.openValueStream(epoch, ReadRequest.Events(decoder.modelName))

    fun changes(): Flow<ChangeItem> = database.openValueStream(epoch, ReadRequest.Changes).changeItems()

    /** The status of the transfer of `uploads[action]` of a row. */
    fun uploadStatus(row: DBModel, action: String): ValueStream = database.openValueStream(epoch, ReadRequest.Blob(row.modelName, row.id, action))

    /** The status of the transfer of a staged upload, before and after its [StagedUpload.commit]. */
    fun uploadStatus(staged: StagedUpload<*>): ValueStream = database.openValueStream(epoch, ReadRequest.Blob(staged.modelName, staged.id, staged.action))

    /** The operations that wait for the server, are on their way or were refused. */
    fun operations(): Flow<OperationQueueItem> = database.openValueStream(epoch, ReadRequest.Operations).operationLists()

    /** Sends a refused operation again, by the fields of its [OperationInfo]. Throws `NotFound` when the operation is gone. */
    suspend fun retry(model: String, id: String, action: String?) {
        offCallerInOrder(database) { it.retry(epoch, model, id, action, null) }
    }

    /** Drops an operation that is not on its way, by the fields of its [OperationInfo]. */
    suspend fun discard(model: String, id: String, action: String?) = offCallerInOrder(database) { it.discard(epoch, model, id, action) }

    /** Writes the model locally and queues it for the server. It works without a network and after the app closed. A model with a `create` operation and a declared upload action work this way; another model is refused. Throws `NoSession` when the session of this scope ended. */
    suspend fun <Model : DBModel> send(decoder: ModelDecoder<Model>, values: JsonObject, args: JsonObject = JsonObject(emptyMap()), upload: UploadCommand? = null): MutationReceipt<Model> {
        return offCallerInOrder(database) { ordered -> MutationReceipt(database, decoder, ordered.send(CommandRequest(epoch, decoder.modelName, values.jsonText(), args.jsonText(), upload?.let { CommandUpload(it.action, it.request.wire()) }))) }
    }

    /** Sends the file of a record through its declared upload action. The row does not need to exist. */
    suspend fun <Model : DBModel> upload(decoder: ModelDecoder<Model>, id: String, action: String, request: UploadRequest): MutationReceipt<Model> {
        return offCallerInOrder(database) { ordered -> MutationReceipt(database, decoder, ordered.upload(epoch, decoder.modelName, id, action, request.wire())) }
    }

    /** Sends the file of an existing record through its declared upload action. */
    suspend fun <Model : DBModel> upload(row: Model, action: String, request: UploadRequest, decoder: ModelDecoder<Model>): MutationReceipt<Model> = upload(decoder, row.id, action, request)

    /** Sends the file before its row exists. [StagedUpload.commit] creates the row and sends the final mutation, [StagedUpload.discard] removes the transfer. */
    suspend fun <Model : DBModel> stage(decoder: ModelDecoder<Model>, action: String, request: UploadRequest): StagedUpload<Model> {
        val staged = offCallerInOrder(database) { it.stage(epoch, decoder.modelName, action, request.wire()) }
        return StagedUpload(database, epoch, decoder, staged.rowId, action)
    }

    suspend fun <Model : DBModel> uploadBatch(decoder: ModelDecoder<Model>, items: List<UploadItem>, request: UploadBatchRequest): OperationReceipt<Model> =
        offCallerInOrder(database) { ordered -> OperationReceipt(database, decoder, ordered.uploadCreateBatch(epoch, decoder.modelName, items.map { it.wire() }, request.wire())) }

    companion object {
        fun current(): AccountScope {
            val database = DBlayer.database
            return AccountScope(database.admit(null), database)
        }
    }
}

/** The value `uploads[action]` of the active transfer this entry shows: the same value a row shows. `null` for an operation that is no transfer. */
val OperationInfo.uploadInfo: UploadInfo? get() = upload?.let { UploadInfo.of(it) }

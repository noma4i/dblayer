package com.noma4i.dblayer

import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.coroutines.ensureActive

@Serializable
enum class SyncState {
    @SerialName("synced") Synced,
    @SerialName("pending") Pending,
    @SerialName("sending") Sending,
    @SerialName("failed") Failed,
}

@Serializable
data class OperationFailure(
    val kind: String,
    val sessionCode: @Serializable(with = SessionCodeSerializer::class) SessionCode?,
    val requestSize: ULong?,
    val requestLimit: ULong?,
    val message: String,
    val extensions: @Serializable(with = GraphqlExtensionsSerializer::class) GraphqlExtensions?,
)

@Serializable
data class SyncInfo(
    val state: SyncState,
    val actions: List<String>,
    val dirtyFields: List<String>,
    val error: OperationFailure?,
    val attempts: Int,
    val since: Long,
) {
    constructor() : this(SyncInfo.of(defaultSync()))

    private constructor(copy: SyncInfo) : this(copy.state, copy.actions, copy.dirtyFields, copy.error, copy.attempts, copy.since)

    companion object
}

@Serializable
enum class UploadStatus {
    @SerialName("waiting") Waiting,
    @SerialName("preparing") Preparing,
    @SerialName("queued") Queued,
    @SerialName("uploading") Uploading,
    @SerialName("sending") Sending,
    @SerialName("done") Done,
    @SerialName("failed") Failed,
    @SerialName("cancelled") Cancelled,
}

@Serializable
data class UploadProcessing(val progress: Double?)

@Serializable
data class UploadInfo(
    val status: UploadStatus,
    val stage: String,
    @SerialName("sent_bytes") val sentBytes: ULong,
    @SerialName("total_bytes") val totalBytes: ULong,
    /** The sent share of the bytes in percent, 0 to 100. */
    val progress: Double,
    @SerialName("wait_reason") val waitReason: String?,
    val attempts: Int,
    val error: OperationFailure?,
    @SerialName("local_url") val localUrl: String?,
    val processing: UploadProcessing?,
) {
    companion object
}

interface DBModel {
    val id: String
    val modelName: String
    val sync: SyncInfo
    val uploads: Map<String, UploadInfo>
    /** The revision of the store the row was read at. */
    val rowRevision: Long
    /** The bytes the core estimates the row holds. */
    val estimatedBytes: ULong
}

val DBModel.errorMessage: String? get() = sync.error?.message

/** The database and the epoch a command belongs to: the epoch the account holds, else the epoch the core admits for the open database. */
internal class Admission(val database: Database, val epoch: ULong)

internal fun admit(account: AccountScope?): Admission {
    val database = account?.database ?: DBlayer.database
    return Admission(database, database.admit(account?.epoch))
}

/** Admits the call before the work leaves the caller, then runs it off the caller with the pair. */
suspend fun <T> onAccount(account: AccountScope?, call: suspend (Database, ULong) -> T): T {
    val admission = admit(account)
    return offCaller { call(admission.database, admission.epoch) }
}

suspend fun <Model : DBModel> ModelDecoder<Model>.retry(id: String, action: String?, source: UploadSource? = null, account: AccountScope? = null): MutationReceipt<Model> = onAccount(account) { database, epoch -> MutationReceipt(database, this, database.retry(epoch, modelName, id, action, source?.wire())) }
suspend fun DBModel.discard(action: String?, account: AccountScope? = null) = onAccount(account) { database, epoch -> database.discard(epoch, modelName, id, action) }

suspend fun <Model : DBModel> ModelDecoder<Model>.destroy(id: String, account: AccountScope? = null): MutationReceipt<Model> = onAccount(account) { database, epoch -> MutationReceipt(database, this, database.destroy(epoch, modelName, id)) }

suspend fun DBModel.setLocal(path: String, value: kotlinx.serialization.json.JsonElement, account: AccountScope? = null) = onAccount(account) { database, epoch -> database.setLocal(epoch, modelName, id, path, value.jsonText()) }

suspend fun <Model : DBModel> Model.act(action: String, values: kotlinx.serialization.json.JsonObject, args: kotlinx.serialization.json.JsonObject, decoder: ModelDecoder<Model>, account: AccountScope? = null): MutationReceipt<Model> {
    return onAccount(account) { database, epoch -> MutationReceipt(database, decoder, database.act(epoch, modelName, id, action, values.jsonText(), args.jsonText())) }
}

sealed interface OperationOutcome<out Model : DBModel> {
    val confirmedValue: Model? get() = null
    class Confirmed<Model : DBModel>(override val confirmedValue: Model?, val answerJson: String?) : OperationOutcome<Model>
    class Rejected(val failure: WireFailure) : OperationOutcome<Nothing>
    data class Ended(val cause: EndCause) : OperationOutcome<Nothing>
}

data class Confirmed<Model : DBModel>(val value: Model?, val answerJson: String?)

class MutationReceipt<Model : DBModel>(private val database: Database, private val decoder: ModelDecoder<Model>, private val wire: WireOperationReceipt) {
    val optimisticValue: Model? = wire.optimisticValue?.let(decoder::read)
    val revision: Long = wire.revision
    val epoch: ULong = wire.epoch
    val operation: Long? = wire.operation

    /** The one outcome of the receipt: the value, or [DbException.Rejected] (`kept` = the failed intent stays) or the exception of the end of the wait. [discardOnFailure] rolls a failed cycle back first. */
    suspend fun confirmed(discardOnFailure: Boolean = false): Confirmed<Model> = offCaller {
        val result = database.confirmed(wire, discardOnFailure)
        Confirmed(result.value?.let(decoder::read), result.answerJson)
    }

    suspend fun outcome(): OperationOutcome<Model> = offCaller {
        when (val result = database.outcome(wire)) {
            is WireOperationOutcome.Confirmed -> OperationOutcome.Confirmed(result.value?.let(decoder::read), result.answerJson)
            is WireOperationOutcome.Rejected -> OperationOutcome.Rejected(result.failure)
            is WireOperationOutcome.Ended -> OperationOutcome.Ended(result.cause)
        }
    }
}

/** [receipt] is the receipt of this item: its own operation, revision and outcome. */
data class ItemReceipt<Model : DBModel>(val item: ItemId, val optimisticValue: Model, val receipt: MutationReceipt<Model>)

class OperationReceipt<Model : DBModel>(internal val database: Database, internal val decoder: ModelDecoder<Model>, internal val wire: WireBatchReceipt) {
    val epoch: ULong = wire.epoch
    val items: List<ItemReceipt<Model>> = wire.items.map { ItemReceipt(it.item, decoder.read(it.optimisticValue), MutationReceipt(database, decoder, it.receipt)) }
}

class ModelEvent<Model : DBModel>(val position: ULong, val area: String, val kind: String, val id: String, val before: Model?, val after: Model?)

sealed interface ModelEventItem<out Model : DBModel> {
    /** The source of the events is open: every event after this item reaches the stream. */
    data object Ready : ModelEventItem<Nothing>

    class Event<Model : DBModel>(val event: ModelEvent<Model>) : ModelEventItem<Model>
    /** The events at these positions of the stream are lost. [first] and [last] are included. */
    class Lost(val first: ULong, val last: ULong) : ModelEventItem<Nothing>

    /** The core lost the order of the events: the reader reads the rows again. */
    data object Gap : ModelEventItem<Nothing>

    /** The stream ended: the core reports why. Nothing follows. */
    class Ended(val cause: EndCause) : ModelEventItem<Nothing>
}

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
    @SerialName("delivery_unknown") DeliveryUnknown,
}

@Serializable
data class OperationFailure(val kind: String, val message: String, val extensions: @Serializable(with = GraphqlExtensionsSerializer::class) GraphqlExtensions?)

@Serializable
data class SyncInfo(
    val state: SyncState = SyncState.Synced,
    val actions: List<String> = emptyList(),
    val dirtyFields: List<String> = emptyList(),
    val error: OperationFailure? = null,
    val attempts: Int = 0,
    val since: Long = 0,
) {
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
    @SerialName("delivery_unknown") DeliveryUnknown,
    @SerialName("cancelled") Cancelled,
}

@Serializable
data class UploadInfo(
    val status: UploadStatus,
    val stage: String,
    @SerialName("sent_bytes") val sentBytes: ULong,
    @SerialName("total_bytes") val totalBytes: ULong,
    @SerialName("wait_reason") val waitReason: String? = null,
    val attempts: Int = 0,
    val error: OperationFailure? = null,
) {
    companion object

    val percent: Double get() = if (totalBytes == 0uL) 0.0 else minOf(100.0, sentBytes.toDouble() * 100 / totalBytes.toDouble())
}

interface DBModel {
    val id: String
    val modelName: String
    val sync: SyncInfo
    val uploads: Map<String, UploadInfo>
}

val DBModel.needsAttention: Boolean get() = sync.state == SyncState.Failed || sync.state == SyncState.DeliveryUnknown
val DBModel.canRetry: Boolean get() = needsAttention
val DBModel.errorMessage: String? get() = sync.error?.message

/** The database and the epoch a command belongs to: the pair the account holds, else the open database and the epoch of its session now. A command of an account that left is refused. */
internal class Admission(val database: Database, val epoch: ULong)

internal fun admit(account: AccountScope?): Admission =
    if (account != null) Admission(account.database, account.epoch) else DBlayer.database.let { Admission(it, it.sessionEpoch()) }

/** Admits the call before the work leaves the caller, then runs it off the caller with the pair. */
suspend fun <T> onAccount(account: AccountScope?, call: suspend (Database, ULong) -> T): T {
    val admission = admit(account)
    return offCaller { call(admission.database, admission.epoch) }
}

suspend fun <Model : DBModel> ModelDecoder<Model>.retry(id: String, action: String?, account: AccountScope? = null): MutationReceipt<Model> = onAccount(account) { database, epoch -> MutationReceipt(database, this, database.retry(epoch, modelName, id, action)) }
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
    data object DeliveryUnknown : OperationOutcome<Nothing>
    data object Interrupted : OperationOutcome<Nothing>
}

class MutationReceipt<Model : DBModel>(private val database: Database, private val decoder: ModelDecoder<Model>, private val wire: WireOperationReceipt) {
    val optimisticValue: Model? = wire.optimisticValue?.let(decoder::read)
    val revision: Long = wire.revision
    val epoch: ULong = wire.epoch
    val operation: Long? = wire.operation

    suspend fun outcome(): OperationOutcome<Model> = offCaller {
        when (val result = database.outcome(wire)) {
            is WireOperationOutcome.Confirmed -> OperationOutcome.Confirmed(result.value?.let(decoder::read), result.answerJson)
            is WireOperationOutcome.Rejected -> OperationOutcome.Rejected(result.failure)
            WireOperationOutcome.DeliveryUnknown -> OperationOutcome.DeliveryUnknown
            WireOperationOutcome.Interrupted -> OperationOutcome.Interrupted
        }
    }
}

data class ItemReceipt<Model : DBModel>(val item: ItemId, val optimisticValue: Model)

sealed interface ItemOutcome<out Model : DBModel> {
    class Confirmed<Model : DBModel>(val item: ItemId, val value: Model) : ItemOutcome<Model>
    class Discarded(val item: ItemId) : ItemOutcome<Nothing>
}

sealed interface BatchResult {
    data object Confirmed : BatchResult
    class Rejected(val failure: WireFailure) : BatchResult
    data object DeliveryUnknown : BatchResult
    data object Interrupted : BatchResult
}

data class BatchOutcome<Model : DBModel>(val result: BatchResult, val items: List<ItemOutcome<Model>>)

class OperationReceipt<Model : DBModel>(private val database: Database, private val decoder: ModelDecoder<Model>, private val wire: WireBatchReceipt) {
    val epoch: ULong = wire.epoch
    val operation: Long = wire.operation
    val revision: Long = wire.revision
    val items: List<ItemReceipt<Model>> = wire.items.map { ItemReceipt(it.item, decoder.read(it.optimisticValue)) }

    suspend fun outcome(): BatchOutcome<Model> = offCaller {
        val outcome = database.batchOutcome(wire)
        val result = when (val value = outcome.result) {
            WireBatchResult.Confirmed -> BatchResult.Confirmed
            is WireBatchResult.Rejected -> BatchResult.Rejected(value.failure)
            WireBatchResult.DeliveryUnknown -> BatchResult.DeliveryUnknown
            WireBatchResult.Interrupted -> BatchResult.Interrupted
        }
        val items = outcome.items.map { item ->
            when (item) {
                is WireItemOutcome.Confirmed -> ItemOutcome.Confirmed(item.item, decoder.read(item.value))
                is WireItemOutcome.Discarded -> ItemOutcome.Discarded(item.item)
            }
        }
        BatchOutcome(result, items)
    }
}

class ModelEvent<Model : DBModel>(val area: String, val kind: String, val id: String, val before: Model?, val after: Model?)

sealed interface ModelEventItem<out Model : DBModel> {
    class Event<Model : DBModel>(val event: ModelEvent<Model>) : ModelEventItem<Model>
    class Missed(val count: ULong) : ModelEventItem<Nothing>

    class Failed(val error: Throwable) : ModelEventItem<Nothing>
}

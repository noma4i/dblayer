package com.noma4i.dblayer

import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.KSerializer
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

/** The epoch a command belongs to: the epoch of the account the caller holds, else the epoch of the session now. A command of an account that left is refused. */
internal fun admitted(account: AccountScope?): ULong = account?.epoch ?: DBlayer.database.sessionEpoch()

suspend fun DBModel.retry(action: String?, account: AccountScope? = null): ULong { val epoch = admitted(account); return offCaller { DBlayer.database.retry(epoch, modelName, id, action) } }
suspend fun <Model : DBModel> ModelDecoder<Model>.retry(id: String, action: String?, account: AccountScope? = null): ULong { val epoch = admitted(account); return offCaller { DBlayer.database.retry(epoch, modelName, id, action) } }
suspend fun DBModel.discard(action: String?, account: AccountScope? = null) { val epoch = admitted(account); offCaller { DBlayer.database.discard(epoch, modelName, id, action) } }

suspend fun DBModel.destroy(account: AccountScope? = null) { val epoch = admitted(account); offCaller { DBlayer.database.destroy(epoch, modelName, id) } }

suspend fun DBModel.setLocal(path: String, value: kotlinx.serialization.json.JsonElement, account: AccountScope? = null) { val epoch = admitted(account); offCaller { DBlayer.database.setLocal(epoch, modelName, id, path, value.jsonText()) } }

/** Waits until the server has answered the queued changes of the record, or of the declared `action` when given, throws on a refusal or when offline. */
suspend fun DBModel.settled(action: String?, account: AccountScope? = null) { val epoch = admitted(account); offCaller { DBlayer.database.settled(epoch, modelName, id, action) } }

suspend fun DBModel.confirmedId(account: AccountScope? = null): String { val epoch = admitted(account); return offCaller { DBlayer.database.confirmed(epoch, modelName, id) } }

suspend fun <Model : DBModel> Model.act(action: String, values: kotlinx.serialization.json.JsonObject, args: kotlinx.serialization.json.JsonObject, decoder: ModelDecoder<Model>, account: AccountScope? = null): Model {
    val epoch = admitted(account)
    return offCaller { decoder.read(DBlayer.database.act(epoch, modelName, id, action, values.jsonText(), args.jsonText()).row) }
}

suspend fun <Model : DBModel, Answer> Model.actAnswering(
    action: String,
    values: kotlinx.serialization.json.JsonObject,
    args: kotlinx.serialization.json.JsonObject,
    decoder: ModelDecoder<Model>,
    answerSerializer: KSerializer<Answer>,
    account: AccountScope? = null,
): ActionAnswer<Model, Answer> {
    val epoch = admitted(account)
    val queued = offCaller { DBlayer.database.act(epoch, modelName, id, action, values.jsonText(), args.jsonText()) }
    return ActionAnswer(offCaller { decoder.read(queued.row) }, queued.operation, answerSerializer, epoch)
}

class ActionAnswer<Model : DBModel, Answer>(val model: Model, private val operation: Long, private val serializer: KSerializer<Answer>, private val epoch: ULong) {
    suspend fun answer(): Answer = offCaller { dbJson.decodeFromString(serializer, DBlayer.database.answer(epoch, model.modelName, operation)) }
}

class ModelEvent<Model : DBModel>(val area: String, val kind: String, val id: String, val before: Model?, val after: Model?)

sealed interface ModelEventItem<out Model : DBModel> {
    class Event<Model : DBModel>(val event: ModelEvent<Model>) : ModelEventItem<Model>
    class Missed(val count: ULong) : ModelEventItem<Nothing>

    class Failed(val error: Throwable) : ModelEventItem<Nothing>
}

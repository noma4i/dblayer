package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
enum class SyncState {
    @SerialName("synced") Synced,
    @SerialName("pending") Pending,
    @SerialName("sending") Sending,
    @SerialName("failed") Failed,
    @SerialName("delivery_unknown") DeliveryUnknown,
}

@Serializable
data class OperationFailure(val kind: String, val message: String)

@Serializable
data class SyncInfo(
    val state: SyncState = SyncState.Synced,
    val actions: List<String> = emptyList(),
    val dirtyFields: List<String> = emptyList(),
    val error: OperationFailure? = null,
    val attempts: Int = 0,
    val since: Long = 0,
)

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
    val percent: Double get() = if (totalBytes == 0uL) 0.0 else minOf(100.0, sentBytes.toDouble() * 100 / totalBytes.toDouble())
}

interface DBModel {
    val id: String
    val modelName: String
    val sync: SyncInfo
    val upload: UploadInfo?
}

val DBModel.needsAttention: Boolean get() = sync.state == SyncState.Failed || sync.state == SyncState.DeliveryUnknown
val DBModel.canRetry: Boolean get() = needsAttention
val DBModel.errorMessage: String? get() = sync.error?.message

private val background = CoroutineScope(Dispatchers.Default)

fun DBModel.retry() { background.launch { runCatching { DBlayer.database.retry(modelName, id) } } }
fun DBModel.discard() { runCatching { DBlayer.database.discard(modelName, id) } }
fun retryAll() { background.launch { runCatching { DBlayer.database.retryAll() } } }

suspend fun DBModel.destroy() { DBlayer.database.destroy(modelName, id) }

/** The server id of a record created locally. Waits for the queued create to be answered, throws when it is refused or the device is offline. */
suspend fun DBModel.confirmedId(): String = DBlayer.database.confirmed(modelName, id)

suspend fun <Model : DBModel> Model.act(action: String, values: kotlinx.serialization.json.JsonObject, args: kotlinx.serialization.json.JsonObject, serializer: KSerializer<Model>): Model =
    dbJson.decodeFromString(serializer, DBlayer.database.act(modelName, id, action, values.toString(), args.toString()))

fun <Target : DBModel> DBModel.related(name: String, serializer: KSerializer<Target>): LiveSource<List<Target>> {
    val model = modelName
    val rowId = id
    return LiveSource(emptyList(), { listener -> DBlayer.database.liveHasMany(model, rowId, name, listener) }) { change -> change.decode(serializer) }
}

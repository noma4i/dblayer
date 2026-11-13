package com.noma4i.dblayer

import kotlinx.serialization.json.JsonObject

class Prepare internal constructor(internal val wire: WirePrepare) {
    companion object {
        fun video(limits: VideoLimits? = null, mirror: Boolean = false, onCannotDecode: CannotDecode = CannotDecode.FAIL, maxBytes: ULong? = null) = Prepare(WirePrepare.Video(limits, mirror, onCannotDecode, maxBytes))
        fun image(edit: PhotoEdit, quality: Double? = null, format: ImageFormat = ImageFormat.JPEG, maxBytes: ULong? = null) = Prepare(WirePrepare.Image(edit, quality, format, maxBytes))
    }
}

data class UploadRequest(
    val path: String?,
    val name: String? = null,
    val mime: String? = null,
    val variables: JsonObject = JsonObject(emptyMap()),
    val prepare: Prepare? = null,
) {
    internal fun wire() = WireUploadRequest(path?.let { WireSource.File(it, name, mime, prepare?.wire) } ?: WireSource.Pending, variables.jsonText())
    companion object {
        fun pending(variables: JsonObject = JsonObject(emptyMap())) = UploadRequest(null, variables = variables)
    }
}

suspend fun DBModel.cancelUpload(action: String, account: AccountScope? = null): Boolean = onAccount(account) { database, epoch -> database.cancelUpload(epoch, modelName, id, action) }

/** A file sent before its row exists. [id] is the id of the row [commit] creates. */
class StagedUpload<Model : DBModel> internal constructor(private val database: Database, private val epoch: ULong, private val decoder: ModelDecoder<Model>, val id: String, val action: String) {
    internal val modelName: String get() = decoder.modelName

    /** Creates the row with [values] and sends the final mutation with [variables]. A second call throws `Invalid`. */
    suspend fun commit(values: JsonObject, variables: JsonObject = JsonObject(emptyMap())): MutationReceipt<Model> =
        offCallerInOrder(database) { ordered -> MutationReceipt(database, decoder, ordered.commitStaged(epoch, decoder.modelName, id, action, values.jsonText(), variables.jsonText())) }

    /** Removes the transfer and its files. Returns the value of the core. */
    suspend fun discard(): Boolean = offCallerInOrder(database) { it.cancelUpload(epoch, decoder.modelName, id, action) }
}

data class UploadItem(
    val values: JsonObject,
    val path: String?,
    val name: String? = null,
    val mime: String? = null,
    val variables: JsonObject = JsonObject(emptyMap()),
    val prepare: Prepare? = null,
) {
    constructor(values: JsonObject, request: UploadRequest) : this(values, request.path, request.name, request.mime, request.variables, request.prepare)
    internal fun wire() = WireUploadItem(values.jsonText(), path?.let { WireSource.File(it, name, mime, prepare?.wire) } ?: WireSource.Pending, variables.jsonText())
}

data class UploadSource(
    val path: String,
    val name: String? = null,
    val mime: String? = null,
    val values: JsonObject = JsonObject(emptyMap()),
    val variables: JsonObject = JsonObject(emptyMap()),
    val prepare: Prepare? = null,
) {
    internal fun wire() = WireUploadSource(path, name, mime, values.jsonText(), variables.jsonText(), prepare?.wire)
}

suspend fun DBModel.supplySource(action: String, source: UploadSource, account: AccountScope? = null) = onAccount(account) { database, epoch -> database.supplySource(epoch, modelName, id, action, source.wire()) }
suspend fun <Model : DBModel> ModelDecoder<Model>.supplySource(id: String, action: String, source: UploadSource, account: AccountScope? = null) = onAccount(account) { database, epoch -> database.supplySource(epoch, modelName, id, action, source.wire()) }
suspend fun DBModel.failSource(action: String, message: String, account: AccountScope? = null) = onAccount(account) { database, epoch -> database.failSource(epoch, modelName, id, action, message) }
suspend fun <Model : DBModel> ModelDecoder<Model>.failSource(id: String, action: String, message: String, account: AccountScope? = null) = onAccount(account) { database, epoch -> database.failSource(epoch, modelName, id, action, message) }

data class UploadBatchRequest(
    val operation: String,
    val variables: JsonObject = JsonObject(emptyMap()),
) {
    internal fun wire() = WireUploadBatch(operation, variables.jsonText())
}

package com.noma4i.dblayer

import kotlinx.serialization.json.JsonObject

data class UploadOperation(val name: String, val rootPath: String? = null) {
    internal fun wire() = WireUploadOperation(name, rootPath)
}

data class UploadOperations(val createBlob: UploadOperation? = null, val multipart: Multipart? = null) {
    data class Multipart(val initiate: UploadOperation, val presign: UploadOperation, val complete: UploadOperation, val abort: UploadOperation) {
        internal fun wire() = WireMultipartOperations(initiate.wire(), presign.wire(), complete.wire(), abort.wire())
    }

    internal fun wire() = WireUploadOperations(createBlob?.wire(), multipart?.wire())
}

data class UploadRequest(
    val path: String,
    val name: String? = null,
    val mime: String? = null,
    val variables: JsonObject = JsonObject(emptyMap()),
) {
    internal fun wire() = WireUploadRequest(path, name, mime, variables.jsonText())
}

/** Creates the record locally at once and sends its file in the background through the declared upload [action]. A failed upload leaves the record failed: `retry(action)` sends again, `discard(action)` removes it. */
suspend fun <Model : DBModel> uploadCreate(modelName: String, action: String, values: JsonObject, request: UploadRequest, decoder: ModelDecoder<Model>): Model =
    offCaller { decoder.read(DBlayer.database.uploadCreate(modelName, action, values.jsonText(), request.wire())) }

suspend fun <Model : DBModel, Answer> Model.upload(action: String, request: UploadRequest, answerSerializer: kotlinx.serialization.KSerializer<Answer>): ActionAnswer<Model, Answer> {
    val operation = offCaller { DBlayer.database.upload(modelName, id, action, request.wire()) }
    return ActionAnswer(this, operation, answerSerializer)
}

suspend fun DBModel.cancelUpload(action: String): Boolean = offCaller { DBlayer.database.cancelUpload(modelName, id, action) }

data class UploadItem(
    val values: JsonObject,
    val path: String,
    val name: String? = null,
    val mime: String? = null,
    val variables: JsonObject = JsonObject(emptyMap()),
) {
    internal fun wire() = WireUploadItem(values.jsonText(), path, name, mime, variables.jsonText())
}

data class UploadBatchRequest(
    val operation: String,
    val variables: JsonObject = JsonObject(emptyMap()),
) {
    internal fun wire() = WireUploadBatch(operation, variables.jsonText())
}

suspend fun <Model : DBModel> ModelDecoder<Model>.uploadCreate(items: List<UploadItem>, request: UploadBatchRequest): List<Model> =
    offCaller { DBlayer.database.uploadCreateBatch(modelName, items.map { it.wire() }, request.wire()).map { read(it) } }

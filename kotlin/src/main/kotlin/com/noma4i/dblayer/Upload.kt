package com.noma4i.dblayer

import kotlinx.serialization.json.JsonObject

/** A registered operation of the upload declaration. [rootPath] names the response field the operation answers under; null keeps the first field of its document. */
data class UploadOperation(val name: String, val rootPath: String? = null) {
    internal fun wire() = WireUploadOperation(name, rootPath)
}

/** The operations that move a file to storage, declared once in `DBlayer.open`. A file takes the single request through [createBlob] or the multipart requests, by its size. */
data class UploadOperations(val createBlob: UploadOperation? = null, val multipart: Multipart? = null) {
    data class Multipart(val initiate: UploadOperation, val presign: UploadOperation, val complete: UploadOperation, val abort: UploadOperation) {
        internal fun wire() = WireMultipartOperations(initiate.wire(), presign.wire(), complete.wire(), abort.wire())
    }

    internal fun wire() = WireUploadOperations(createBlob?.wire(), multipart?.wire())
}

/** A local file sent to storage, then the content mutation of the declared upload action, which carries the stored blob as `signedId`. */
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

/** Sends the file of an existing record through the declared upload [action], then runs its content mutation. */
suspend fun DBModel.upload(action: String, request: UploadRequest) { offCaller { DBlayer.database.upload(modelName, id, action, request.wire()) } }

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

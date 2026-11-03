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

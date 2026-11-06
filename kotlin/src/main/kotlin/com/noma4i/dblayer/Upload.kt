package com.noma4i.dblayer

import kotlinx.serialization.json.JsonObject

data class UploadRequest(
    val path: String,
    val name: String? = null,
    val mime: String? = null,
    val variables: JsonObject = JsonObject(emptyMap()),
) {
    internal fun wire() = WireUploadRequest(path, name, mime, variables.jsonText())
}

suspend fun DBModel.cancelUpload(action: String, account: AccountScope? = null): Boolean = onAccount(account) { database, epoch -> database.cancelUpload(epoch, modelName, id, action) }

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

package com.noma4i.dblayer

import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/** A local file sent to storage, then one content mutation that carries the stored blob as `signedId`. */
data class UploadRequest(
    val path: String,
    val mutation: String,
    val createBlob: String? = null,
    val multipart: Multipart? = null,
    val name: String? = null,
    val mime: String? = null,
    val variables: JsonObject = JsonObject(emptyMap()),
) {
    data class Multipart(val initiate: String, val presign: String, val complete: String, val abort: String)

    internal val json: String
        get() = buildJsonObject {
            put("path", path)
            put("name", name?.let(::JsonPrimitive) ?: JsonNull)
            put("mime", mime?.let(::JsonPrimitive) ?: JsonNull)
            put("variables", variables)
            put("steps", buildJsonObject {
                put("single", createBlob?.let { buildJsonObject { put("create", it) } } ?: JsonNull)
                put("multipart", multipart?.let { buildJsonObject { put("init", it.initiate); put("presign", it.presign); put("complete", it.complete); put("abort", it.abort) } } ?: JsonNull)
                put("mutation", mutation)
                put("signed_id_variable", "signedId")
                put("blob_id_variable", JsonNull)
            })
        }.jsonText()
}

/** Creates the record locally at once and sends its file in the background. A failed upload leaves the record failed: `retry()` sends again, `discard()` removes it. */
suspend fun <Model : DBModel> uploadCreate(modelName: String, values: JsonObject, request: UploadRequest, decoder: ModelDecoder<Model>): Model =
    decoder.read(DBlayer.database.uploadCreate(modelName, values.jsonText(), request.json))

/** Sends the file of an existing record, then runs the content mutation. */
suspend fun DBModel.upload(request: UploadRequest) { DBlayer.database.upload(modelName, id, request.json) }

suspend fun DBModel.cancelUpload(): Boolean = DBlayer.database.cancelUpload(modelName, id)

data class UploadItem(
    val values: JsonObject,
    val path: String,
    val name: String? = null,
    val mime: String? = null,
    val variables: JsonObject = JsonObject(emptyMap()),
) {
    internal val value: JsonObject
        get() = buildJsonObject {
            put("values", values)
            put("path", path)
            put("name", name?.let(::JsonPrimitive) ?: JsonNull)
            put("mime", mime?.let(::JsonPrimitive) ?: JsonNull)
            put("variables", variables)
        }
}

data class UploadBatchRequest(
    val operation: String,
    val createBlob: String? = null,
    val multipart: UploadRequest.Multipart? = null,
    val variables: JsonObject = JsonObject(emptyMap()),
) {
    internal val json: String
        get() = buildJsonObject {
            put("operation", operation)
            put("variables", variables)
            put("steps", buildJsonObject {
                put("single", createBlob?.let { buildJsonObject { put("create", it) } } ?: JsonNull)
                put("multipart", multipart?.let { buildJsonObject { put("init", it.initiate); put("presign", it.presign); put("complete", it.complete); put("abort", it.abort) } } ?: JsonNull)
                put("mutation", "")
                put("signed_id_variable", "signedId")
                put("blob_id_variable", JsonNull)
            })
        }.jsonText()
}

suspend fun <Model : DBModel> ModelDecoder<Model>.uploadCreate(items: List<UploadItem>, request: UploadBatchRequest): List<Model> =
    DBlayer.database.uploadCreateBatch(modelName, JsonArray(items.map { it.value }).jsonText(), request.json).map { read(it) }

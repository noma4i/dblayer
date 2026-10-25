package com.noma4i.dblayer

import java.io.File
import java.net.URI
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long

data class FileInfo(val id: String, val size: Long, val name: String?, val mime: String?, val sha256: String?) {
    companion object {
        internal fun of(json: JsonElement): FileInfo {
            val members = json as? JsonObject ?: throw IllegalArgumentException("File description needs id and size")
            fun text(key: String) = members[key]?.jsonPrimitive?.contentOrNull
            val id = text("id") ?: throw IllegalArgumentException("File description needs id and size")
            val size = members["size"]?.jsonPrimitive?.long ?: throw IllegalArgumentException("File description needs id and size")
            return FileInfo(id, size, text("name"), text("mime"), text("sha256"))
        }

        internal fun parse(text: String): FileInfo = of(dbJson.parseToJsonElement(text))
    }
}

class FileField<Model : DBModel>(val name: String)

suspend fun <Model : DBModel> Model.attach(field: FileField<Model>, data: ByteArray, name: String, mime: String): FileInfo =
    offCaller { FileInfo.parse(DBlayer.database.attach(modelName, id, field.name, data, name, mime)) }

suspend fun <Model : DBModel> Model.file(field: FileField<Model>): FileInfo? =
    offCaller { DBlayer.database.file(modelName, id, field.name)?.let(FileInfo::parse) }

suspend fun <Model : DBModel> Model.read(field: FileField<Model>): ByteArray? =
    offCaller { DBlayer.database.read(modelName, id, field.name) }

suspend fun <Model : DBModel> Model.url(field: FileField<Model>): URI? =
    offCaller { DBlayer.database.url(modelName, id, field.name)?.let { File(it).toURI() } }

suspend fun <Model : DBModel> Model.detach(field: FileField<Model>): Boolean =
    offCaller { DBlayer.database.detach(modelName, id, field.name) }

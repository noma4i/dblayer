package com.noma4i.dblayer

import java.io.File
import java.net.URI

data class FileInfo(val id: String, val size: Long, val name: String?, val mime: String?, val sha256: String?) {
    companion object {
        internal fun of(cells: List<Cell>): FileInfo {
            val r = CellReader(cells)
            return FileInfo(r.req(0, Cell::asText), r.req(1, Cell::asInt), r.opt(2, Cell::asText), r.opt(3, Cell::asText), r.opt(4, Cell::asText))
        }

        internal fun of(wire: WireFile): FileInfo = FileInfo(wire.id, wire.size.toLong(), wire.name, wire.mime, wire.sha256)
    }
}

class FileField<Model : DBModel>(val name: String)

suspend fun <Model : DBModel> Model.attach(field: FileField<Model>, data: ByteArray, name: String, mime: String, account: AccountScope? = null): FileInfo {
    return onAccount(account) { database, epoch -> FileInfo.of(database.attach(epoch, modelName, id, field.name, FileBytes(data, name, mime))) }
}

suspend fun <Model : DBModel> Model.file(field: FileField<Model>, account: AccountScope? = null): FileInfo? {
    return onAccount(account) { database, epoch -> database.file(epoch, modelName, id, field.name)?.let(FileInfo::of) }
}

suspend fun <Model : DBModel> Model.read(field: FileField<Model>, account: AccountScope? = null): ByteArray? {
    return onAccount(account) { database, epoch -> database.read(epoch, modelName, id, field.name) }
}

suspend fun <Model : DBModel> Model.url(field: FileField<Model>, account: AccountScope? = null): URI? {
    return onAccount(account) { database, epoch -> database.url(epoch, modelName, id, field.name)?.let { File(it).toURI() } }
}

suspend fun <Model : DBModel> Model.detach(field: FileField<Model>, account: AccountScope? = null): Boolean {
    return onAccount(account) { database, epoch -> database.detach(epoch, modelName, id, field.name) }
}

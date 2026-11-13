package com.noma4i.dblayer

import java.io.File
import java.net.URI

data class FileInfo(val id: String, val size: ULong, val name: String?, val mime: String?, val sha256: String?) {
    companion object {
        internal fun of(cells: List<Cell>): FileInfo {
            val r = CellReader(cells)
            return FileInfo(r.req(0, Cell::asText), r.req(1, Cell::asInt).also { if (it < 0) throw DbException.Invalid("the file size is negative") }.toULong(), r.opt(2, Cell::asText), r.opt(3, Cell::asText), r.opt(4, Cell::asText))
        }

        internal fun of(wire: WireFile): FileInfo = FileInfo(wire.id, wire.size, wire.name, wire.mime, wire.sha256)
    }
}

class FileField<Model : DBModel>(val name: String)

suspend fun <Model : DBModel> Model.attach(field: FileField<Model>, source: File, name: String, mime: String, account: AccountScope? = null): FileInfo {
    return onAccountOf(account) { _, database, epoch -> FileInfo.of(database.attachFile(epoch, modelName, id, field.name, source.absolutePath, name, mime)) }
}

suspend fun <Model : DBModel> Model.file(field: FileField<Model>, account: AccountScope? = null): FileInfo? {
    return onAccountRead(account) { database, epoch -> database.file(epoch, modelName, id, field.name)?.let(FileInfo::of) }
}

suspend fun <Model : DBModel> Model.url(field: FileField<Model>, account: AccountScope? = null): URI? {
    return onAccountRead(account) { database, epoch -> database.url(epoch, modelName, id, field.name)?.let { File(it).toURI() } }
}

suspend fun <Model : DBModel> Model.detach(field: FileField<Model>, account: AccountScope? = null): Boolean {
    return onAccountOf(account) { _, database, epoch -> database.detach(epoch, modelName, id, field.name) }
}

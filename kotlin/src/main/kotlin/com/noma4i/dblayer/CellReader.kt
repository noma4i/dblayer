package com.noma4i.dblayer

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

fun Cell.asText(): String? = (this as? Cell.Text)?.value
fun Cell.asId(): String? = (this as? Cell.Text)?.value
fun Cell.asBool(): Boolean? = (this as? Cell.Bool)?.value
fun Cell.asInt(): Long? = (this as? Cell.Int)?.value
fun Cell.asDouble(): Double? = (this as? Cell.Real)?.value

fun Cell.asJson(): JsonElement = when (this) {
    Cell.Null -> JsonNull
    is Cell.Bool -> JsonPrimitive(value)
    is Cell.Int -> JsonPrimitive(value)
    is Cell.Real -> JsonPrimitive(value)
    is Cell.Text -> JsonPrimitive(value)
    is Cell.Seq -> JsonArray(items.map { it.asJson() })
    is Cell.Keyed -> JsonObject(entries.groupBy { it.key }.mapValues { (key, same) -> same.singleOrNull()?.value?.asJson() ?: throw DbException.Invalid("the keyed cell repeats the key $key") })
}

class CellReader(private val cells: List<Cell>) {
    private fun cell(at: Int): Cell = cells.getOrNull(at) ?: throw DbException.Invalid("the row has no cell $at")

    private fun wrong(at: Int) = DbException.Invalid("the cell $at has the wrong type")

    private fun <T> required(value: T?, at: Int): T = value ?: throw DbException.Invalid("the cell $at is empty")

    fun <T> opt(at: Int, convert: (Cell) -> T?): T? {
        val cell = cell(at)
        if (cell == Cell.Null) return null
        return convert(cell) ?: throw wrong(at)
    }

    fun <T> req(at: Int, convert: (Cell) -> T?): T = required(opt(at, convert), at)

    fun <T> listOpt(at: Int, convert: (Cell) -> T?): List<T>? {
        val cell = cell(at)
        if (cell == Cell.Null) return null
        val items = (cell as? Cell.Seq)?.items ?: throw wrong(at)
        return items.map { convert(it) ?: throw wrong(at) }
    }

    fun <T> list(at: Int, convert: (Cell) -> T?): List<T> = required(listOpt(at, convert), at)

    fun <T> objOpt(at: Int, make: (List<Cell>) -> T): T? {
        val cell = cell(at)
        if (cell == Cell.Null) return null
        return make((cell as? Cell.Seq)?.items ?: throw wrong(at))
    }

    fun <T> obj(at: Int, make: (List<Cell>) -> T): T = required(objOpt(at, make), at)

    fun <T> arrOpt(at: Int, make: (List<Cell>) -> T): List<T>? {
        val cell = cell(at)
        if (cell == Cell.Null) return null
        val items = (cell as? Cell.Seq)?.items ?: throw wrong(at)
        return items.map { make((it as? Cell.Seq)?.items ?: throw wrong(at)) }
    }

    fun <T> arr(at: Int, make: (List<Cell>) -> T): List<T> = required(arrOpt(at, make), at)

    fun jsonOpt(at: Int): JsonElement? = cell(at).let { if (it == Cell.Null) null else it.asJson() }

    fun json(at: Int): JsonElement = cell(at).asJson()

    fun fileOpt(at: Int): FileInfo? = objOpt(at, FileInfo::of)

    fun file(at: Int): FileInfo = required(fileOpt(at), at)
}

interface ModelDecoder<Model : DBModel> {
    val modelName: String
    val layout: String
    fun decode(row: WireRow): Model
}

fun <Model : DBModel> ModelDecoder<Model>.read(row: WireRow): Model {
    if (row.layout != layout) throw DbException.Invalid("the generated $modelName does not match the library: layout $layout against ${row.layout}")
    return decode(row)
}

fun SyncInfo.Companion.of(wire: WireSync): SyncInfo = SyncInfo(
    state = when (wire.state) {
        WireSyncState.SYNCED -> SyncState.Synced
        WireSyncState.PENDING -> SyncState.Pending
        WireSyncState.SENDING -> SyncState.Sending
        WireSyncState.FAILED -> SyncState.Failed
    },
    actions = wire.actions,
    dirtyFields = wire.dirtyFields,
    error = wire.error?.let { OperationFailure(it.kind, it.sessionCode, it.requestSize, it.requestLimit, it.message, it.extensions, it.responseLimit, it.reason) },
    attempts = wire.attempts.toInt(),
    since = wire.since,
)

fun UploadInfo.Companion.of(wire: WireUpload): UploadInfo = UploadInfo(
    status = when (wire.status) {
        WireUploadStatus.WAITING -> UploadStatus.Waiting
        WireUploadStatus.PREPARING -> UploadStatus.Preparing
        WireUploadStatus.QUEUED -> UploadStatus.Queued
        WireUploadStatus.UPLOADING -> UploadStatus.Uploading
        WireUploadStatus.SENDING -> UploadStatus.Sending
        WireUploadStatus.DONE -> UploadStatus.Done
        WireUploadStatus.FAILED -> UploadStatus.Failed
    },
    stage = wire.stage,
    sentBytes = wire.sentBytes,
    totalBytes = wire.totalBytes,
    progress = wire.progress,
    batchProgress = wire.batchProgress,
    waitReason = wire.waitReason,
    attempts = wire.attempts.toInt(),
    error = wire.error?.let { OperationFailure(it.kind, it.sessionCode, it.requestSize, it.requestLimit, it.message, it.extensions, it.responseLimit, it.reason) },
    localUrl = wire.localUrl,
)

package com.noma4i.dblayer

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

data class IndexedRow<Model : DBModel>(val index: Int, val row: Model)

enum class ListChangeKind { Delta, Replaced }

data class ListChange<Model : DBModel>(
    val kind: ListChangeKind,
    val inserted: List<IndexedRow<Model>>,
    val updated: List<IndexedRow<Model>>,
    val removed: List<String>,
    val moved: List<Placement>,
    val replaced: List<Replacement>,
    val revision: Long,
    val size: Int,
    val failure: String?,
) {
    internal companion object {
        fun <Model : DBModel> decode(change: LiveChange, decoder: ModelDecoder<Model>): ListChange<Model> {
            var failure = change.error?.toString()
            val rows = change.rows.mapIndexed { position, wire ->
                runCatching { IndexedRow(if (change.full) position else change.rowIndexes[position].toInt(), decoder.read(wire)) }
                    .onFailure { failure = it.toString() }.getOrNull()
            }
            return ListChange(if (change.full) ListChangeKind.Replaced else ListChangeKind.Delta, change.inserted.mapNotNull { rows[it.toInt()] }, change.updated.mapNotNull { rows[it.toInt()] },
                change.removed, change.moved, change.renamed, change.revision, change.size.toInt(), failure)
        }
    }
}

val <Model : DBModel> Query<Model>.changes: Flow<ListChange<Model>>
    get() = flow {
        val queue = DBlayer.database.changes(json)
        try {
            while (true) {
                val change = queue.next() ?: break
                emit(ListChange.decode(change, decoder))
            }
        } finally {
            queue.cancel()
        }
    }

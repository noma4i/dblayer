package com.noma4i.dblayer

import java.util.concurrent.atomic.AtomicReference

class ListUpdate<Model>(val rows: List<Model>, val edits: List<ListEdit<Model>>?, val revision: ULong)

internal class ListUpdates<Model> {
    private class Bundle<Model>(val rows: List<Model>, val edits: List<ListEdit<Model>>?, val revision: ULong)

    private val bundle = AtomicReference(Bundle<Model>(emptyList(), null, 0uL))

    fun publish(snapshot: Snapshot<Model>) {
        bundle.updateAndGet { Bundle(snapshot.rows, ListEdit.composing(it.edits, snapshot.edits, snapshot.rows.size), snapshot.seq) }
    }

    fun take(): ListUpdate<Model> {
        val taken = bundle.getAndUpdate { Bundle(it.rows, emptyList(), it.revision) }
        return ListUpdate(taken.rows, taken.edits, taken.revision)
    }
}

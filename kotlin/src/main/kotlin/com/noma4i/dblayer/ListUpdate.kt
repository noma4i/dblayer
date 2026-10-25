package com.noma4i.dblayer

import java.util.concurrent.atomic.AtomicReference

/** The rows of a list with the edits since the last take, from one revision. `edits` is null when the rows cannot be reached by edits: replace the mirror with `rows`. Empty `edits` mean no change. `rows` never changes after it is returned. */
class ListUpdate<Model>(val rows: List<Model>, val edits: List<ListEdit<Model>>?, val revision: ULong)

/** The one owner of what a consumer takes: the publisher and the consumer swap one immutable bundle by compare-and-set, so neither waits for the other and no edit is lost or taken twice. */
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

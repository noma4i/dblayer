package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch

data class WindowEdges(val hasBefore: Boolean, val hasAfter: Boolean, val total: Int?) {
    internal constructor(edge: LiveWindowEdge) : this(edge.hasBefore, edge.hasAfter, edge.total?.toInt())
}

internal class WindowControl(tail: Int) {
    private var subscription: LiveSubscription? = null
    private var rows = tail

    val size: Int get() = synchronized(this) { rows }

    fun attach(next: LiveSubscription) = synchronized(this) { subscription = next }
    fun shown(count: Int) = synchronized(this) { rows = maxOf(rows, count) }

    fun grow(before: Int, after: Int) {
        val current = synchronized(this) { subscription }
        if (before > 0) current?.expandBefore(before.toUInt())
        if (after > 0) current?.expandAfter(after.toUInt())
    }
}

class Window<Model : DBModel> internal constructor(private val query: Query<Model>, tail: Int) {
    private val control = WindowControl(tail)

    /** Delivers the rows of the window with `edits` in window positions, the edges and the total of the query. A row outside the window is not decoded. */
    val snapshots: LiveSource<Snapshot<Model>>
        get() = listSource(Snapshot(emptyList(), emptyMap()), query.decoder, { older, newer -> Snapshot.merging(older, newer) }, { listener ->
            val subscription = DBlayer.database.liveWindow(query.json, control.size.toUInt(), listener)
            control.attach(subscription)
            subscription
        }) { applied, change ->
            control.shown(applied.rows.size)
            Snapshot(applied.rows, change.replacedIds(), applied.reordered, applied.failure, applied.edits, change.window?.let(::WindowEdges))
        }

    fun expand(before: Int = 0, after: Int = 0) = control.grow(before, after)
}

class LiveWindow<Model : DBModel>(private val window: Window<Model>, scope: CoroutineScope) {
    private val mutableRows = MutableStateFlow<List<Model>>(emptyList())
    private val mutableIds = MutableStateFlow<List<String>>(emptyList())
    private val mutableReplaced = MutableStateFlow<Map<String, String>>(emptyMap())
    private val mutableEdges = MutableStateFlow(WindowEdges(false, false, null))
    private val mutableFailure = MutableStateFlow<String?>(null)
    private var pending: List<ListEdit<Model>>? = null

    init {
        scope.launch {
            try {
                window.snapshots.flow.collect { snapshot ->
                    synchronized(this@LiveWindow) { pending = ListEdit.composing(pending, snapshot.edits, snapshot.rows.size) }
                    mutableReplaced.value = snapshot.replaced
                    mutableFailure.value = snapshot.failure
                    snapshot.window?.let { mutableEdges.value = it }
                    mutableRows.value = snapshot.rows
                    if (snapshot.reordered) mutableIds.value = snapshot.rows.map { it.id }
                }
            } catch (error: IllegalStateException) {
                mutableFailure.value = error.message
            }
        }
    }

    val rows: StateFlow<List<Model>> get() = mutableRows
    val ids: StateFlow<List<String>> get() = mutableIds
    val replaced: StateFlow<Map<String, String>> get() = mutableReplaced
    val edges: StateFlow<WindowEdges> get() = mutableEdges
    val failure: StateFlow<String?> get() = mutableFailure

    fun takeEdits(): List<ListEdit<Model>>? = synchronized(this) { pending.also { pending = emptyList() } }

    fun expand(before: Int = 0, after: Int = 0) = window.expand(before, after)
}

package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import java.lang.ref.WeakReference

data class WindowEdges(val hasBefore: Boolean, val hasAfter: Boolean, val total: Long?) {
    internal constructor(edge: LiveWindowEdge) : this(edge.hasBefore, edge.hasAfter, edge.total?.toLong())
}

internal class WindowControl(private val tail: Int, val max: Int?) {
    private val subscriptions = mutableListOf<WeakReference<LiveSubscription>>()
    private var rows = tail

    val size: Int get() = synchronized(this) { rows }

    fun attach(next: LiveSubscription) = synchronized(this) {
        subscriptions.removeAll { it.get() == null }
        subscriptions.add(WeakReference(next))
        Unit
    }
    fun shown(count: Int) = synchronized(this) {
        rows = maxOf(rows, count)
        if (max != null && rows > max) rows = tail
    }

    fun grow(before: Int, after: Int) {
        val current = synchronized(this) { subscriptions.mapNotNull { it.get() } }
        for (subscription in current) {
            if (before > 0) subscription.expandBefore(before.toUInt())
            if (after > 0) subscription.expandAfter(after.toUInt())
        }
    }
}

class Window<Model : DBModel> internal constructor(private val query: Query<Model>, tail: Int, max: Int?) {
    private val control = WindowControl(tail, max)

    /** Delivers the rows of the window with `edits` in window positions, the edges and the total of the query. A row outside the window is not decoded. */
    val snapshots: LiveSource<Snapshot<Model>>
        get() = listSource(Snapshot(emptyList(), emptyMap()), query.decoder, { older, newer -> Snapshot.merging(older, newer) }, { listener ->
            val subscription = DBlayer.database.liveWindow(query.json, control.size.toUInt(), control.max?.toUInt(), listener)
            control.attach(subscription)
            subscription
        }) { applied, change ->
            control.shown(applied.rows.size)
            Snapshot.of(applied, change)
        }

    fun expand(before: Int = 0, after: Int = 0) = control.grow(before, after)

    fun <Target : DBModel> related(association: Association<Model, Target>) = RelatedWindow(association.name, association.decoder, { query.json }, control)
}

class RelatedWindow<Target : DBModel> internal constructor(
    private val name: String,
    private val decoder: ModelDecoder<Target>,
    private val sourceJson: () -> String,
    private val control: WindowControl,
) {
    private fun connect(listener: QueryListener): LiveSubscription {
        val subscription = DBlayer.database.liveWindowRelated(sourceJson(), control.size.toUInt(), control.max?.toUInt(), name, listener)
        control.attach(subscription)
        return subscription
    }

    val live: LiveSource<List<Target>>
        get() = listSource(emptyList(), decoder, open = { listener -> connect(listener) }) { applied, _ -> applied.rows }

    val snapshots: LiveSource<Snapshot<Target>>
        get() = listSource(Snapshot(emptyList(), emptyMap()), decoder, { older, newer -> Snapshot.merging(older, newer) }, { listener -> connect(listener) }) { applied, change -> Snapshot.of(applied, change) }
}

class LiveWindow<Model : DBModel>(private val window: Window<Model>, scope: CoroutineScope) {
    private val mutableRows = MutableStateFlow<List<Model>>(emptyList())
    private val mutableIds = MutableStateFlow<List<String>>(emptyList())
    private val mutableReplaced = MutableStateFlow<Map<String, String>>(emptyMap())
    private val mutableEdges = MutableStateFlow(WindowEdges(false, false, null))
    private val mutableFailure = MutableStateFlow<Throwable?>(null)
    private val updates = ListUpdates<Model>()

    init {
        scope.launch(Dispatchers.Default) {
            try {
                window.snapshots.flow.collect { snapshot ->
                    updates.publish(snapshot)
                    if (snapshot.reordered) mutableIds.value = snapshot.rows.map { row -> row.id }
                    mutableReplaced.value = snapshot.replaced
                    mutableFailure.value = snapshot.error
                    snapshot.window?.let { mutableEdges.value = it }
                    mutableRows.value = snapshot.rows
                }
            } catch (error: IllegalStateException) {
                mutableFailure.value = error
            }
        }
    }

    val rows: StateFlow<List<Model>> get() = mutableRows
    val ids: StateFlow<List<String>> get() = mutableIds
    val replaced: StateFlow<Map<String, String>> get() = mutableReplaced
    val edges: StateFlow<WindowEdges> get() = mutableEdges
    val failure: StateFlow<Throwable?> get() = mutableFailure

    fun takeUpdate(): ListUpdate<Model> = updates.take()

    fun expand(before: Int = 0, after: Int = 0) = window.expand(before, after)
}

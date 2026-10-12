package com.noma4i.dblayer

import kotlinx.serialization.KSerializer
import java.util.concurrent.atomic.AtomicInteger

private val decodeLog = java.util.logging.Logger.getLogger("DBlayer")

internal class ListState<Model>(val rows: List<Model>, val reordered: Boolean, val desynced: Boolean = false)

internal class ListFeed<Model : DBModel>(private val serializer: KSerializer<Model>) {
    private val rows = ArrayList<Model>()
    private val decoded = AtomicInteger()
    private val desynced = AtomicInteger()
    private var dropped = 0

    val decodedRows: Int get() = decoded.get()
    val desyncs: Int get() = desynced.get()

    @Synchronized
    fun reset() {
        rows.clear()
        dropped = 0
    }

    @Synchronized
    fun apply(change: LiveChange): ListState<Model> {
        if (change.error != null) return ListState(rows.toList(), false)
        var failed = 0
        val carried = change.rows.mapIndexedNotNull { position, text ->
            decoded.incrementAndGet()
            val row = runCatching { dbJson.decodeFromString(serializer, text) }
                .onFailure { failed++; decodeLog.severe("Row of ${serializer.descriptor.serialName} failed to decode: $it") }
                .getOrNull()
            row?.let { it to if (change.full) position else change.rowIndexes[position].toInt() }
        }
        if (change.full) {
            rows.clear()
            rows.addAll(carried.map { it.first })
            dropped = failed
            return ListState(rows.toList(), true)
        }
        val reordered = change.removed.isNotEmpty() || change.placed.isNotEmpty() || change.replaced.isNotEmpty()
        if (failed > 0 || !applyDelta(change, carried)) {
            desynced.incrementAndGet()
            return ListState(rows.toList(), reordered, desynced = true)
        }
        return ListState(rows.toList(), reordered)
    }

    private fun applyDelta(change: LiveChange, carried: List<Pair<Model, Int>>): Boolean {
        val byId = carried.associate { it.first.id to it.first }
        for (pair in change.replaced) {
            val at = rows.indexOfFirst { it.id == pair.from }
            val row = byId[pair.to]
            if (at < 0 || row == null) return false
            rows[at] = row
        }
        if (change.removed.isNotEmpty()) {
            val gone = change.removed.toSet()
            val before = rows.size
            rows.removeAll { it.id in gone }
            if (before - rows.size != gone.size) return false
        }
        val taken = HashMap<String, Model>()
        for (place in change.placed) {
            val at = rows.indexOfFirst { it.id == place.id }
            if (at >= 0) taken[place.id] = rows.removeAt(at)
        }
        for (place in change.placed) {
            val row = taken[place.id] ?: byId[place.id]
            if (row == null || place.index.toInt() > rows.size) return false
            rows.add(place.index.toInt(), row)
        }
        for ((row, index) in carried) {
            if (index >= rows.size) return false
            rows[index] = row
        }
        return rows.size == change.size.toInt() - dropped
    }
}

internal class ListSession<Model : DBModel, Value>(
    serializer: KSerializer<Model>,
    private val open: (QueryListener) -> () -> Unit,
    private val emit: (Value) -> Unit,
    private val map: (ListState<Model>, LiveChange) -> Value,
) {
    private val feed = ListFeed(serializer)
    private var cancelSubscription: (() -> Unit)? = null
    private var cancelled = false
    private var generation = 0
    private val reopens = AtomicInteger()

    val reopened: Int get() = reopens.get()
    val desyncs: Int get() = feed.desyncs

    fun start() {
        val current = synchronized(this) { generation }
        val made = open(ChangeListener { receive(it, current) })
        val keep = synchronized(this) { if (cancelled || generation != current) false else { cancelSubscription = made; true } }
        if (!keep) made()
    }

    fun cancel() {
        val current = synchronized(this) { cancelled = true; cancelSubscription.also { cancelSubscription = null } }
        current?.invoke()
    }

    private fun receive(change: LiveChange, current: Int) {
        if (synchronized(this) { cancelled || generation != current }) return
        val applied = feed.apply(change)
        if (applied.desynced) reopen(current) else emit(map(applied, change))
    }

    private fun reopen(current: Int) {
        val old = synchronized(this) {
            if (cancelled || generation != current) return
            generation++
            reopens.incrementAndGet()
            cancelSubscription.also { cancelSubscription = null }
        }
        feed.reset()
        old?.invoke()
        kotlin.concurrent.thread(isDaemon = true) { runCatching { start() } }
    }
}

internal fun <Model : DBModel, Value> listSource(
    initial: Value,
    serializer: KSerializer<Model>,
    merge: ((Value, Value) -> Value)? = null,
    open: (QueryListener) -> LiveSubscription,
    map: (ListState<Model>, LiveChange) -> Value,
): LiveSource<Value> = LiveSource(initial, merge) { emit ->
    val session = ListSession(serializer, { listener -> val subscription = open(listener); val cancel: () -> Unit = { subscription.cancel() }; cancel }, emit, map)
    session.start()
    val cancel: () -> Unit = { session.cancel() }
    cancel
}

internal fun LiveChange.replacedIds(): Map<String, String> = replaced.associate { it.to to it.from }

package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.util.concurrent.atomic.AtomicInteger

private val decodeLog = java.util.logging.Logger.getLogger("DBlayer")
private val reopening = CoroutineScope(SupervisorJob() + Dispatchers.Default)

internal class ListState<Model>(val rows: List<Model>, val reordered: Boolean, val desynced: Boolean = false, val failure: String? = null)

internal class ListFeed<Model : DBModel>(private val serializer: KSerializer<Model>) {
    private class Slot<Model>(val id: String, val row: Model?)

    private val slots = ArrayList<Slot<Model>>()
    private val decoded = AtomicInteger()
    private val undecodable = AtomicInteger()
    private val desynced = AtomicInteger()

    val decodedRows: Int get() = decoded.get()
    val undecodableRows: Int get() = undecodable.get()
    val desyncs: Int get() = desynced.get()

    @get:Synchronized
    val rows: List<Model> get() = slots.mapNotNull { it.row }

    @Synchronized
    fun apply(change: LiveChange): ListState<Model> {
        if (change.error != null) return ListState(rows, false)
        val carried = change.rows.mapIndexed { position, text ->
            decoded.incrementAndGet()
            val index = if (change.full) position else change.rowIndexes[position].toInt()
            val row = runCatching { dbJson.decodeFromString(serializer, text) }
                .onFailure { undecodable.incrementAndGet(); decodeLog.severe("Row of ${serializer.descriptor.serialName} failed to decode: $it") }
                .getOrNull()
            val id = row?.id ?: runCatching { dbJson.parseToJsonElement(text).jsonObject["id"]!!.jsonPrimitive.content }.getOrDefault("undecodable-$position")
            Slot(id, row) to index
        }
        if (change.full) {
            slots.clear()
            slots.addAll(carried.map { it.first })
            return ListState(rows, true)
        }
        val reordered = change.removed.isNotEmpty() || change.placed.isNotEmpty() || change.replaced.isNotEmpty()
        val next = ArrayList(slots)
        if (!applyDelta(change, carried, next)) {
            desynced.incrementAndGet()
            return ListState(rows, reordered, desynced = true)
        }
        slots.clear()
        slots.addAll(next)
        return ListState(rows, reordered)
    }

    private fun applyDelta(change: LiveChange, carried: List<Pair<Slot<Model>, Int>>, slots: ArrayList<Slot<Model>>): Boolean {
        val byId = carried.associate { it.first.id to it.first }
        for (pair in change.replaced) {
            val at = slots.indexOfFirst { it.id == pair.from }
            val slot = byId[pair.to]
            if (at < 0 || slot == null) return false
            slots[at] = slot
        }
        if (change.removed.isNotEmpty()) {
            val gone = change.removed.toSet()
            val before = slots.size
            slots.removeAll { it.id in gone }
            if (before - slots.size != gone.size) return false
        }
        val taken = HashMap<String, Slot<Model>>()
        for (place in change.placed) {
            val at = slots.indexOfFirst { it.id == place.id }
            if (at >= 0) taken[place.id] = slots.removeAt(at)
        }
        for (place in change.placed) {
            val slot = taken[place.id] ?: byId[place.id]
            if (slot == null || place.index.toInt() > slots.size) return false
            slots.add(place.index.toInt(), slot)
        }
        for ((slot, index) in carried) {
            if (index >= slots.size) return false
            slots[index] = slot
        }
        return slots.size == change.size.toInt()
    }
}

internal class ListSession<Model : DBModel, Value>(
    serializer: KSerializer<Model>,
    private val open: (QueryListener) -> () -> Unit,
    private val emit: (Value) -> Unit,
    private val map: (ListState<Model>, LiveChange) -> Value,
    private val maxReopens: Int = 2,
) {
    private val feed = ListFeed(serializer)
    private var cancelSubscription: (() -> Unit)? = null
    private var cancelled = false
    private var stalled = false
    private var generation = 0
    private var reopensInRow = 0
    private val reopens = AtomicInteger()

    val reopened: Int get() = reopens.get()
    val desyncs: Int get() = feed.desyncs
    val isStalled: Boolean get() = synchronized(this) { stalled }

    fun start() {
        val current = synchronized(this) { generation }
        val made = open(ChangeListener { receive(it, current) })
        val keep = synchronized(this) { if (cancelled || stalled || generation != current) false else { cancelSubscription = made; true } }
        if (!keep) made()
    }

    fun cancel() {
        val current = synchronized(this) { cancelled = true; cancelSubscription.also { cancelSubscription = null } }
        current?.invoke()
    }

    private fun receive(change: LiveChange, current: Int) {
        if (synchronized(this) { cancelled || stalled || generation != current }) return
        val applied = feed.apply(change)
        if (applied.desynced) { reopen(current); return }
        if (!change.full) synchronized(this) { reopensInRow = 0 }
        emit(map(applied, change))
    }

    private fun reopen(current: Int) {
        var old: (() -> Unit)? = null
        var stall = false
        synchronized(this) {
            if (cancelled || stalled || generation != current) return
            if (reopensInRow >= maxReopens) stall = true else {
                generation++
                reopens.incrementAndGet()
                reopensInRow++
                old = cancelSubscription
                cancelSubscription = null
            }
        }
        if (stall) { stall("the list is out of sync with the database and a new subscription did not fix it"); return }
        old?.invoke()
        reopening.launch { restart() }
    }

    private fun restart() {
        while (true) {
            val failure = runCatching { start() }.exceptionOrNull() ?: return
            val again = synchronized(this) { if (cancelled || stalled || reopensInRow >= maxReopens) false else { reopensInRow++; true } }
            if (!again) { stall("the subscription could not be opened again: $failure"); return }
        }
    }

    private fun stall(reason: String) {
        val current = synchronized(this) { if (cancelled || stalled) return; stalled = true; cancelSubscription.also { cancelSubscription = null } }
        current?.invoke()
        decodeLog.severe("List stopped: $reason")
        emit(map(ListState(feed.rows, false, failure = reason), LiveChange(0L, false, 0u, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), null, reason)))
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

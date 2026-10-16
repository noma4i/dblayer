package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.util.concurrent.atomic.AtomicInteger

private val decodeLog = java.util.logging.Logger.getLogger("DBlayer")
private val reopening = CoroutineScope(SupervisorJob() + Dispatchers.Default)

internal class ListState<Model>(
    val rows: List<Model>,
    val reordered: Boolean,
    val desynced: Boolean = false,
    val failure: String? = null,
    /** The steps from the rows of the previous delivery to `rows`, or null when the reader has to take `rows` whole. */
    val edits: List<ListEdit<Model>>? = null,
)

internal class ListFeed<Model : DBModel>(private val decoder: ModelDecoder<Model>) {
    private class Slot<Model>(val id: String, val row: Model?)

    private sealed class Undo<Model> {
        class Restore<Model>(val index: Int, val slot: Slot<Model>) : Undo<Model>()
        class Reinsert<Model>(val index: Int, val slot: Slot<Model>) : Undo<Model>()
        class Drop<Model>(val index: Int) : Undo<Model>()
    }

    private val slots = ArrayList<Slot<Model>>()
    private var loaded = ArrayList<Model>()
    private var missing = 0
    private val decoded = AtomicInteger()
    private val undecodable = AtomicInteger()
    private val desynced = AtomicInteger()

    val decodedRows: Int get() = decoded.get()
    val undecodableRows: Int get() = undecodable.get()
    val desyncs: Int get() = desynced.get()

    @get:Synchronized
    val rows: List<Model> get() = current()

    private fun current(): List<Model> = if (missing == 0) ArrayList(loaded) else slots.mapNotNull { it.row }

    @OptIn(ExperimentalSerializationApi::class)
    @Synchronized
    fun apply(change: LiveChange): ListState<Model> {
        if (change.error != null) return ListState(current(), false)
        val carried = change.rows.mapIndexed { position, wire ->
            decoded.incrementAndGet()
            val index = if (change.full) position else change.rowIndexes[position].toInt()
            val row = runCatching { decoder.read(wire) }
                .onFailure { undecodable.incrementAndGet(); decodeLog.severe("Row of ${decoder.modelName} failed to decode: $it") }
                .getOrNull()
            Slot(wire.id, row) to index
        }
        if (change.full) {
            slots.clear()
            slots.addAll(carried.map { it.first })
            missing = slots.count { it.row == null }
            loaded = if (missing == 0) ArrayList(slots.map { it.row!! }) else ArrayList()
            return ListState(current(), true)
        }
        val reordered = change.removed.isNotEmpty() || change.placed.isNotEmpty() || change.replaced.isNotEmpty()
        val held = missing == 0
        val edits = ArrayList<ListEdit<Model>>()
        if (!applyDelta(change, carried, edits)) {
            desynced.incrementAndGet()
            return ListState(current(), reordered, desynced = true)
        }
        if (!held || missing != 0 || edits.size > slots.size) {
            loaded = if (missing == 0) ArrayList(slots.map { it.row!! }) else ArrayList()
            return ListState(current(), reordered)
        }
        loaded.replay(edits)
        return ListState(current(), reordered, edits = edits)
    }

    private fun applyDelta(change: LiveChange, carried: List<Pair<Slot<Model>, Int>>, edits: MutableList<ListEdit<Model>>): Boolean {
        val undo = ArrayList<Undo<Model>>()
        val missingBefore = missing
        fun fail(): Boolean {
            for (step in undo.asReversed()) when (step) {
                is Undo.Restore -> slots[step.index] = step.slot
                is Undo.Reinsert -> slots.add(step.index, step.slot)
                is Undo.Drop -> slots.removeAt(step.index)
            }
            missing = missingBefore
            return false
        }
        fun write(index: Int, slot: Slot<Model>) {
            val old = slots[index]
            undo.add(Undo.Restore(index, old))
            if (old.row == null) missing--
            slots[index] = slot
            if (slot.row == null) missing++ else edits.add(ListEdit.Rewrite(index, slot.row))
        }
        fun take(index: Int): Slot<Model> {
            val slot = slots.removeAt(index)
            undo.add(Undo.Reinsert(index, slot))
            if (slot.row == null) missing--
            edits.add(ListEdit.Remove(index, slot.id))
            return slot
        }
        fun put(index: Int, slot: Slot<Model>) {
            slots.add(index, slot)
            undo.add(Undo.Drop(index))
            if (slot.row == null) missing++ else edits.add(ListEdit.Insert(index, slot.row))
        }
        val byId = carried.associate { it.first.id to it.first }
        val landed = HashSet<String>()
        for (pair in change.replaced) {
            val at = slots.indexOfFirst { it.id == pair.from }
            val slot = byId[pair.to]
            if (at < 0 || slot == null) return fail()
            write(at, slot)
            landed.add(pair.to)
        }
        if (change.removed.isNotEmpty()) {
            val gone = change.removed.toSet()
            val found = slots.indices.filter { slots[it].id in gone }
            if (found.size != gone.size) return fail()
            for (index in found.asReversed()) take(index)
        }
        val taken = HashMap<String, Slot<Model>>()
        for (place in change.placed) {
            val at = slots.indexOfFirst { it.id == place.id }
            if (at >= 0) taken[place.id] = take(at)
        }
        for (place in change.placed) {
            val slot = taken[place.id] ?: byId[place.id]
            if (slot == null || place.index.toInt() > slots.size) return fail()
            put(place.index.toInt(), slot)
            if (taken[place.id] == null) landed.add(place.id)
        }
        for ((slot, index) in carried) {
            if (index >= slots.size) return fail()
            if (slot.id in landed && slots[index].id == slot.id) continue
            write(index, slot)
        }
        return if (slots.size == change.size.toInt()) true else fail()
    }
}

internal class ListSession<Model : DBModel, Value>(
    decoder: ModelDecoder<Model>,
    private val open: (QueryListener) -> () -> Unit,
    private val emit: (Value) -> Unit,
    private val map: (ListState<Model>, LiveChange) -> Value,
    private val maxReopens: Int = 2,
) {
    private val feed = ListFeed(decoder)
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
        emit(map(ListState(feed.rows, false, failure = reason), LiveChange(0L, false, 0u, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), null, reason)))
    }
}

internal fun <Model : DBModel, Value> listSource(
    initial: Value,
    decoder: ModelDecoder<Model>,
    merge: ((Value, Value) -> Value)? = null,
    open: (QueryListener) -> LiveSubscription,
    map: (ListState<Model>, LiveChange) -> Value,
): LiveSource<Value> = LiveSource(initial, merge) { emit ->
    val session = ListSession(decoder, { listener -> val subscription = open(listener); val cancel: () -> Unit = { subscription.cancel() }; cancel }, emit, map)
    session.start()
    val cancel: () -> Unit = { session.cancel() }
    cancel
}

internal fun LiveChange.replacedIds(): Map<String, String> = renamed.associate { it.to to it.from }

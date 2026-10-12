package com.noma4i.dblayer

import kotlinx.serialization.KSerializer
import java.util.concurrent.atomic.AtomicInteger

private val decodeLog = java.util.logging.Logger.getLogger("DBlayer")

/** The rows of one live list after a delivery, and whether the ids or their order changed. */
internal class ListState<Model>(val rows: List<Model>, val reordered: Boolean)

/** The rows of one live list, rebuilt from its deliveries. Only the rows that a delivery carries are decoded. */
internal class ListFeed<Model : DBModel>(private val serializer: KSerializer<Model>) {
    private val rows = ArrayList<Model>()
    private val decoded = AtomicInteger()

    /** Rows decoded so far. */
    val decodedRows: Int get() = decoded.get()

    @Synchronized
    fun apply(change: LiveChange): ListState<Model> {
        val carried = change.rows.mapIndexedNotNull { position, text ->
            decoded.incrementAndGet()
            runCatching { dbJson.decodeFromString(serializer, text) }
                .onFailure { decodeLog.severe("Row of ${serializer.descriptor.serialName} failed to decode: $it") }
                .getOrNull()
                ?.let { it to if (change.full) position else change.rowIndexes[position].toInt() }
        }
        if (change.full) {
            rows.clear()
            rows.addAll(carried.map { it.first })
            return ListState(rows.toList(), true)
        }
        val byId = carried.associate { it.first.id to it.first }
        for (pair in change.replaced) {
            val at = rows.indexOfFirst { it.id == pair.from }
            val row = byId[pair.to]
            if (at >= 0 && row != null) rows[at] = row
        }
        if (change.removed.isNotEmpty()) {
            val gone = change.removed.toSet()
            rows.removeAll { it.id in gone }
        }
        val taken = HashMap<String, Model>()
        for (place in change.placed) {
            val at = rows.indexOfFirst { it.id == place.id }
            if (at >= 0) taken[place.id] = rows.removeAt(at)
        }
        for (place in change.placed) {
            val row = taken[place.id] ?: byId[place.id] ?: continue
            rows.add(minOf(place.index.toInt(), rows.size), row)
        }
        for ((row, index) in carried) if (index < rows.size) rows[index] = row
        return ListState(rows.toList(), change.removed.isNotEmpty() || change.placed.isNotEmpty() || change.replaced.isNotEmpty())
    }
}

/** A live source of the values that one list produces: each delivery of the core is applied to the rows of the list, and `map` reads what it needs from them. */
internal fun <Model : DBModel, Value> listSource(
    initial: Value,
    serializer: KSerializer<Model>,
    merge: ((Value, Value) -> Value)? = null,
    open: (QueryListener) -> LiveSubscription,
    map: (ListFeed<Model>, LiveChange) -> Value,
): LiveSource<Value> = LiveSource(initial, merge) { emit ->
    val feed = ListFeed(serializer)
    val subscription = open(ChangeListener { emit(map(feed, it)) })
    val cancel: () -> Unit = { subscription.cancel() }
    cancel
}

/** Real id of a row that replaced a temporary one in this delivery, to the temporary id. */
internal fun LiveChange.replacedIds(): Map<String, String> = replaced.associate { it.to to it.from }

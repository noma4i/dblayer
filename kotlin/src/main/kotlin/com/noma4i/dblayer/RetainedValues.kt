package com.noma4i.dblayer

internal class Retained<Model>(val value: Model, val revision: Long, val bytes: ULong)

internal class RetainedValues {
    private val pool = RetentionPool()
    private val values = HashMap<ULong, Any>()

    @Synchronized
    fun <Model : DBModel> keep(model: String, id: String, retained: Retained<Model>, now: Long) {
        val change = pool.keep(model, id, retained.bytes, now.toULong())
        apply(change)
        change.selected?.let { values[it] = retained }
    }

    @Synchronized
    @Suppress("UNCHECKED_CAST")
    fun <Model : DBModel> take(model: String, id: String, now: Long): Retained<Model>? {
        val change = pool.take(model, id, now.toULong())
        apply(change)
        return change.selected?.let { values.remove(it) as Retained<Model>? }
    }

    @Synchronized
    fun count(model: String, now: Long): Int {
        val change = pool.count(model, now.toULong())
        apply(change)
        return change.count.toInt()
    }

    @Synchronized
    fun clear(model: String) = apply(pool.clear(model))

    private fun apply(change: RetentionChange) {
        change.evicted.forEach(values::remove)
    }
}

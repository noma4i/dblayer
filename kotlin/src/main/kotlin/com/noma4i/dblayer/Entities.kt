package com.noma4i.dblayer

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.serialization.KSerializer
import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

internal class EntityCell<Model : DBModel> {
    val value = MutableStateFlow<Model?>(null)
    @Volatile var stale = false
    @Volatile var revision = Long.MIN_VALUE
    @Volatile var epoch = 0

    @Synchronized
    fun accept(next: Model?, incoming: Long) {
        if (incoming < revision) return
        revision = incoming
        stale = false
        value.value = next
    }
}

/** The live value of one record. The library keeps it current while the object is reachable and lets go of the record when it is not. */
class Entity<Model : DBModel> internal constructor(val id: String, internal val cell: EntityCell<Model>) {
    /** The newest value, null while none has arrived or when the record does not exist. */
    val value: StateFlow<Model?> get() = cell.value
    /** The value comes from the retention of a released entity and no fresh delivery has replaced it yet. */
    val isStale: Boolean get() = cell.stale
}

internal interface EntitySource {
    fun retain(ids: List<String>)
    fun release(ids: List<String>)
}

internal class EntityDeliveryRows<Model>(val revision: Long, val rows: List<Model>, val removed: List<String>)

private class Retention<Model : DBModel>(val cell: EntityCell<Model>, val releasedAt: Long)

private class LiveRef<Model : DBModel>(entity: Entity<Model>, queue: ReferenceQueue<Entity<Model>>, val id: String, val cell: EntityCell<Model>) :
    WeakReference<Entity<Model>>(entity, queue)

private enum class Step { Retain, Release, Refresh }

/** The registry of one model: one [Entity] per id, one core observer for the whole model. */
class Entities<Model : DBModel> internal constructor(
    private val modelName: String,
    private val serializer: KSerializer<Model>,
    private val maxAge: Long = 120_000,
    private val maxCount: Int = 200,
    private val now: () -> Long = { System.nanoTime() / 1_000_000 },
    source: EntitySource? = null,
) {
    private val lock = Any()
    private val queue = ReferenceQueue<Entity<Model>>()
    private val live = HashMap<String, LiveRef<Model>>()
    private val kept = LinkedHashMap<String, Retention<Model>>()
    private val steps = HashMap<String, Step>()
    private var flushScheduled = false
    private var database: Database? = null
    private val followsDatabase = source == null
    private var epoch = 0
    private val core = CoreEntitySource(modelName, serializer) { delivery -> apply(delivery) }
    private val source: EntitySource = source ?: core

    internal val decodedRows: Int get() = core.decodedRows

    /** The entity of an id. Hold it for as long as the value is shown. The value is there at once when the id is live or retained. */
    fun entity(id: String): Entity<Model> = synchronized(lock) {
        follow(DBlayer.databaseOrNull)
        drain()
        live[id]?.get()?.let { return it }
        purge()
        val seed = kept.remove(id)
        val cell = seed?.cell ?: EntityCell()
        cell.stale = seed != null
        cell.epoch = epoch
        val made = Entity(id, cell)
        live[id] = LiveRef(made, queue, id, cell)
        steps[id] = if (seed != null || steps[id] == Step.Release) Step.Refresh else Step.Retain
        scheduleFlush()
        made
    }

    /** A newly opened database starts with no entity of the old one. */
    private fun follow(current: Database?) {
        if (!followsDatabase || database === current) return
        if (database != null) reset()
        database = current
    }

    fun entities(ids: List<String>): List<Entity<Model>> = ids.map(::entity)

    /** Drops every entity, lease and retained value. Called when the account ends. */
    fun reset() = synchronized(lock) {
        epoch += 1
        val all = live.keys + kept.keys
        live.values.forEach { it.cell.value.value = null }
        live.clear()
        kept.clear()
        all.forEach { steps[it] = Step.Release }
        scheduleFlush()
    }

    internal val retainedCount: Int get() = synchronized(lock) { drain(); purge(); kept.size }
    internal val liveCount: Int get() = synchronized(lock) { drain(); live.values.count { it.get() != null } }

    internal fun apply(delivery: EntityDeliveryRows<Model>) = synchronized(lock) {
        drain()
        delivery.rows.forEach { model -> live[model.id]?.takeIf { it.get() != null }?.cell?.accept(model, delivery.revision) }
        delivery.removed.forEach { id -> live[id]?.takeIf { it.get() != null }?.cell?.accept(null, delivery.revision) }
    }

    private fun drain() {
        while (true) {
            @Suppress("UNCHECKED_CAST")
            val ref = queue.poll() as LiveRef<Model>? ?: return
            if (live[ref.id] !== ref) continue
            live.remove(ref.id)
            if (ref.cell.epoch != epoch) continue
            steps[ref.id] = Step.Release
            if (ref.cell.value.value != null) keep(ref.id, ref.cell)
            scheduleFlush()
        }
    }

    private fun keep(id: String, cell: EntityCell<Model>) {
        kept.remove(id)
        kept[id] = Retention(cell, now())
        purge()
        if (kept.size > maxCount) kept.remove(kept.entries.minByOrNull { it.value.releasedAt }!!.key)
    }

    private fun purge() {
        val cutoff = now() - maxAge
        kept.entries.removeAll { it.value.releasedAt <= cutoff }
    }

    private fun scheduleFlush() {
        if (flushScheduled) return
        flushScheduled = true
        flusher.execute { flush() }
    }

    private fun flush() {
        val pending = synchronized(lock) {
            flushScheduled = false
            drain()
            HashMap(steps).also { steps.clear() }
        }
        val release = pending.filterValues { it != Step.Retain }.keys.toList()
        val retain = pending.filterValues { it != Step.Release }.keys.toList()
        if (release.isNotEmpty()) source.release(release)
        if (retain.isNotEmpty()) source.retain(retain)
    }

    companion object {
        private val flusher = Executors.newSingleThreadExecutor { task -> Thread(task, "dblayer-entities").apply { isDaemon = true } }
    }
}

/** Opens the one core observer of a model on first use. */
internal class CoreEntitySource<Model : DBModel>(
    private val modelName: String,
    private val serializer: KSerializer<Model>,
    private val deliver: (EntityDeliveryRows<Model>) -> Unit,
) : EntitySource, EntityListener {
    private var observer: LiveEntities? = null
    private var database: Database? = null
    private val leased = HashSet<String>()
    private val decoded = AtomicInteger()

    val decodedRows: Int get() = decoded.get()

    @Synchronized
    override fun retain(ids: List<String>) {
        leased.addAll(ids)
        runCatching { open().retain(ids) }
    }

    @Synchronized
    override fun release(ids: List<String>) {
        leased.removeAll(ids.toSet())
        runCatching { open().release(ids) }
    }

    /** The observer belongs to the database that was current when it opened. A newly opened database gets a new observer with the ids still leased. */
    private fun open(): LiveEntities {
        val current = DBlayer.database
        val existing = observer
        if (existing != null && database === current) return existing
        database = current
        return current.liveEntities(modelName, this).also {
            observer = it
            if (leased.isNotEmpty()) it.retain(leased.toList())
        }
    }

    override fun changed(delivery: EntityDelivery) {
        val rows = delivery.rows.mapNotNull { row -> runCatching { dbJson.decodeFromString(serializer, row) }.getOrNull() }
        decoded.addAndGet(rows.size)
        deliver(EntityDeliveryRows(delivery.revision, rows, delivery.removed))
    }
}

internal object EntityRegistries {
    private val registries = ConcurrentHashMap<String, Entities<*>>()

    @Suppress("UNCHECKED_CAST")
    fun <Model : DBModel> of(modelName: String, serializer: KSerializer<Model>): Entities<Model> =
        registries.getOrPut(modelName) { Entities(modelName, serializer) } as Entities<Model>

    fun resetAll() = registries.values.forEach { it.reset() }
}

/** The live entity of a record by id. */
fun <Model : DBModel> entityOf(modelName: String, serializer: KSerializer<Model>, id: String): Entity<Model> =
    EntityRegistries.of(modelName, serializer).entity(id)

package com.noma4i.dblayer

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger

private val entityLog = Logger.getLogger("DBlayer")

internal class EntityCell<Model : DBModel> {
    val value = MutableStateFlow<Model?>(null)
    @Volatile var stale = false
    @Volatile var revision = Long.MIN_VALUE
    @Volatile var bytes = 0uL
    @Volatile var closed = false

    @Synchronized
    fun accept(next: Model?, incoming: Long, bytes: ULong = 0uL) {
        if (incoming < revision) return
        revision = incoming
        this.bytes = bytes
        stale = false
        value.value = next
    }

    @Synchronized
    fun clear() {
        value.value = null
        revision = Long.MIN_VALUE
        stale = false
    }
}

/** The live value of one record. The library keeps it current while the object is reachable and lets go of the record when it is not. */
class Entity<Model : DBModel> internal constructor(private val fixedId: String?, internal val cell: EntityCell<Model>) {
    /** The id of the record. A singleton has no id until its row has loaded. */
    val id: String get() = fixedId ?: cell.value.value?.id.orEmpty()
    /** The newest value, null while none has arrived or when the record does not exist. */
    val value: StateFlow<Model?> get() = cell.value
    val isStale: Boolean get() = cell.stale
    /** True after the database closed. A closed entity never receives a value again. */
    val isClosed: Boolean get() = cell.closed
}

internal interface EntitySource {
    fun cachedRow(id: String): WireRow?
    fun retain(ids: List<String>)
    fun release(ids: List<String>)
}

internal class EntityDeliveryRows<Model>(val revision: Long, val rows: List<Pair<Model, ULong>>, val removed: List<String>)

private class LiveRef<Model : DBModel>(entity: Entity<Model>, val id: String, val cell: EntityCell<Model>, private val collect: (LiveRef<Model>) -> Unit) :
    WeakReference<Entity<Model>>(entity, EntityReferences.queue) {
    fun collected() = collect(this)
}

private object EntityReferences {
    val queue = ReferenceQueue<Any>()

    init {
        Thread({ while (true) (queue.remove() as LiveRef<*>).collected() }, "dblayer-entity-releases").apply { isDaemon = true }.start()
    }
}

private enum class Step { Retain, Release, Refresh }

/** The registry of one model: one [Entity] per id, one core observer for the whole model. */
class Entities<Model : DBModel> internal constructor(
    private val modelName: String,
    private val decoder: ModelDecoder<Model>,
    private val now: () -> Long = { System.nanoTime() / 1_000_000 },
    source: EntitySource? = null,
) {
    private val lock = Any()
    private val live = HashMap<String, LiveRef<Model>>()
    private val kept = if (source == null) EntityRegistries.retained else RetainedValues()
    private val steps = HashMap<String, Step>()
    private var flushScheduled = false
    private var single: Entity<Model>? = null
    private var singleQuery: Query<Model>? = null
    private var singleFeed: SingletonFeed<Model>? = null
    private val core = CoreEntitySource(modelName, decoder) { delivery -> apply(delivery) }
    private val source: EntitySource = source ?: core

    internal val decodedRows: Int get() = core.decodedRows
    internal val leaseFailures: Int get() = core.leaseFailures

    /** The entity of an id. Hold it for as long as the value is shown. The value is there at once when the id is live or retained. */
    fun entity(id: String): Entity<Model> = synchronized(lock) {
        val previous = live[id]
        previous?.get()?.let { return it }
        if (previous != null) collected(previous)
        var seed = kept.take<Model>(modelName, id, now())
        val retained = seed != null
        val cached = source.cachedRow(id)
        if (cached != null && cached.revision > (seed?.revision ?: Long.MIN_VALUE)) {
            runCatching { decoder.read(cached) }.getOrNull()?.let { value ->
                seed = EntityCell<Model>().also { it.accept(value, cached.revision, cached.estimatedBytes) }
            }
        }
        val cell = seed ?: EntityCell()
        cell.stale = seed != null
        val made = Entity(id, cell)
        live[id] = LiveRef(made, id, cell, ::collected)
        steps[id] = if (retained || steps[id] == Step.Release) Step.Refresh else Step.Retain
        scheduleFlush()
        made
    }

    fun entities(ids: List<String>): List<Entity<Model>> = ids.map(::entity)

    /** The entity of the one row of a model that has one. It lives as long as the database handle, holds no lease and has no id until the row has loaded. */
    fun singleton(query: Query<Model>): Entity<Model> = synchronized(lock) {
        single?.let { return it }
        val made = Entity(null, EntityCell<Model>())
        single = made
        singleQuery = query
        startSingleton()
        made
    }

    private fun startSingleton() {
        val query = singleQuery ?: return
        singleFeed?.cancel()
        val feed = SingletonFeed(query, decoder) { from, revision, row -> receive(from, revision, row) }
        singleFeed = feed
        flusher.execute { feed.attach() }
    }

    /** A delivery of a feed that was replaced by a reset writes nothing. */
    internal fun receive(feed: SingletonFeed<Model>, revision: Long, row: Model?) = synchronized(lock) {
        if (singleFeed === feed) single?.cell?.accept(row, revision)
    }

    internal val currentSingleFeed: SingletonFeed<Model>? get() = synchronized(lock) { singleFeed }

    /** The account ended. The core tells every leased entity that its row is gone and later delivers the rows of the next account. */
    fun reset() = synchronized(lock) {
        kept.clear(modelName)
        singleFeed?.cancel()
        singleFeed = null
        single?.cell?.clear()
    }

    /** The database closed. Every entity that is still held becomes closed and the registry starts empty. */
    internal fun close() = synchronized(lock) {
        live.values.forEach { ref -> ref.get()?.cell?.let { it.closed = true; it.clear() } }
        live.clear()
        steps.clear()
        kept.clear(modelName)
        singleFeed?.cancel()
        singleFeed = null
        single?.cell?.let { it.closed = true; it.clear() }
        single = null
        singleQuery = null
        core.close()
    }

    internal fun resume() = synchronized(lock) {
        if (singleFeed == null) startSingleton()
    }

    internal val retainedCount: Int get() = synchronized(lock) { kept.count(modelName, now()) }
    internal val liveCount: Int get() = synchronized(lock) { live.size }

    internal fun apply(delivery: EntityDeliveryRows<Model>) = synchronized(lock) {
        delivery.rows.forEach { (model, bytes) -> live[model.id]?.takeIf { it.get() != null }?.cell?.accept(model, delivery.revision, bytes) }
        delivery.removed.forEach { id -> live[id]?.takeIf { it.get() != null }?.cell?.accept(null, delivery.revision) }
    }

    private fun collected(ref: LiveRef<Model>) = synchronized(lock) {
        if (live[ref.id] !== ref) return@synchronized
        live.remove(ref.id)
        steps[ref.id] = Step.Release
        if (ref.cell.value.value != null) kept.keep(modelName, ref.id, ref.cell, now())
        scheduleFlush()
    }

    private fun scheduleFlush() {
        if (flushScheduled) return
        flushScheduled = true
        flusher.execute { flush() }
    }

    private fun flush() {
        val pending = synchronized(lock) {
            flushScheduled = false
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
    private val decoder: ModelDecoder<Model>,
    private val deliver: (EntityDeliveryRows<Model>) -> Unit,
) : EntitySource {
    private class Changes<Model : DBModel>(source: CoreEntitySource<Model>) : EntityListener {
        private val source = WeakReference(source)
        override fun changed(delivery: EntityDelivery) {
            source.get()?.changed(delivery)
        }
    }

    private var observer: LiveEntities? = null
    private val decoded = AtomicInteger()
    private val failures = AtomicInteger()

    val decodedRows: Int get() = decoded.get()
    val leaseFailures: Int get() = failures.get()

    override fun cachedRow(id: String): WireRow? = DBlayer.database.peekRow(modelName, id)

    @Synchronized
    override fun retain(ids: List<String>) {
        try { open().retain(ids) } catch (error: Exception) { failed("retain", ids, error) }
    }

    @Synchronized
    override fun release(ids: List<String>) {
        try { open().release(ids) } catch (error: Exception) { failed("release", ids, error) }
    }

    @Synchronized
    fun close() {
        observer = null
    }

    private fun failed(call: String, ids: List<String>, error: Exception) {
        failures.incrementAndGet()
        entityLog.severe("Entity $call of ${ids.size} $modelName ids failed: $error")
    }

    private fun open(): LiveEntities = observer ?: DBlayer.database.liveEntities(modelName, Changes(this)).also { observer = it }

    private fun changed(delivery: EntityDelivery) {
        val rows = delivery.rows.mapNotNull { row ->
            runCatching { decoder.read(row) to row.estimatedBytes }
                .onFailure { failures.incrementAndGet(); entityLog.severe("Row of $modelName failed to decode for its entity: $it") }
                .getOrNull()
        }
        decoded.addAndGet(rows.size)
        deliver(EntityDeliveryRows(delivery.revision, rows, delivery.removed))
    }
}

/** The one relation subscription of a singleton. It belongs to one database and one reset: a replaced feed writes nothing. */
internal class SingletonFeed<Model : DBModel>(
    private val query: Query<Model>,
    private val decoder: ModelDecoder<Model>,
    private val deliver: (SingletonFeed<Model>, Long, Model?) -> Unit,
) {
    private var session: ListSession<Model, Pair<Long, Model?>>? = null
    private var cancelled = false
    private val count = AtomicInteger()

    /** Deliveries made so far. */
    val delivered: Int get() = count.get()

    fun attach() {
        val database = DBlayer.database
        val made = ListSession<Model, Pair<Long, Model?>>(
            decoder,
            { listener -> val subscription = database.liveQuery(query.json, listener); val cancel: () -> Unit = { subscription.cancel() }; cancel },
            { value -> count.incrementAndGet(); deliver(this, value.first, value.second) },
            { applied, change -> change.revision to applied.rows.firstOrNull() },
        )
        val keep = synchronized(this) { if (cancelled) false else { session = made; true } }
        if (keep) runCatching { made.start() }
    }

    fun cancel() {
        val current = synchronized(this) { cancelled = true; session.also { session = null } }
        current?.cancel()
    }
}

internal object EntityRegistries {
    val retained = RetainedValues()
    private val registries = ConcurrentHashMap<String, Entities<*>>()

    @Suppress("UNCHECKED_CAST")
    fun <Model : DBModel> of(modelName: String, decoder: ModelDecoder<Model>): Entities<Model> =
        registries.getOrPut(modelName) { Entities(modelName, decoder) } as Entities<Model>

    fun leaseFailures(): Int = registries.values.sumOf { it.leaseFailures }
    fun resetAll() = registries.values.forEach { it.reset() }
    fun closeAll() = registries.values.forEach { it.close() }
    fun resumeAll() = registries.values.forEach { it.resume() }
}

/** The live entity of a record by id. */
fun <Model : DBModel> entityOf(modelName: String, decoder: ModelDecoder<Model>, id: String): Entity<Model> =
    EntityRegistries.of(modelName, decoder).entity(id)

/** The live entity of the one row of a model that has one. */
fun <Model : DBModel> singletonEntityOf(modelName: String, decoder: ModelDecoder<Model>, query: Query<Model>): Entity<Model> =
    EntityRegistries.of(modelName, decoder).singleton(query)

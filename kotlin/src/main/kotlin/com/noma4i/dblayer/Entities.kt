package com.noma4i.dblayer

import kotlinx.coroutines.ExperimentalForInheritanceCoroutinesApi
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import java.lang.ref.Reference
import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger

private val entityLog = Logger.getLogger("DBlayer")

/** `Opening` until the core answers, then `Present` or `Absent`. `Failed` keeps the last value and says why the core could not read or decode the record. `Closed` after the database closed. */
sealed class EntityPhase {
    data object Opening : EntityPhase()
    data object Present : EntityPhase()
    data object Absent : EntityPhase()
    data class Failed(val error: String) : EntityPhase()
    data object Closed : EntityPhase()
}

internal class EntityCell<Model : DBModel>(singleton: Boolean = false) {
    private val machine = EntityMachine(singleton)
    val value = MutableStateFlow<Model?>(null)
    val phase = MutableStateFlow<EntityPhase>(EntityPhase.Opening)
    @Volatile var stale = false
    @Volatile var revision = Long.MIN_VALUE
    @Volatile var bytes = 0uL

    private fun show(verdict: EntityVerdict) {
        revision = verdict.revision
        stale = verdict.stale
        phase.value = when (val stage = verdict.stage) {
            is EntityStage.Opening -> EntityPhase.Opening
            is EntityStage.Present -> EntityPhase.Present
            is EntityStage.Absent -> EntityPhase.Absent
            is EntityStage.Failed -> EntityPhase.Failed(stage.message)
            is EntityStage.Closed -> EntityPhase.Closed
        }
    }

    private fun emptied(verdict: EntityVerdict) {
        value.value = null
        bytes = 0uL
        show(verdict)
    }

    @Synchronized
    fun seed(incoming: Long, bytes: ULong, make: () -> Model?) {
        val verdict = machine.seed(incoming)
        if (!verdict.applied) return
        try { value.value = make() } catch (error: Exception) { fail(error.toString()); return }
        this.bytes = bytes
        show(verdict)
    }

    @Synchronized
    fun accept(next: Model?, incoming: Long, bytes: ULong = 0uL, feed: ULong? = null) {
        val verdict = machine.accept(incoming, next != null, feed)
        if (!verdict.applied) return
        this.bytes = bytes
        show(verdict)
        value.value = next
    }

    @Synchronized
    fun fail(message: String, feed: ULong? = null) = show(machine.fail(message, feed))

    @Synchronized
    fun reset() {
        val verdict = machine.reset()
        if (verdict.cleared) emptied(verdict)
    }

    @Synchronized
    fun close() = emptied(machine.close())

    @Synchronized
    fun unopened(feed: ULong): Boolean {
        val verdict = machine.unopened(feed)
        if (verdict.cleared) emptied(verdict)
        return verdict.cleared
    }

    @Synchronized
    fun beginFeed(): ULong = machine.beginFeed().feed
}

/** Collecting keeps the entity reachable until the collection ends. */
@OptIn(ExperimentalForInheritanceCoroutinesApi::class)
private class HeldState<T>(private val owner: Any, private val inner: StateFlow<T>) : StateFlow<T> {
    override val value: T get() = inner.value
    override val replayCache: List<T> get() = inner.replayCache
    override suspend fun collect(collector: FlowCollector<T>): Nothing {
        try { inner.collect(collector) } finally { Reference.reachabilityFence(owner) }
    }
}

/** The live value of one record. The library keeps it current while the object is reachable and lets go of the record when it is not. */
class Entity<Model : DBModel> internal constructor(private val fixedId: String?, internal val cell: EntityCell<Model>) {
    /** The id of the record. A singleton has no id until its row has loaded. */
    val id: String get() = fixedId ?: cell.value.value?.id.orEmpty()
    /** The newest value, null while none has arrived or when the record does not exist. */
    val value: StateFlow<Model?> = HeldState(this, cell.value)
    val isStale: Boolean get() = cell.stale
    val phase: StateFlow<EntityPhase> = HeldState(this, cell.phase)
    /** True after the database closed. A closed entity never receives a value again. */
    val isClosed: Boolean get() = cell.phase.value == EntityPhase.Closed
}

internal interface EntitySource {
    fun cachedRow(id: String): WireRow?
    fun retain(ids: List<String>)
    fun release(ids: List<String>)
}

internal class EntityDeliveryRows<Model>(val revision: Long, val rows: List<Pair<Model, ULong>>, val removed: List<String>, val failure: String? = null, val failedIds: Map<String, String> = emptyMap())

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
        val retained = kept.take<Model>(modelName, id, now())
        val cell = EntityCell<Model>()
        retained?.let { cell.seed(it.revision, it.bytes) { it.value } }
        try {
            source.cachedRow(id)?.let { cached -> cell.seed(cached.revision, cached.estimatedBytes) { decoder.read(cached) } }
        } catch (error: Exception) {
            cell.fail(error.toString())
        }
        val made = Entity(id, cell)
        live[id] = LiveRef(made, id, cell, ::collected)
        steps[id] = if (retained != null || steps[id] == Step.Release) Step.Refresh else Step.Retain
        scheduleFlush()
        made
    }

    fun entities(ids: List<String>): List<Entity<Model>> = ids.map(::entity)

    /** The entity of the one row of a model that has one. It lives as long as the database handle, holds no lease and has no id until the row has loaded. */
    fun singleton(query: Query<Model>): Entity<Model> = synchronized(lock) {
        single?.let { return it }
        val made = Entity(null, EntityCell<Model>(singleton = true))
        single = made
        singleQuery = query
        startSingleton()
        made
    }

    private fun startSingleton() {
        val query = singleQuery ?: return
        val cell = single?.cell ?: return
        singleFeed?.cancel()
        val feed = SingletonFeed(query, decoder, cell.beginFeed(), { from, revision, row -> receive(from, revision, row) }, ::unopened, ::singletonFailed)
        singleFeed = feed
        flusher.execute { feed.attach() }
    }

    internal fun receive(feed: SingletonFeed<Model>, revision: Long, row: Model?) = synchronized(lock) {
        single?.cell?.accept(row, revision, feed = feed.generation)
    }

    private fun singletonFailed(feed: SingletonFeed<Model>, message: String) = synchronized(lock) {
        single?.cell?.fail(message, feed.generation)
    }

    private fun unopened(feed: SingletonFeed<Model>) = synchronized(lock) {
        if (single?.cell?.unopened(feed.generation) != true) return@synchronized
        singleFeed = null
        single = null
        singleQuery = null
    }

    internal val currentSingleFeed: SingletonFeed<Model>? get() = synchronized(lock) { singleFeed }

    /** The account ended. The core tells every leased entity that its row is gone and later delivers the rows of the next account. */
    fun reset() = synchronized(lock) {
        kept.clear(modelName)
        singleFeed?.cancel()
        singleFeed = null
        single?.cell?.reset()
    }

    /** The database closed. Every entity that is still held becomes closed and the registry starts empty. */
    internal fun close() = synchronized(lock) {
        live.values.forEach { ref -> ref.get()?.cell?.let { it.close() } }
        live.clear()
        steps.clear()
        kept.clear(modelName)
        singleFeed?.cancel()
        singleFeed = null
        single?.cell?.let { it.close() }
        single = null
        singleQuery = null
        core.close()
    }

    internal fun resume() = synchronized(lock) {
        if (singleFeed == null) startSingleton()
    }

    internal val retainedCount: Int get() = synchronized(lock) { kept.count(modelName, now()) }
    internal val liveCount: Int get() = synchronized(lock) { live.size }

    internal fun apply(delivery: EntityDeliveryRows<Model>) {
        val writes = ArrayList<() -> Unit>()
        synchronized(lock) {
            fun liveCell(id: String) = live[id]?.takeIf { it.get() != null }?.cell
            delivery.rows.forEach { (model, bytes) -> liveCell(model.id)?.let { writes += { it.accept(model, delivery.revision, bytes) } } }
            delivery.removed.forEach { id -> liveCell(id)?.let { writes += { it.accept(null, delivery.revision) } } }
            delivery.failure?.let { message -> live.values.forEach { ref -> if (ref.get() != null) writes += { ref.cell.fail(message) } } }
            delivery.failedIds.forEach { (id, message) -> liveCell(id)?.let { writes += { it.fail(message) } } }
        }
        writes.forEach { it() }
    }

    private fun collected(ref: LiveRef<Model>) = synchronized(lock) {
        if (live[ref.id] !== ref) return@synchronized
        live.remove(ref.id)
        steps[ref.id] = Step.Release
        ref.cell.value.value?.let { kept.keep(modelName, ref.id, Retained(it, ref.cell.revision, ref.cell.bytes), now()) }
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
        deliver(EntityDeliveryRows(Long.MIN_VALUE, emptyList(), emptyList(), failedIds = ids.associateWith { error.toString() }))
    }

    private fun open(): LiveEntities = observer ?: DBlayer.database.liveEntities(modelName, Changes(this)).also { observer = it }

    private fun changed(delivery: EntityDelivery) {
        delivery.failure?.let { failures.incrementAndGet(); entityLog.severe("Entity read of $modelName failed: $it") }
        val failedIds = HashMap<String, String>()
        val rows = delivery.rows.mapNotNull { row ->
            runCatching { decoder.read(row) to row.estimatedBytes }
                .onFailure { failures.incrementAndGet(); failedIds[row.id] = it.toString(); entityLog.severe("Row of $modelName failed to decode for its entity: $it") }
                .getOrNull()
        }
        decoded.addAndGet(rows.size)
        deliver(EntityDeliveryRows(delivery.revision, rows, delivery.removed, delivery.failure, failedIds))
    }
}

/** The one relation subscription of a singleton. It belongs to one database and one reset: a replaced feed writes nothing. */
internal class SingletonFeed<Model : DBModel>(
    private val query: Query<Model>,
    private val decoder: ModelDecoder<Model>,
    val generation: ULong,
    private val deliver: (SingletonFeed<Model>, Long, Model?) -> Unit,
    private val unopened: (SingletonFeed<Model>) -> Unit,
    private val failed: (SingletonFeed<Model>, String) -> Unit,
) {
    private var session: ListSession<Model, Pair<Long, Model?>>? = null
    private var cancelled = false
    private val count = AtomicInteger()

    /** Deliveries made so far. */
    val delivered: Int get() = count.get()

    fun attach() {
        val database = try { DBlayer.database } catch (_: IllegalStateException) { unopened(this); return }
        val made = ListSession<Model, Pair<Long, Model?>>(
            decoder,
            { listener -> val subscription = database.liveQuery(query.json, listener); val cancel: () -> Unit = { subscription.cancel() }; cancel },
            { value -> count.incrementAndGet(); deliver(this, value.first, value.second) },
            { applied, change -> change.revision to applied.rows.firstOrNull() },
        )
        val keep = synchronized(this) { if (cancelled) false else { session = made; true } }
        if (keep) try { made.start() } catch (error: Exception) { failed(this, error.toString()) }
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

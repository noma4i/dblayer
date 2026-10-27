package com.noma4i.dblayer

import java.lang.ref.Reference
import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.ExperimentalForInheritanceCoroutinesApi
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.coroutines.resume
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map

sealed class EntityPhase {
    data object Opening : EntityPhase()
    data object Present : EntityPhase()
    data object Absent : EntityPhase()
    data class Failed(val error: String) : EntityPhase()
    data object Closed : EntityPhase()
}

data class EntitySnapshot<Model : DBModel>(
    val value: Model?,
    val phase: EntityPhase,
    val read: EntityRead,
    val revision: Long,
    val isStale: Boolean,
    val settled: Boolean,
)

@OptIn(ExperimentalForInheritanceCoroutinesApi::class)
private class EntityField<Model : DBModel, T>(
    private val owner: Any,
    private val state: StateFlow<EntitySnapshot<Model>>,
    private val select: (EntitySnapshot<Model>) -> T,
) : StateFlow<T> {
    override val value get() = select(state.value)
    override val replayCache get() = listOf(value)
    override suspend fun collect(collector: FlowCollector<T>): Nothing {
        try { state.map(select).distinctUntilChanged().collect(collector) }
        finally { Reference.reachabilityFence(owner) }
        error("entity snapshot collection ended")
    }
}

class Entity<Model : DBModel> internal constructor(
    private val fixedId: String?,
    internal val lease: ULong,
    private val owner: Entities<Model>,
) {
    private val shown = MutableStateFlow(EntitySnapshot<Model>(null, EntityPhase.Opening, EntityRead.NotLoaded, Long.MIN_VALUE, false, false))
    val snapshot: StateFlow<EntitySnapshot<Model>> = EntityField(this, shown) { it }
    val id: String get() = fixedId ?: shown.value.value?.id.orEmpty()
    val value: StateFlow<Model?> = EntityField(this, shown) { it.value }
    val phase: StateFlow<EntityPhase> = EntityField(this, shown) { it.phase }
    val read: StateFlow<EntityRead> = EntityField(this, shown) { it.read }
    val isStale: Boolean get() = shown.value.isStale
    val isClosed: Boolean get() = shown.value.phase == EntityPhase.Closed
    internal fun publish(snapshot: EntitySnapshot<Model>) { shown.value = snapshot }
    suspend fun waitForInitialLoad() {
        val ready = shown.first { it.settled || it.phase == EntityPhase.Closed }
        if (ready.phase == EntityPhase.Closed) throw DbException.Interrupted("entity closed")
        (ready.read as? EntityRead.Failed)?.let { throw EntityReadException(it.error) }
    }
}

class EntityReadException(val error: EntityRefreshError) : Exception(error.toString())

private class LiveRef<Model : DBModel>(entity: Entity<Model>, val id: String, val lease: ULong, private val collect: (LiveRef<Model>) -> Unit) :
    WeakReference<Entity<Model>>(entity, EntityReferences.queue) {
    fun collected() = collect(this)
}

private object EntityReferences {
    val queue = ReferenceQueue<Any>()
    init { Thread({ while (true) (queue.remove() as LiveRef<*>).collected() }, "dblayer-entity-releases").apply { isDaemon = true }.start() }
}

class Entities<Model : DBModel> internal constructor(private val modelName: String, private val decoder: ModelDecoder<Model>) {
    private val lock = Any()
    private val live = HashMap<String, LiveRef<Model>>()
    private val next = AtomicLong()
    private val pending = HashMap<String, EntityRequest>()
    private var scheduled = false
    private var single: Entity<Model>? = null
    private val core = CoreEntitySource(modelName, decoder, ::publish)
    internal val decodedRows get() = core.decodedRows
    internal val leaseFailures get() = core.leaseFailures
    internal val liveCount get() = synchronized(lock) { live.values.count { it.get() != null } }

    fun entity(id: String, fetch: EntityFetch = EntityFetch.NONE): Entity<Model> = synchronized(lock) {
        live[id]?.get()?.let { return it }
        val lease = next.incrementAndGet().toULong()
        val made = Entity(id, lease, this)
        live[id] = LiveRef(made, id, lease, ::collected)
        pending[id] = EntityRequest(id, lease, fetch)
        if (!scheduled) { scheduled = true; flusher.execute { flush() } }
        made
    }

    fun entities(ids: List<String>): List<Entity<Model>> = ids.map { entity(it) }

    fun singleton(query: Query<Model>): Entity<Model> = synchronized(lock) {
        single?.let { return it }
        val lease = next.incrementAndGet().toULong()
        val made = Entity<Model>(null, lease, this)
        single = made
        core.singleton(query.json, lease)
        made
    }

    private fun flush() {
        val requests = synchronized(lock) { scheduled = false; pending.values.toList().also { pending.clear() } }
        if (requests.isNotEmpty()) core.retain(requests)
    }

    private fun collected(ref: LiveRef<Model>) {
        synchronized(lock) {
            if (live[ref.id] === ref) live.remove(ref.id)
            if (pending[ref.id]?.lease == ref.lease) pending.remove(ref.id)
        }
        core.release(listOf(EntityLease(ref.id, ref.lease)))
    }

    private fun publish(lease: ULong, id: String, snapshot: EntitySnapshot<Model>) {
        val target = synchronized(lock) { if (id.isEmpty()) single else live[id]?.get() } ?: return
        if (target.lease != lease) return
        target.publish(snapshot)
        if (snapshot.phase == EntityPhase.Closed) synchronized(lock) {
            if (id.isEmpty()) single = null else if (live[id]?.lease == lease) live.remove(id)
        }
    }

    fun reset() = core.reset()
    internal suspend fun delivered() = core.delivered()
    internal fun close() { synchronized(lock) { pending.clear() }; core.close() }
    internal fun resume() = core.resume()

    companion object {
        internal val flusher = Executors.newSingleThreadExecutor { task -> Thread(task, "dblayer-entities").apply { isDaemon = true } }
    }
}

internal class CoreEntitySource<Model : DBModel>(
    private val modelName: String,
    private val decoder: ModelDecoder<Model>,
    private val deliver: (ULong, String, EntitySnapshot<Model>) -> Unit,
) {
    private class Changes<Model : DBModel>(source: CoreEntitySource<Model>) : EntityListener {
        private val source = WeakReference(source)
        override fun changed(delivery: EntityDelivery) { source.get()?.changed(delivery) }
    }
    private var observer: LiveEntities? = null
    private val values = HashMap<ULong, Pair<ULong, Model?>>()
    private val decoded = AtomicInteger()
    private val failures = AtomicInteger()
    val decodedRows get() = decoded.get()
    val leaseFailures get() = failures.get()
    private fun open(): LiveEntities = observer ?: DBlayer.database.liveEntities(modelName, Changes(this)).also { observer = it }
    private fun submit(work: () -> Unit) = Entities.flusher.execute(work)

    fun retain(requests: List<EntityRequest>) = submit {
        perform(requests.map { EntityLease(it.id, it.lease) }) { open().retain(requests) }
    }
    fun release(leases: List<EntityLease>) = submit {
        leases.forEach { values.remove(it.lease) }
        observer?.let { perform(leases) { it.release(leases) } }
    }
    fun singleton(query: String, lease: ULong) = submit {
        perform(listOf(EntityLease("", lease))) { open().singleton(query, lease) }
    }
    fun reset() = submit { observer?.reset() }
    fun resume() = submit { observer?.resume() }
    fun close() = submit { observer?.shutdown(); observer = null; values.clear() }
    suspend fun delivered(): Unit = suspendCancellableCoroutine { continuation -> submit { continuation.resume(Unit) } }

    private fun perform(leases: List<EntityLease>, call: () -> Unit) {
        try { call() } catch (error: Exception) {
            failures.incrementAndGet()
            leases.forEach { decode(entityOpenFailed(it.id, it.lease, error.toString(), error is IllegalStateException)) }
        }
    }

    private fun changed(delivery: EntityDelivery) = submit { delivery.snapshots.forEach(::decode) }

    private fun decode(wire: WireEntitySnapshot) {
        val value = try {
            val held = values[wire.lease]
            if (held != null && held.first == wire.valueToken) held.second
            else wire.value?.let { decoder.read(it).also { decoded.incrementAndGet() } }.also { values[wire.lease] = wire.valueToken to it }
        } catch (error: Exception) {
            failures.incrementAndGet()
            observer?.decodeFailed(EntityLease(wire.id, wire.lease), wire.revision, error.toString())
            return
        }
        val phase = when (val state = wire.phase) {
            is EntityStage.Opening -> EntityPhase.Opening
            is EntityStage.Present -> EntityPhase.Present
            is EntityStage.Absent -> EntityPhase.Absent
            is EntityStage.Failed -> EntityPhase.Failed(state.message)
            is EntityStage.Closed -> EntityPhase.Closed
        }
        deliver(wire.lease, wire.id, EntitySnapshot(value, phase, wire.read, wire.revision, wire.isStale, wire.settled))
    }
}

internal object EntityRegistries {
    private val registries = ConcurrentHashMap<String, Entities<*>>()
    @Suppress("UNCHECKED_CAST")
    fun <Model : DBModel> of(modelName: String, decoder: ModelDecoder<Model>): Entities<Model> =
        registries.getOrPut(modelName) { Entities(modelName, decoder) } as Entities<Model>
    fun leaseFailures(): Int = registries.values.sumOf { it.leaseFailures }
    suspend fun settleAll() { registries.values.forEach { it.delivered() } }
    fun closeAll() { registries.values.forEach { it.close() }; registries.clear() }
    fun resumeAll() = registries.values.forEach { it.resume() }
}

fun <Model : DBModel> entityOf(modelName: String, decoder: ModelDecoder<Model>, id: String, fetch: EntityFetch = EntityFetch.NONE): Entity<Model> =
    EntityRegistries.of(modelName, decoder).entity(id, fetch)
fun <Model : DBModel> singletonEntityOf(modelName: String, decoder: ModelDecoder<Model>, query: Query<Model>): Entity<Model> =
    EntityRegistries.of(modelName, decoder).singleton(query)

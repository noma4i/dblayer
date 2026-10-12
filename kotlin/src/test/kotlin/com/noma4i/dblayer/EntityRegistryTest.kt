package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

private class CountingSource : EntitySource {
    val retained = java.util.concurrent.CopyOnWriteArrayList<String>()
    val released = java.util.concurrent.CopyOnWriteArrayList<String>()
    override fun retain(ids: List<String>) { retained.addAll(ids) }
    override fun release(ids: List<String>) { released.addAll(ids) }
}

class EntityRegistryTest {
    private fun moment(id: String, text: String) = Moment(id = id, uuid = id, text = text, likedByMe = false, media = JsonObject(emptyMap()), createdAt = "2026-10-08T00:00:00.000Z")

    private fun delivery(revision: Long, rows: List<Moment> = emptyList(), removed: List<String> = emptyList()) = EntityDeliveryRows(revision, rows, removed)

    private fun registry(source: EntitySource = CountingSource(), maxAge: Long = 120_000, maxCount: Int = 200, now: () -> Long = { 0 }) =
        Entities("Moment", Moment.serializer(), maxAge, maxCount, now, source)

    private fun collectGarbage(until: () -> Boolean) = eventuallyBlocking("the garbage collector frees the readers") { System.gc(); until() }

    /** REL-01 */
    @Test
    fun oneValuePerIdForEveryReader() {
        val registry = registry()
        val readers = List(3) { registry.entity("1") }
        registry.apply(delivery(1, listOf(moment("1", "A"))))
        registry.apply(delivery(2, listOf(moment("1", "B"))))
        assertTrue(readers.all { it.value.value?.text == "B" })
        assertEquals(1, readers.map { System.identityHashCode(it) }.toSet().size)
        assertEquals(1, registry.liveCount)
    }

    /** REL-04 */
    @Test
    fun lowerRevisionNeverReplacesHigher() {
        val registry = registry()
        val entity = registry.entity("1")
        registry.apply(delivery(7, listOf(moment("1", "seven"))))
        registry.apply(delivery(5, listOf(moment("1", "five"))))
        assertEquals("seven", entity.value.value?.text)
        registry.apply(delivery(9, listOf(moment("1", "nine"))))
        registry.apply(delivery(8, removed = listOf("1")))
        assertEquals("nine", entity.value.value?.text)
    }

    /** REL-05 */
    @Test
    fun releasedLeaseNeverWritesAValue() {
        val registry = registry()
        var entity: Entity<Moment>? = registry.entity("1")
        registry.apply(delivery(1, listOf(moment("1", "old"))))
        entity = null
        collectGarbage { registry.liveCount == 0 }
        registry.apply(delivery(2, listOf(moment("1", "new"))))
        val again = registry.entity("1")
        assertEquals("old", again.value.value?.text)
        assertTrue(again.isStale)
        registry.apply(delivery(0, listOf(moment("1", "late"))))
        assertEquals("old", again.value.value?.text)
        registry.apply(delivery(3, listOf(moment("1", "fresh"))))
        assertEquals("fresh", again.value.value?.text)
        assertTrue(!again.isStale)
    }

    /** REL-09 */
    @Test
    fun oneChangedIdInvalidatesOnlyItsReaders() = bounded {
        val registry = registry()
        val entities = List(10) { registry.entity("$it") }
        registry.apply(delivery(1, List(10) { moment("$it", "first") }))
        val emissions = AtomicInteger()
        val jobs = List(50) { index -> launch(Dispatchers.Unconfined) { entities[index % 10].value.collect { emissions.incrementAndGet() } } }
        val before = emissions.get()
        registry.apply(delivery(2, listOf(moment("3", "second"))))
        assertEquals(5, emissions.get() - before)
        jobs.forEach { it.cancel() }
    }

    /** REL-10 */
    @Test
    fun secondReaderReadsWarmValueSynchronously() {
        val source = CountingSource()
        val registry = registry(source)
        val first = registry.entity("1")
        registry.apply(delivery(1, listOf(moment("1", "warm"))))
        Thread.sleep(100)
        val retainedBefore = source.retained.toList()
        val second = registry.entity("1")
        Thread.sleep(100)
        assertEquals("warm", second.value.value?.text)
        assertTrue(second === first)
        assertEquals(retainedBefore, source.retained.toList())
        assertEquals(listOf("1"), retainedBefore)
    }

    /** REL-11 */
    @Test
    fun retainedValueIsDroppedAfterTheBound() {
        var clock = 0L
        val registry = registry(maxAge = 120_000, maxCount = 200, now = { clock })
        repeat(201) { index ->
            var entity: Entity<Moment>? = registry.entity("$index")
            registry.apply(delivery(1, listOf(moment("$index", "v"))))
            entity = null
            collectGarbage { registry.liveCount == 0 }
        }
        assertEquals(200, registry.retainedCount)
        val shown = registry.entity("150")
        assertTrue(shown.isStale && shown.value.value?.text == "v")
        clock = 120_001
        assertEquals(0, registry.retainedCount)
    }

    /** REL-24 */
    @Test
    fun resetDropsEveryEntityLeaseAndRetainedValue() {
        val source = CountingSource()
        val registry = registry(source)
        val held = List(5) { registry.entity("$it") }
        repeat(5) { registry.entity("${it + 5}") }
        registry.apply(delivery(1, List(10) { moment("$it", "v") }))
        collectGarbage { registry.retainedCount == 5 }
        registry.reset()
        Thread.sleep(150)
        assertEquals(0, registry.retainedCount)
        assertEquals(0, registry.liveCount)
        assertTrue(held.all { it.value.value == null })
        assertTrue(source.released.containsAll(List(10) { "$it" }))
    }

    /** Addendum 3a: a cancelled attach leaves no subscription, at each of the 3 timings. */
    @Test
    fun attachCancelledAtAnyMomentLeavesNoSubscription() = bounded {
        val active = AtomicInteger()
        val gate = CountDownLatch(1)
        val blocking = LiveSource(0) { emit: (Int) -> Unit ->
            active.incrementAndGet()
            gate.await()
            emit(1)
            val cancel: () -> Unit = { active.decrementAndGet() }
            cancel
        }
        val before = launch(start = CoroutineStart.LAZY) { blocking.flow.collect { } }
        before.cancel()
        before.join()
        assertEquals(0, active.get())

        val during = launch { blocking.flow.collect { } }
        eventually("not (active.get() == 0)", 5000) { !(active.get() == 0) }
        during.cancel()
        gate.countDown()
        during.join()
        assertEquals(0, active.get())

        val immediate = LiveSource(0) { emit: (Int) -> Unit ->
            active.incrementAndGet()
            emit(1)
            val cancel: () -> Unit = { active.decrementAndGet() }
            cancel
        }
        assertEquals(1, immediate.flow.first())
        eventually("not (active.get() != 0)", 5000) { !(active.get() != 0) }
    }

    /** PERF-16: a slow reader gets one pending result that holds every replacement pair of the burst. */
    @Test
    fun slowConsumerStreamKeepsOnePendingResultAndEveryReplacementPair() = bounded {
        for (burst in listOf(10, 100, 1000)) {
            val latest = Latest<Snapshot<Moment>> { older, newer -> Snapshot.merging(older, newer) }
            repeat(burst) { step -> latest.put(Snapshot(emptyList(), mapOf("real$step" to "temp$step"))) }
            assertEquals(burst, latest.next().replaced.size)
            assertTrue(!latest.hasPending)
        }
    }

    /** A chain of replacements across missed deliveries reads as one pair from the first id to the last. */
    @Test
    fun aChainOfReplacementsAcrossMissedDeliveriesReadsAsOnePair() = bounded {
        val source = LiveSource(Snapshot<Moment>(emptyList(), emptyMap()), { older, newer -> Snapshot.merging(older, newer) }) { emit ->
            repeat(1000) { step -> emit(Snapshot(emptyList(), mapOf("id${step + 1}" to "id$step"))) }
            val cancel: () -> Unit = {}
            cancel
        }
        assertEquals(mapOf("id1000" to "id0"), withTimeout(5000) { source.flow.first() }.replaced)
    }

    /** REL-24: a newly opened database starts with no entity of the old one. */
    @Test
    fun aNewDatabaseStartsWithNoEntityOfTheOldOne() = bounded {
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, null)
        val moment = Moment.create(text = "old")
        val old = Moment.entity(moment.id)
        assertEquals("old", withTimeout(5000) { old.value.first { it != null } }?.text)

        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, null)
        val fresh = Moment.entity(moment.id)
        delay(300)
        assertNull(old.value.value)
        assertNull(fresh.value.value)
        assertTrue(fresh !== old)
        val created = Moment.create(text = "new")
        assertEquals("new", withTimeout(5000) { Moment.entity(created.id).value.first { it != null } }?.text)
    }
}

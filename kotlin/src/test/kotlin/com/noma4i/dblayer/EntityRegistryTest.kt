package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
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

private class CountingSource(private val cached: WireRow? = null) : EntitySource {
    val peeks = AtomicInteger()
    override fun cachedRow(id: String): WireRow? { peeks.incrementAndGet(); return cached?.takeIf { it.id == id } }
    val retained = java.util.concurrent.CopyOnWriteArrayList<String>()
    val released = java.util.concurrent.CopyOnWriteArrayList<String>()
    override fun retain(ids: List<String>) { retained.addAll(ids) }
    override fun release(ids: List<String>) { released.addAll(ids) }
}

class EntityRegistryTest {
    @Test
    fun cachedFirstReadAndRetainedNewerValueAreAvailableBeforeDelivery() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val row = Moment.create(text = "cached")
        Moment.recent.warm()
        val cached = DBlayer.database.peekRow("Moment", row.id)!!
        val registry = registry(CountingSource(cached))
        var held: Entity<Moment>? = registry.entity(row.id)
        assertEquals("cached", held!!.value.value?.text)
        val newer = moment(row.id, "newer")
        registry.apply(delivery(cached.revision + 1, listOf(newer)))
        assertEquals("newer", registry.entity(row.id).value.value?.text)
        held = null
        collectGarbage { registry.liveCount == 0 }
        val returned = registry.entity(row.id)
        assertEquals("newer", returned.value.value?.text)
        registry.apply(delivery(cached.revision, listOf(row)))
        assertEquals("newer", returned.value.value?.text)
        assertNull(registry.entity("missing").value.value)
    }

    /** REL-31 */
    @Test
    fun repeatedReadsOfOneRecordPeekTheCoreOnce() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val row = Moment.create(text = "cached")
        Moment.recent.warm()
        val source = CountingSource(DBlayer.database.peekRow("Moment", row.id)!!)
        val registry = registry(source)
        repeat(5) { assertEquals("cached", registry.entity(row.id).value.value?.text) }
        collectGarbage { registry.liveCount == 0 }
        repeat(5) { assertEquals("cached", registry.entity(row.id).value.value?.text) }
        assertEquals(1, source.peeks.get())
    }

    private fun moment(id: String, text: String) = Moment(id = id, uuid = id, text = text, likedByMe = false, media = JsonObject(emptyMap()), createdAt = "2026-10-08T00:00:00.000Z")

    private fun delivery(revision: Long, rows: List<Moment> = emptyList(), removed: List<String> = emptyList()) = EntityDeliveryRows(revision, rows.map { it to 1uL }, removed)

    private fun registry(source: EntitySource = CountingSource(), now: () -> Long = { 0 }) =
        Entities("Moment", Moment, now, source)

    private fun collectGarbage(until: () -> Boolean) = eventuallyBlocking("the garbage collector frees the readers") { until() || run { System.gc(); false } }

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

    @Test
    fun secondReaderReadsWarmValueSynchronously() {
        val source = CountingSource()
        val registry = registry(source)
        val first = registry.entity("1")
        registry.apply(delivery(1, listOf(moment("1", "warm"))))
        eventuallyBlocking("the first reader is retained") { source.retained.toList() == listOf("1") }
        val second = registry.entity("1")
        val fence = registry.entity("2")
        eventuallyBlocking("a later reader is retained") { "2" in source.retained }
        assertEquals("warm", second.value.value?.text)
        assertTrue(second === first && fence !== first)
        assertEquals(listOf("1", "2"), source.retained.toList())
    }

    @Test
    fun retainedValueIsDroppedAfterTheBound() {
        var clock = 0L
        val registry = registry(now = { clock })
        repeat(3) { index ->
            var entity: Entity<Moment>? = registry.entity("$index")
            registry.apply(delivery(1, listOf(moment("$index", "v"))))
            entity = null
            collectGarbage { registry.liveCount == 0 }
        }
        assertEquals(3, registry.retainedCount)
        val shown = registry.entity("1")
        assertTrue(shown.isStale && shown.value.value?.text == "v")
        clock = 3_600_000
        assertEquals(0, registry.retainedCount)
    }

    /** REL-24 */
    @Test
    fun resetDropsRetainedValuesAndKeepsEveryLeaseOfTheHeldEntities() {
        val registry = registry()
        val held = List(5) { registry.entity("$it") }
        repeat(5) { registry.entity("${it + 5}") }
        registry.apply(delivery(1, List(10) { moment("$it", "v") }))
        collectGarbage { registry.liveCount == 5 }
        assertEquals(5, registry.retainedCount)
        registry.reset()
        assertEquals(0, registry.retainedCount)
        assertEquals(5, registry.liveCount)
        registry.apply(delivery(2, removed = List(5) { "$it" }))
        assertTrue(held.all { it.value.value == null && !it.isClosed })
        registry.apply(delivery(3, List(5) { moment("$it", "next") }))
        assertTrue(held.all { it.value.value?.text == "next" })
    }

    /** REL-24: a reset of the account empties the entities that readers hold and the same objects receive the next rows. */
    @Test
    fun aResetEmptiesTheHeldEntityAndTheSameObjectReceivesTheNextRow() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val moment = Moment.create(text = "old")
        val held = Moment.entity(moment.id)
        assertEquals("old", withTimeout(5000) { held.value.first { it != null } }?.text)

        DBlayer.logout()
        withTimeout(5000) { held.value.first { it == null } }
        assertTrue(!held.isClosed)
        DBlayer.database.create("Moment", """{"id":"${moment.id}","text":"new"}""", "{}")
        assertEquals("new", withTimeout(5000) { held.value.first { it != null } }?.text)
        assertTrue(held === Moment.entity(moment.id))
    }

    /** REL-25: closing the database closes the held entities and the next open starts empty. */
    @Test
    fun aClosedDatabaseClosesTheHeldEntitiesAndTheNextOpenStartsEmpty() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val moment = Moment.create(text = "old")
        val held = Moment.entity(moment.id)
        assertEquals("old", withTimeout(5000) { held.value.first { it != null } }?.text)

        DBlayer.close()
        assertTrue(held.isClosed)
        assertNull(held.value.value)
        reopen(Config(null, null, 32u), modelsJSON, null)
        val fresh = Moment.entity(moment.id)
        val other = Moment.entity(Moment.create(text = "other").id)
        withTimeout(5000) { other.value.first { it != null } }
        assertNull(fresh.value.value)
        assertTrue(fresh !== held && !fresh.isClosed && held.isClosed)
    }

    /** Addendum 3a: a collection cancelled before, during or after the attach leaves no subscription. */
    @Test
    fun aCollectionCancelledAtAnyMomentLeavesNoSubscription() = bounded {
        val active = AtomicInteger()
        fun blocking(gate: CountDownLatch) = LiveSource<Int>(0) { emit ->
            active.incrementAndGet()
            gate.await()
            emit(1)
            val release: () -> Unit = { active.decrementAndGet() }
            release
        }
        val open = CountDownLatch(0)
        val before = launch { blocking(open).flow.first() }
        before.cancel()
        before.join()
        eventually("the attach that began cancelled is released", 5000) { active.get() == 0 }

        val duringGate = CountDownLatch(1)
        val during = launch { blocking(duringGate).flow.first() }
        eventually("the attach is running") { active.get() == 1 }
        during.cancel()
        duringGate.countDown()
        during.join()
        eventually("the attach cancelled while running is released", 5000) { active.get() == 0 }

        val immediate = LiveSource<Int>(0) { emit ->
            active.incrementAndGet()
            emit(1)
            val release: () -> Unit = { active.decrementAndGet() }
            release
        }
        immediate.flow.first()
        eventually("the attach cancelled afterwards is released", 5000) { active.get() == 0 }
    }

    private enum class Replacements { Independent, Chain }

    /** PERF-16: a slow reader gets one pending result that holds every replacement pair of the burst, a chain of replacements reads as one pair. */
    @Test
    fun aSlowReaderGetsOnePendingResultThatHoldsEveryReplacementPair() = bounded {
        val cases = listOf(Replacements.Independent to 10, Replacements.Independent to 100, Replacements.Independent to 1000, Replacements.Chain to 1000)
        for ((shape, burst) in cases) {
            val latest = Latest<Snapshot<Moment>> { older, newer -> Snapshot.merging(older, newer) }
            for (step in 0 until burst) {
                latest.put(Snapshot(emptyList(), if (shape == Replacements.Chain) mapOf("id${step + 1}" to "id$step") else mapOf("real$step" to "temp$step")))
            }
            val merged = latest.next()
            when (shape) {
                Replacements.Independent -> assertEquals("$shape $burst", burst, merged.replaced.size)
                Replacements.Chain -> assertEquals("$shape $burst", mapOf("id$burst" to "id0"), merged.replaced)
            }
            assertTrue("$shape $burst", !latest.hasPending)
        }
    }
}

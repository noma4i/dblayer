package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class EntityApplyTest {
    private class SilentSource : EntitySource {
        override fun cachedRow(id: String): WireRow? = null
        override fun retain(ids: List<String>) {}
        override fun release(ids: List<String>) {}
    }

    private class HeldEquality(private val entered: CountDownLatch, private val gate: CountDownLatch) : Map<String, JsonElement> by emptyMap() {
        override fun equals(other: Any?): Boolean {
            entered.countDown()
            gate.await(10, TimeUnit.SECONDS)
            return true
        }
        override fun hashCode() = 0
    }

    private fun moment(media: JsonElement) = Moment(id = "a", uuid = "a", text = "t", likedByMe = false, media = media, createdAt = "2026-10-08T00:00:00.000Z")

    private fun delivery(revision: Long, row: Moment) = EntityDeliveryRows(revision, listOf(row to 1uL), emptyList())

    @Test
    fun a_closed_entity_ignores_a_delivery_that_was_already_collected() {
        val cell = EntityCell<Moment>()
        cell.close()

        cell.accept(moment(JsonObject(emptyMap())), 1)
        cell.fail("late")
        cell.reset()

        assertEquals(EntityPhase.Closed, cell.phase.value)
        assertNull(cell.value.value)
    }

    @Test
    fun a_slow_delivery_does_not_block_a_reader_that_asks_for_another_entity() {
        val entered = CountDownLatch(1)
        val gate = CountDownLatch(1)
        val registry = Entities("Moment", Moment, { 0 }, SilentSource())
        val first = registry.entity("a")
        registry.apply(delivery(1, moment(JsonObject(HeldEquality(entered, gate)))))
        val applying = Thread { registry.apply(delivery(2, moment(JsonObject(emptyMap())))) }
        applying.start()
        assertTrue("the delivery reached the comparison of the old and the new value", entered.await(5, TimeUnit.SECONDS))

        val reading = Thread {
            registry.entity("b")
            registry.entity("a")
            first.value.value
            first.phase.value
            first.isStale
        }
        reading.start()
        reading.join(2000)
        val blocked = reading.isAlive
        gate.countDown()
        applying.join(5000)
        reading.join(5000)

        assertFalse("a reader waited for a delivery that was applying", blocked)
        assertTrue(first.value.value != null)
    }
}

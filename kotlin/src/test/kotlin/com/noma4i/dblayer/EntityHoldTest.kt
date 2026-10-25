package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.serialization.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class EntityHoldTest {
    private object NoSource : EntitySource {
        override fun cachedRow(id: String): WireRow? = null
        override fun retain(ids: List<String>) {}
        override fun release(ids: List<String>) {}
    }

    private fun moment(id: String, text: String) = Moment(id = id, uuid = id, text = text, likedByMe = false, media = JsonObject(emptyMap()), createdAt = "2026-10-08T00:00:00.000Z")

    @Test
    fun collectorsOfValueAndPhaseReceiveDeliveriesAndCancelOnlyEndsTheCancelled() = bounded {
        val registry = Entities("Moment", Moment, { 0 }, NoSource)
        val entity = registry.entity("1")
        val untilB = async(Dispatchers.Unconfined, CoroutineStart.UNDISPATCHED) { entity.value.first { it?.text == "B" } }
        val untilPresent = async(Dispatchers.Unconfined, CoroutineStart.UNDISPATCHED) { entity.phase.first { it == EntityPhase.Present } }
        val cancelled = async(Dispatchers.Unconfined, CoroutineStart.UNDISPATCHED) { entity.value.first { it?.text == "C" } }
        val kept = async(Dispatchers.Unconfined, CoroutineStart.UNDISPATCHED) { entity.value.first { it?.text == "C" } }
        assertTrue(untilB.isActive && untilPresent.isActive && cancelled.isActive && kept.isActive)

        registry.apply(EntityDeliveryRows(1, listOf(moment("1", "B") to 1uL), emptyList()))
        assertTrue(untilB.isCompleted && untilPresent.isCompleted)
        assertEquals("B", untilB.await()?.text)

        cancelled.cancel()
        assertTrue(cancelled.isCancelled && kept.isActive)
        registry.apply(EntityDeliveryRows(2, listOf(moment("1", "C") to 1uL), emptyList()))
        assertTrue(kept.isCompleted)
        assertEquals("C", kept.await()?.text)
    }
}

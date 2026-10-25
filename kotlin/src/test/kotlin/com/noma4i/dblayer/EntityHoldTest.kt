package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
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

    private fun collectGarbage(rounds: Int, until: () -> Boolean): Boolean {
        repeat(rounds) { if (until()) return true; System.gc(); Thread.sleep(20) }
        return until()
    }

    @Test
    fun collectingValueOrPhaseHoldsTheEntityUntilTheCollectionEnds() = runBlocking {
        val registry = Entities("Moment", Moment, { 0 }, NoSource)
        var entity: Entity<Moment>? = registry.entity("1")
        val valueReader = launch(Dispatchers.Unconfined, CoroutineStart.UNDISPATCHED) { entity!!.value.first { it?.text == "B" } }
        val phaseReader = launch(Dispatchers.Unconfined, CoroutineStart.UNDISPATCHED) { entity!!.phase.first { it == EntityPhase.Closed } }
        entity = null
        assertTrue("both readers still run", valueReader.isActive && phaseReader.isActive)
        collectGarbage(10) { false }
        assertEquals(1, registry.liveCount)
        registry.apply(EntityDeliveryRows(1, listOf(moment("1", "B") to 1uL), emptyList()))
        valueReader.join()
        collectGarbage(10) { false }
        assertEquals("a reader of the phase holds the entity alone", 1, registry.liveCount)
        phaseReader.cancelAndJoin()
        assertTrue("the entity is freed after its readers end", collectGarbage(100) { registry.liveCount == 0 })
    }
}

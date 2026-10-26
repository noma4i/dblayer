package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import kotlinx.serialization.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Test

class EntityPhaseTest {
    private object NoSource : EntitySource {
        override fun cachedRow(id: String): WireRow? = null
        override fun retain(ids: List<String>, fetch: EntityFetch) {}
        override fun release(ids: List<String>) {}
    }

    private fun moment(id: String, text: String) = Moment(id = id, uuid = id, text = text, likedByMe = false, media = JsonObject(emptyMap()), createdAt = "2026-10-08T00:00:00.000Z")

    @Test
    fun phaseFollowsTheDeliveriesOfTheCore() {
        val registry = Entities("Moment", Moment, { 0 }, NoSource)
        val entity = registry.entity("1")
        assertEquals(EntityPhase.Opening, entity.phase.value)
        registry.apply(EntityDeliveryRows(1, listOf(moment("1", "A") to 1uL), emptyList()))
        assertEquals(EntityPhase.Present, entity.phase.value)
        registry.apply(EntityDeliveryRows(2, emptyList(), emptyList(), failedIds = mapOf("1" to "bad row")))
        assertEquals(EntityPhase.Failed("bad row"), entity.phase.value)
        assertEquals("A", entity.value.value?.text)
        registry.apply(EntityDeliveryRows(3, emptyList(), listOf("1")))
        assertEquals(EntityPhase.Absent, entity.phase.value)
        registry.apply(EntityDeliveryRows(4, emptyList(), emptyList(), failure = "read failed"))
        assertEquals(EntityPhase.Failed("read failed"), entity.phase.value)
    }
}

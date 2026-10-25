package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class SilentErrorTest {
    private fun wire(id: String) = WireRow(id, 1L, 1uL, Moment.layout,
        listOf(Cell.Text(id), Cell.Null, Cell.Text(id), Cell.Bool(false), Cell.Keyed(emptyList()), Cell.Null, Cell.Text("x")),
        WireSync(WireSyncState.SYNCED, emptyList(), emptyList(), null, 0u, 0L), null)

    private fun broken(id: String) = wire(id).copy(layout = "other")

    private fun full(vararg rows: WireRow) = LiveChange(1L, true, rows.size.toUInt(), rows.toList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), null, null, inserted = rows.indices.map { it.toUInt() })

    @Test
    fun aRowThatDoesNotDecodeReachesTheStateAsAFailure() {
        val feed = ListFeed(Moment)
        val state = feed.apply(full(wire("1"), broken("2")))
        assertEquals(listOf("2"), state.undecodable)
        assertTrue(state.failure.orEmpty(), state.failure != null)
        assertNull(ListFeed(Moment).apply(full(wire("1"))).failure)
    }

    @Test
    fun theFailureOutlivesTheDeltaThatDidNotTouchTheBrokenRow() {
        val feed = ListFeed(Moment)
        feed.apply(full(wire("1"), broken("2")))
        val delta = LiveChange(2L, false, 3u, listOf(wire("3")), listOf(2u), emptyList(), listOf(Placement("3", 2u)), emptyList(), emptyList(), null, null, inserted = listOf(0u))
        assertTrue(feed.apply(delta).failure != null)
    }

    @Test
    fun cachedReadsThrowWhenARowDoesNotDecode() { bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val row = Moment.create(text = "m")
        Moment.recent.warm()
        val failing = object : ModelDecoder<Moment> by Moment {
            override fun decode(row: WireRow): Moment = throw DbException.Invalid("cannot decode")
        }
        val query = Query("Moment", failing)
        query.warm()
        assertTrue(runCatching { query.cached() }.exceptionOrNull() is DbException)
        assertTrue(runCatching { query.cachedRow(row.id) }.exceptionOrNull() is DbException)
    } }

    @Test
    fun aSeedThatDoesNotDecodeFailsTheEntityPhase() {
        val source = object : EntitySource {
            override fun cachedRow(id: String): WireRow? = broken(id)
            override fun retain(ids: List<String>) {}
            override fun release(ids: List<String>) {}
        }
        val entity = Entities("Moment", Moment, { 0 }, source).entity("1")
        assertTrue(entity.phase.value.toString(), entity.phase.value is EntityPhase.Failed)
        assertNull(entity.value.value)
    }

    @Test
    fun aSeedReadThatThrowsFailsTheEntityPhase() {
        val source = object : EntitySource {
            override fun cachedRow(id: String): WireRow? = throw DbException.Invalid("peek failed")
            override fun retain(ids: List<String>) {}
            override fun release(ids: List<String>) {}
        }
        val entity = Entities("Moment", Moment, { 0 }, source).entity("1")
        assertTrue(entity.phase.value.toString(), entity.phase.value is EntityPhase.Failed)
    }
}

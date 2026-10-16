package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import org.junit.Assert.assertEquals
import org.junit.Test
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class ListWindowTest {
    private val byText get() = Query("Moment", Moment).order(Order("text", "asc"))

    private fun open() {
        DBlayer.database = Database.open(Config(null, null, 64u), modelsJSON, null)
    }

    @Test
    fun a_window_decodes_only_its_rows_at_every_list_size() = bounded {
        val costs = listOf(20, 200, 2000).map { count ->
            open()
            val created = (0 until count).map { Moment.create(text = "t%05d".format(it)) }
            val feed = ListFeed(Moment)
            val states = LinkedBlockingQueue<List<Moment>>()
            val subscription = DBlayer.database.liveWindow(byText.json, 10u, ChangeListener { states.put(feed.apply(it).rows) })
            assertEquals(10, states.poll(20, TimeUnit.SECONDS)!!.size)
            val atStart = feed.decodedRows
            created[count - 1].update(text = "t%05dx".format(count - 1))
            assertEquals("t%05dx".format(count - 1), states.poll(20, TimeUnit.SECONDS)!!.last().text)
            val afterInside = feed.decodedRows
            created[0].update(text = "t00000x")
            created[count - 1].update(text = "t%05dy".format(count - 1))
            assertEquals("t%05dy".format(count - 1), states.poll(20, TimeUnit.SECONDS)!!.last().text)
            subscription.cancel()
            listOf(atStart, afterInside - atStart, feed.decodedRows - afterInside)
        }
        assertEquals(List(3) { listOf(10, 1, 1) }, costs)
    }

    @Test
    fun a_live_window_follows_the_tail_and_expands_before() = bounded {
        open()
        repeat(30) { Moment.create(text = "t%05d".format(it)) }
        val scope = CoroutineScope(Dispatchers.Default)
        val live = LiveWindow(byText.window(tail = 5), scope)
        eventually("the window") { live.rows.value.size == 5 }
        assertEquals((25 until 30).map { "t%05d".format(it) }, live.rows.value.map { it.text })
        assertEquals(WindowEdges(true, false, 30), live.edges.value)
        val held = ArrayList(live.rows.value)
        live.takeEdits()

        Moment.create(text = "t00030")
        eventually("the new tail row") { live.rows.value.lastOrNull()?.text == "t00030" }
        held.replay(live.takeEdits() ?: emptyList())
        assertEquals(live.rows.value.map { it.id }, held.map { it.id })
        assertEquals(31, live.edges.value.total)

        live.expand(before = 3)
        eventually("the wider window") { live.rows.value.size == 8 }
        held.replay(live.takeEdits() ?: emptyList())
        assertEquals(live.rows.value.map { it.id }, held.map { it.id })
        live.expand(before = 100)
        eventually("every row") { live.rows.value.size == 31 }
        assertEquals(WindowEdges(false, false, 31), live.edges.value)
        scope.cancel()
    }
}

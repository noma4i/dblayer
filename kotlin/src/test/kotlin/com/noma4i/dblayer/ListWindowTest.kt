package com.noma4i.dblayer

import com.noma4i.dblayer.models.Record
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

    private suspend fun open() {
        reopen(Config(null, null, 64u), modelsJSON, null)
    }

    @Test
    fun a_live_window_follows_the_tail_and_expands_before() = bounded {
        open()
        repeat(30) { Moment.create(text = "t%05d".format(it)) }
        val scope = CoroutineScope(Dispatchers.Default)
        val live = LiveWindow(byText.window(tail = 5), scope)
        eventually("the window") { live.rows.value.size == 5 }
        assertEquals((25 until 30).map { "t%05d".format(it) }, live.rows.value.map { it.text })
        assertEquals(WindowEdges(true, false, 30L), live.edges.value)
        val held = ArrayList(live.rows.value)
        live.takeUpdate()

        Moment.create(text = "t00030")
        eventually("the new tail row") { live.rows.value.lastOrNull()?.text == "t00030" }
        held.replay(live.takeUpdate().edits ?: emptyList())
        assertEquals(live.rows.value.map { it.id }, held.map { it.id })
        assertEquals(31L, live.edges.value.total)

        live.expand(before = 3)
        eventually("the wider window") { live.rows.value.size == 8 }
        held.replay(live.takeUpdate().edits ?: emptyList())
        assertEquals(live.rows.value.map { it.id }, held.map { it.id })
        live.expand(before = 100)
        eventually("every row") { live.rows.value.size == 31 }
        assertEquals(WindowEdges(false, false, 31L), live.edges.value)
        scope.cancel()
    }
}

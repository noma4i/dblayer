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
        assertEquals(live.rows.value.map { it.id }, live.snapshot.value.rows.map { it.id })
        assertEquals(live.edges.value, live.snapshot.value.window)
        val held = ArrayList(live.rows.value)
        live.takeUpdate()

        Moment.create(text = "t00030")
        eventually("the new tail row") { live.rows.value.lastOrNull()?.text == "t00030" }
        held.replay(live.takeUpdate().edits ?: emptyList())
        assertEquals(live.rows.value.map { it.id }, held.map { it.id })
        assertEquals(31L, live.edges.value.total)

        live.expand(before = 3)
        eventually("the wider window") { live.rows.value.size == 8 }
        assertEquals(live.rows.value.map { it.id }, live.snapshot.value.rows.map { it.id })
        held.replay(live.takeUpdate().edits ?: emptyList())
        assertEquals(live.rows.value.map { it.id }, held.map { it.id })
        live.expand(before = 100)
        eventually("every row") { live.rows.value.size == 31 }
        assertEquals(WindowEdges(false, false, 31L), live.edges.value)
        scope.cancel()
    }

    @Test
    fun a_related_window_reads_the_senders_of_the_window_rows_only() = bounded {
        open()
        listOf("u1" to "Ann", "u2" to "Bob", "u3" to "Cy").forEach { (id, name) -> DBlayer.database.create("User", """{"id":"$id","name":"$name"}""", "{}") }
        listOf("u2", "u2", "u1", "u3").forEachIndexed { number, user -> DBlayer.database.create("Moment", """{"text":"t%05d","userId":"$user"}""".format(number), "{}") }
        val senders = byText.window(tail = 2).related(Moment.Relations.author)
        assertEquals(listOf("Ann", "Cy"), senders.live.current().map { it.name })
    }

    @Test
    fun a_window_that_grows_past_max_shrinks_to_its_first_size() = bounded {
        open()
        repeat(30) { Moment.create(text = "t%05d".format(it)) }
        val scope = CoroutineScope(Dispatchers.Default)
        val live = LiveWindow(byText.window(tail = 5, max = 8), scope)
        eventually("the window") { live.rows.value.size == 5 }
        live.expand(before = 3)
        eventually("the widest window") { live.rows.value.size == 8 }
        live.expand(before = 1)
        eventually("the shrunk window") { live.rows.value.size == 5 }
        assertEquals((25 until 30).map { "t%05d".format(it) }, live.rows.value.map { it.text })
        scope.cancel()
    }
}

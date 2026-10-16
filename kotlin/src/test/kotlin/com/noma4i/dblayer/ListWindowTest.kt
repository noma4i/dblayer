package com.noma4i.dblayer

import com.noma4i.dblayer.models.Message
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

    private fun node(number: Int, text: String = "t$number") =
        """{"__typename":"Message","id":"m$number","chatId":"c1","authorId":"a","text":"$text","createdAt":"2026-10-0${number}T00:00:00.000Z","sequenceNumber":$number}"""

    private fun reply(nodes: List<String>) =
        """{"data":{"chat":{"messages":{"nodes":[${nodes.joinToString(",")}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}"""

    @Test
    fun a_window_over_the_thread_shape_decodes_only_its_rows() = bounded {
        val transport = MockTransport()
        transport.enqueue("ChatThread", reply((1..7).map { node(it) }))
        DBlayer.database = Database.open(Config(null, null, 64u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"ChatThread","document":"query ChatThread(${'$'}chatId: ID!, ${'$'}first: Int, ${'$'}after: String) { chat(id: ${'$'}chatId) { messages(first: ${'$'}first, after: ${'$'}after) { nodes { id chatId authorId text createdAt sequenceNumber } pageInfo { hasNextPage endCursor } } } }"},{"name":"CreateMessage","document":"mutation CreateMessage(${'$'}chatId: ID!, ${'$'}text: String, ${'$'}signedId: String, ${'$'}attachedUserId: ID) { createMessage(chatId: ${'$'}chatId, text: ${'$'}text, signedId: ${'$'}signedId, attachedUserId: ${'$'}attachedUserId) { id chatId authorId text createdAt sequenceNumber } }"}]""")
        val thread = Message.thread("c1")
        thread.load()
        val feed = ListFeed(Message)
        val states = LinkedBlockingQueue<List<Message>>()
        val subscription = DBlayer.database.liveWindow(thread.json, 3u, ChangeListener { states.put(feed.apply(it).rows) })
        fun next(what: String, wanted: List<String>): List<Message> {
            while (true) {
                val rows = states.poll(20, TimeUnit.SECONDS) ?: throw AssertionError("timed out waiting for $what")
                if (rows.map { it.text } == wanted) return rows
            }
        }
        next("the last 3 rows", listOf("t5", "t6", "t7"))
        val atStart = feed.decodedRows

        transport.enqueue("ChatThread", reply((1..7).map { node(it, if (it == 1 || it == 6) "t${it}x" else "t$it") }))
        thread.refresh()
        next("the edited row inside", listOf("t5", "t6x", "t7"))
        val afterEdits = feed.decodedRows

        transport.enqueue("CreateMessage", """{"data":{"createMessage":{"__typename":"Message","id":"m8","chatId":"c1","authorId":"a","text":"t8","createdAt":"2026-10-08T00:00:00.000Z","sequenceNumber":null}}}""")
        Message.create(chatId = "c1", authorId = "a", text = "t8")
        next("the new last row", listOf("t6x", "t7", "t8"))
        assertEquals(listOf(3, 1), listOf(atStart, afterEdits - atStart))
        assertEquals(true, feed.decodedRows - afterEdits <= 2)
        subscription.cancel()
    }
}

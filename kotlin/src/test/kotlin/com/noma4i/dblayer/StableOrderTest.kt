package com.noma4i.dblayer

import com.noma4i.dblayer.models.Message
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Test
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class StableOrderTest {
    @Test
    fun lands_declaration_and_row_cross_the_bridge() = bounded {
        val schema = """[{"key":"Record","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}}],"actions":[{"name":"create","root_shape":"upsert","mutation":"CreateRecord","root":"createRecord.record","has_optimistic":true,"has_temp_row_retention":true,"lands":[{"model":"Record","root":"createRecord.extra"}]}]}]"""
        val transport = MockTransport()
        transport.enqueue("CreateRecord", """{"data":{"createRecord":{"record":{"id":"r1"},"extra":{"id":"r2"}}}}""")
        val database = Database.open(Config(null, null, 64u), schema, transport)
        database.registerOperations("""[{"name":"CreateRecord","document":"mutation CreateRecord { createRecord { record { id } extra { id } } }"}]""")
        val row = database.create("Record", "{}", "{}")
        database.settled("Record", row.id)
        database.warm("""{"model":"Record"}""")
        assertEquals("r2", database.peekRow("Record", "r2")?.id)
    }

    private fun node(id: String, sequence: Int, text: String) =
        """{"__typename":"Message","id":"$id","chatId":"c1","authorId":"a","text":"$text","createdAt":"2026-10-0${sequence}T00:00:00.000Z","sequenceNumber":$sequence}"""

    @Test
    fun a_confirmed_message_keeps_its_place_in_the_thread_window() = bounded {
        val transport = MockTransport()
        val page = listOf(node("m1", 1, "t1"), node("m2", 2, "t2"), node("m5", 5, "t5"))
        transport.enqueue("ChatThread", """{"data":{"chat":{"messages":{"nodes":[${page.joinToString(",")}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}""")
        DBlayer.database = Database.open(Config(null, null, 64u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"ChatThread","document":"query ChatThread(${'$'}chatId: ID!, ${'$'}first: Int, ${'$'}after: String) { chat(id: ${'$'}chatId) { messages(first: ${'$'}first, after: ${'$'}after) { nodes { id chatId authorId text createdAt sequenceNumber } pageInfo { hasNextPage endCursor } } } }"},{"name":"CreateMessage","document":"mutation CreateMessage(${'$'}chatId: ID!, ${'$'}text: String, ${'$'}signedId: String, ${'$'}attachedUserId: ID) { createMessage(chatId: ${'$'}chatId, text: ${'$'}text, signedId: ${'$'}signedId, attachedUserId: ${'$'}attachedUserId) { id chatId authorId text createdAt sequenceNumber } }"}]""")
        val thread = Message.thread("c1")
        thread.load()
        val feed = ListFeed(Message)
        val states = LinkedBlockingQueue<List<Message>>()
        val subscription = DBlayer.database.liveWindow(thread.json, 10u, ChangeListener { states.put(feed.apply(it).rows) })
        fun next(what: String, wanted: (List<Message>) -> Boolean): List<Message> {
            while (true) {
                val rows = states.poll(20, TimeUnit.SECONDS) ?: throw AssertionError("timed out waiting for $what")
                if (wanted(rows)) return rows
            }
        }
        next("the loaded page") { rows -> rows.map { it.text } == listOf("t1", "t2", "t5") }

        transport.enqueue("CreateMessage", """{"data":{"createMessage":{"__typename":"Message","id":"m9","chatId":"c1","authorId":"a","text":"mine","createdAt":"2026-10-09T00:00:00.000Z","sequenceNumber":3}}}""")
        Message.create(chatId = "c1", authorId = "a", text = "mine")
        val confirmed = next("the confirmed message after the rows shown before it") { rows -> rows.lastOrNull()?.id == "m9" }
        assertEquals(listOf("t1", "t2", "t5", "mine"), confirmed.map { it.text })
        subscription.cancel()
    }
}

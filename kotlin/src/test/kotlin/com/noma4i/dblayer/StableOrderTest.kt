package com.noma4i.dblayer

import com.noma4i.dblayer.models.Message
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Test
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class StableOrderTest {
    @Test
    fun lands_adds_a_new_message_to_the_loaded_thread() = bounded {
        val models = parseJson(modelsJSON).jsonArray.map { model ->
            if (model.jsonObject["key"]?.jsonPrimitive?.content != "Message") model else {
                val action = parseJson("""{"name":"land","root_shape":"upsert","mutation":"LandMessage","lands":[{"model":"Message","root":"landMessage.extra"}]}""")
                JsonObject(model.jsonObject + ("actions" to JsonArray(model.jsonObject.getValue("actions").jsonArray + action)))
            }
        }
        val transport = MockTransport()
        val page = listOf(node("m1", 1, "t1"), node("m5", 5, "t5"))
        transport.enqueue("ChatThread", """{"data":{"chat":{"messages":{"nodes":[${page.joinToString(",")}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}""")
        DBlayer.database = Database.open(Config(null, null, 64u), JsonArray(models).toString(), transport)
        DBlayer.database.registerOperations("""[{"name":"ChatThread","document":"query ChatThread(${'$'}chatId: ID!, ${'$'}first: Int, ${'$'}after: String) { chat(id: ${'$'}chatId) { messages(first: ${'$'}first, after: ${'$'}after) { nodes { id chatId authorId text createdAt sequenceNumber } pageInfo { hasNextPage endCursor } } } }"},{"name":"LandMessage","document":"mutation LandMessage { landMessage { extra { id chatId authorId text createdAt sequenceNumber } } }"}]""")
        val thread = Message.thread("c1")
        thread.load()
        repeat(2) {
            transport.enqueue("LandMessage", """{"data":{"landMessage":{"extra":${node("m3", 3, "landed")}}}}""")
            DBlayer.database.act("Message", "m1", "land", "{}", "{}")
            DBlayer.database.settled("Message", "m1")
            DBlayer.database.warm(thread.json)
            assertEquals(listOf("m1", "m3", "m5"), DBlayer.database.peekQuery(thread.json)?.rows?.map { it.id })
        }
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

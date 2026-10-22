package com.noma4i.dblayer

import com.noma4i.dblayer.models.Record
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
        """{"__typename":"Record","id":"$id","containerId":"c1","authorId":"a","text":"$text","createdAt":"2026-10-0${sequence}T00:00:00.000Z","sequenceNumber":$sequence}"""

    @Test
    fun a_confirmed_record_keeps_its_place_in_the_thread_window() = bounded {
        val transport = MockTransport()
        val page = listOf(node("m1", 1, "t1"), node("m2", 2, "t2"), node("m5", 5, "t5"))
        transport.enqueue("ContainerThread", """{"data":{"container":{"records":{"nodes":[${page.joinToString(",")}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}""")
        DBlayer.database = Database.open(Config(null, null, 64u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"ContainerThread","document":"query ContainerThread(${'$'}containerId: ID!, ${'$'}first: Int, ${'$'}after: String) { container(id: ${'$'}containerId) { records(first: ${'$'}first, after: ${'$'}after) { nodes { id containerId authorId text createdAt sequenceNumber } pageInfo { hasNextPage endCursor } } } }"},{"name":"CreateRecord","document":"mutation CreateRecord(${'$'}containerId: ID!, ${'$'}text: String, ${'$'}signedId: String, ${'$'}attachedUserId: ID) { createRecord(containerId: ${'$'}containerId, text: ${'$'}text, signedId: ${'$'}signedId, attachedUserId: ${'$'}attachedUserId) { id containerId authorId text createdAt sequenceNumber } }"}]""")
        val thread = Record.thread("c1")
        thread.load()
        val feed = ListFeed(Record)
        val states = LinkedBlockingQueue<List<Record>>()
        val subscription = DBlayer.database.liveWindow(thread.json, 10u, ChangeListener { states.put(feed.apply(it).rows) })
        fun next(what: String, wanted: (List<Record>) -> Boolean): List<Record> {
            while (true) {
                val rows = states.poll(20, TimeUnit.SECONDS) ?: throw AssertionError("timed out waiting for $what")
                if (wanted(rows)) return rows
            }
        }
        next("the loaded page") { rows -> rows.map { it.text } == listOf("t1", "t2", "t5") }

        transport.enqueue("CreateRecord", """{"data":{"createRecord":{"__typename":"Record","id":"m9","containerId":"c1","authorId":"a","text":"mine","createdAt":"2026-10-09T00:00:00.000Z","sequenceNumber":3}}}""")
        Record.create(containerId = "c1", authorId = "a", text = "mine")
        val confirmed = next("the confirmed record after the rows shown before it") { rows -> rows.lastOrNull()?.id == "m9" }
        assertEquals(listOf("t1", "t2", "t5", "mine"), confirmed.map { it.text })
        subscription.cancel()
    }
}

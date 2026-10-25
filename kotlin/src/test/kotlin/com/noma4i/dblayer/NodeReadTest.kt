package com.noma4i.dblayer

import com.noma4i.dblayer.models.Container
import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertEquals
import org.junit.Test

class NodeReadTest {
    private val documents = listOf(
        "ContainerNode" to "query ContainerNode(\$id: ID!) { node(id: \$id) { ... on Container { id title shareUrl isPublic } } }",
        "ContainersByIds" to "query ContainersByIds(\$ids: [ID!]!) { nodes(ids: \$ids) { ... on Container { id title } } }",
        "MomentTranscodeStatus" to "query MomentTranscodeStatus(\$momentId: ID!) { node(id: \$momentId) { ... on Moment { id uuid createdAt media } } }",
    )

    private suspend fun open(): MockTransport {
        val transport = MockTransport()
        reopen(Config(null, null, 64u), modelsJSON, transport)
        val defs = documents.map { (name, document) -> buildJsonObject { put("name", JsonPrimitive(name)); put("document", JsonPrimitive(document)) } }
        DBlayer.database.registerOperations(JsonArray(defs).toString())
        return transport
    }

    private fun MockTransport.callsOf(operation: String) = calls.filter { it.operationName == operation }

    @Test
    fun nodePassesItsIdAndForceAndDecodesTheTypedRow() = bounded {
        val transport = open()
        transport.enqueue("ContainerNode", """{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}""")
        transport.enqueue("ContainerNode", """{"data":{"node":{"id":"c1","title":"two","shareUrl":"v","isPublic":false}}}""")

        val first = Container.node("c1")
        assertEquals(Triple("one", "u", true), Triple(first?.title, first?.shareUrl, first?.isPublic))
        assertEquals("""{"id":"c1"}""", transport.callsOf("ContainerNode").first().variablesJson)
        val rereads = Container.node("c1", force = true)
        assertEquals("two", rereads?.title)
        assertEquals(2, transport.callsOf("ContainerNode").size)
    }

    @Test
    fun theVariableNameOfANodeReadFollowsTheDeclaration() = bounded {
        val transport = open()
        transport.enqueue("MomentTranscodeStatus", """{"data":{"node":{"id":"mo1","uuid":"u","createdAt":"2026-10-01T00:00:00.000Z","media":{"kind":"VIDEO"}}}}""")
        assertEquals("mo1", Moment.transcodeStatus("mo1")?.id)
        assertEquals("""{"momentId":"mo1"}""", transport.callsOf("MomentTranscodeStatus").first().variablesJson)
    }

    @Test
    fun nodesPassesIdsAndDecodesTypedRows() = bounded {
        val transport = open()
        transport.enqueue("ContainersByIds", """{"data":{"nodes":[{"id":"c2","title":"b"},{"id":"c1","title":"a"}]}}""")
        assertEquals(mapOf("c2" to "b", "c1" to "a"), Container.nodes(listOf("c2", "c1")).associate { it.id to it.title })
        assertEquals(1, transport.callsOf("ContainersByIds").size)
        assertEquals("""{"ids":["c2","c1"]}""", transport.callsOf("ContainersByIds").first().variablesJson)
    }

}

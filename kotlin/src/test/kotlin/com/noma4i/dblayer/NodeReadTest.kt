package com.noma4i.dblayer

import com.noma4i.dblayer.models.Container
import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class NodeReadTest {
    private val documents = listOf(
        "ContainerNode" to "query ContainerNode(\$id: ID!) { node(id: \$id) { ... on Container { id title shareUrl isPublic } } }",
        "ContainersByIds" to "query ContainersByIds(\$ids: [ID!]!) { nodes(ids: \$ids) { ... on Container { id title } } }",
        "ContainerMedia" to "query ContainerMedia(\$containerId: ID!, \$first: Int, \$after: String) { node(id: \$containerId) { ... on Container { records(first: \$first, after: \$after) { edges { node { id containerId authorId text createdAt media { kind transcoded fileUrl } } } pageInfo { hasNextPage endCursor } } } } }",
        "RecordTranscodeStatus" to "query RecordTranscodeStatus(\$id: ID!) { node(id: \$id) { ... on Record { id containerId media { kind transcoded fileUrl } } } }",
        "MomentTranscodeStatus" to "query MomentTranscodeStatus(\$momentId: ID!) { node(id: \$momentId) { ... on Moment { id uuid createdAt media } } }",
    )

    private suspend fun open(models: String = modelsJSON): MockTransport {
        val transport = MockTransport()
        DBlayer.database = Database.open(Config(null, null, 64u), models, transport)
        val defs = documents.map { (name, document) -> buildJsonObject { put("name", JsonPrimitive(name)); put("document", JsonPrimitive(document)) } }
        DBlayer.database.registerOperations(JsonArray(defs).toString())
        return transport
    }

    private fun MockTransport.callsOf(operation: String) = calls.filter { it.operationName == operation }

    @Test
    fun aNodeReadReturnsTheTypedRowAndOnlyAnEventForcesAnotherRequest() = bounded {
        val transport = open()
        transport.enqueue("ContainerNode", """{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}""")
        transport.enqueue("ContainerNode", """{"data":{"node":{"id":"c1","title":"two"}}}""")

        val first = Container.node("c1")
        assertEquals(Triple("one", "u", true), Triple(first?.title, first?.shareUrl, first?.isPublic))
        assertEquals("""{"id":"c1"}""", transport.callsOf("ContainerNode").first().variablesJson)
        assertEquals("one", Container.node("c1")?.title)
        assertEquals(1, transport.callsOf("ContainerNode").size)
        val rereads = Container.node("c1", force = true)
        assertEquals(Pair("two", "u"), Pair(rereads?.title, rereads?.shareUrl))
        assertEquals(2, transport.callsOf("ContainerNode").size)
    }

    @Test
    fun aRelationUnderNodeLandsAndASubsetReadChangesOnlyTheSelectedFields() = bounded {
        val transport = open()
        val record = """{"id":"m1","containerId":"c1","authorId":"a","text":"keep","createdAt":"2026-10-01T00:00:00.000Z","media":{"kind":"VIDEO","transcoded":false,"fileUrl":"http://f"}}"""
        transport.enqueue("ContainerMedia", """{"data":{"node":{"records":{"edges":[{"node":$record}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}""")
        Record.containerMedia("c1").load()
        assertEquals("keep", Record.find("m1").current()?.text)

        transport.enqueue("RecordTranscodeStatus", """{"data":{"node":{"id":"m1","containerId":"c1","media":{"kind":"VIDEO","transcoded":true,"fileUrl":""}}}}""")
        val status = Record.transcodeStatus("m1")
        assertEquals(Triple("keep", true, "http://f"), Triple(status?.text, status?.media?.transcoded, status?.media?.fileUrl))
        assertEquals("""{"id":"m1"}""", transport.callsOf("RecordTranscodeStatus").first().variablesJson)
    }

    @Test
    fun theVariableNameOfANodeReadFollowsTheDeclaration() = bounded {
        val transport = open()
        transport.enqueue("MomentTranscodeStatus", """{"data":{"node":{"id":"mo1","uuid":"u","createdAt":"2026-10-01T00:00:00.000Z","media":{"kind":"VIDEO"}}}}""")
        assertEquals("mo1", Moment.transcodeStatus("mo1")?.id)
        assertEquals("""{"momentId":"mo1"}""", transport.callsOf("MomentTranscodeStatus").first().variablesJson)
    }

    @Test
    fun nodesReadsManyInOneRequestInTheAnsweredOrder() = bounded {
        val transport = open()
        transport.enqueue("ContainersByIds", """{"data":{"nodes":[{"id":"c2","title":"b"},null,{},{"id":"c1","title":"a"}]}}""")
        assertEquals(listOf("b", "a"), Container.nodes(listOf("c2", "x", "y", "c1")).map { it.title })
        assertEquals(1, transport.callsOf("ContainersByIds").size)
        assertEquals("""{"ids":["c2","x","y","c1"]}""", transport.callsOf("ContainersByIds").first().variablesJson)
    }

    @Test
    fun anAnswerWithoutAMatchingTypeIsNullAndWritesNothing() = bounded {
        val transport = open()
        transport.enqueue("ContainerNode", """{"data":{"node":{"id":"c1","title":"old"}}}""")
        Container.node("c1")
        for (answer in listOf("""{"data":{"node":null}}""", """{"data":{"node":{}}}""", """{"data":{"node":{"__typename":"Gadget"}}}""")) {
            transport.enqueue("ContainerNode", answer)
            assertNull(Container.node("c1", force = true))
        }
        assertEquals("old", Container.find("c1").current()?.title)
    }

    @Test
    fun nodesKeepsDuplicatesInResponseOrder() = bounded {
        val transport = open()
        transport.enqueue("ContainersByIds", """{"data":{"nodes":[{"id":"c2","title":"old","shareUrl":"kept"},null,{"id":"c1","title":"middle"},{"id":"c2","title":"last"}]}}""")
        val rows = Container.nodes(listOf("c2", "missing", "c1", "c2"))
        assertEquals(listOf("c2", "c1", "c2"), rows.map { it.id })
        assertEquals(listOf("last", "middle", "last"), rows.map { it.title })
        assertEquals("kept", Container.find("c2").current()?.shareUrl)
    }

    @Test
    fun nullPathsClearAndMissingPathsPreserve() = bounded {
        val models = Json.parseToJsonElement(modelsJSON).jsonArray.map { element ->
            val model = element.jsonObject
            if (model["key"]?.jsonPrimitive?.content != "Container") model else JsonObject(model + ("fields" to JsonArray(model.getValue("fields").jsonArray.map {
                val field = it.jsonObject
                if (field["name"]?.jsonPrimitive?.content == "shareUrl") JsonObject(field + ("from" to JsonPrimitive("profile.url"))) else field
            })))
        }
        val transport = open(JsonArray(models).toString())
        for ((patch, expected) in listOf(""""profile":{}""" to "kept", """"profile":null""" to null, """"profile":{"url":null}""" to null)) {
            transport.enqueue("ContainerNode", """{"data":{"node":{"id":"c1","title":"container","profile":{"url":"kept"}}}}""")
            assertEquals("kept", Container.node("c1", force = true)?.shareUrl)
            transport.enqueue("ContainerNode", """{"data":{"node":{"id":"c1",$patch}}}""")
            assertEquals(expected, Container.node("c1", force = true)?.shareUrl)
        }
    }
}

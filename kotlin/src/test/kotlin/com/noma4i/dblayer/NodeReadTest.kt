package com.noma4i.dblayer

import com.noma4i.dblayer.models.Chat
import com.noma4i.dblayer.models.Message
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
        "ChatNode" to "query ChatNode(\$id: ID!) { node(id: \$id) { ... on Chat { id title shareUrl isPublic } } }",
        "ChatsByIds" to "query ChatsByIds(\$ids: [ID!]!) { nodes(ids: \$ids) { ... on Chat { id title } } }",
        "ChatMedia" to "query ChatMedia(\$chatId: ID!, \$first: Int, \$after: String) { node(id: \$chatId) { ... on Chat { messages(first: \$first, after: \$after) { edges { node { id chatId authorId text createdAt media { kind transcoded fileUrl } } } pageInfo { hasNextPage endCursor } } } } }",
        "MessageTranscodeStatus" to "query MessageTranscodeStatus(\$id: ID!) { node(id: \$id) { ... on Message { id chatId media { kind transcoded fileUrl } } } }",
        "MomentTranscodeStatus" to "query MomentTranscodeStatus(\$momentId: ID!) { node(id: \$momentId) { ... on Moment { id uuid createdAt media } } }",
    )

    private fun open(models: String = modelsJSON): MockTransport {
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
        transport.enqueue("ChatNode", """{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}""")
        transport.enqueue("ChatNode", """{"data":{"node":{"id":"c1","title":"two"}}}""")

        val first = Chat.node("c1")
        assertEquals(Triple("one", "u", true), Triple(first?.title, first?.shareUrl, first?.isPublic))
        assertEquals("""{"id":"c1"}""", transport.callsOf("ChatNode").first().variablesJson)
        assertEquals("one", Chat.node("c1")?.title)
        assertEquals(1, transport.callsOf("ChatNode").size)
        val rereads = Chat.node("c1", force = true)
        assertEquals(Pair("two", "u"), Pair(rereads?.title, rereads?.shareUrl))
        assertEquals(2, transport.callsOf("ChatNode").size)
    }

    @Test
    fun aRelationUnderNodeLandsAndASubsetReadChangesOnlyTheSelectedFields() = bounded {
        val transport = open()
        val message = """{"id":"m1","chatId":"c1","authorId":"a","text":"keep","createdAt":"2026-10-01T00:00:00.000Z","media":{"kind":"VIDEO","transcoded":false,"fileUrl":"http://f"}}"""
        transport.enqueue("ChatMedia", """{"data":{"node":{"messages":{"edges":[{"node":$message}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}""")
        Message.chatMedia("c1").load()
        assertEquals("keep", Message.find("m1").current()?.text)

        transport.enqueue("MessageTranscodeStatus", """{"data":{"node":{"id":"m1","chatId":"c1","media":{"kind":"VIDEO","transcoded":true,"fileUrl":""}}}}""")
        val status = Message.transcodeStatus("m1")
        assertEquals(Triple("keep", true, "http://f"), Triple(status?.text, status?.media?.transcoded, status?.media?.fileUrl))
        assertEquals("""{"id":"m1"}""", transport.callsOf("MessageTranscodeStatus").first().variablesJson)
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
        transport.enqueue("ChatsByIds", """{"data":{"nodes":[{"id":"c2","title":"b"},null,{},{"id":"c1","title":"a"}]}}""")
        assertEquals(listOf("b", "a"), Chat.nodes(listOf("c2", "x", "y", "c1")).map { it.title })
        assertEquals(1, transport.callsOf("ChatsByIds").size)
        assertEquals("""{"ids":["c2","x","y","c1"]}""", transport.callsOf("ChatsByIds").first().variablesJson)
    }

    @Test
    fun anAnswerWithoutAMatchingTypeIsNullAndWritesNothing() = bounded {
        val transport = open()
        transport.enqueue("ChatNode", """{"data":{"node":{"id":"c1","title":"old"}}}""")
        Chat.node("c1")
        for (answer in listOf("""{"data":{"node":null}}""", """{"data":{"node":{}}}""", """{"data":{"node":{"__typename":"Gadget"}}}""")) {
            transport.enqueue("ChatNode", answer)
            assertNull(Chat.node("c1", force = true))
        }
        assertEquals("old", Chat.find("c1").current()?.title)
    }

    @Test
    fun v3NodesKeepsDuplicatesInResponseOrder() = bounded {
        val transport = open()
        transport.enqueue("ChatsByIds", """{"data":{"nodes":[{"id":"c2","title":"old","shareUrl":"kept"},null,{"id":"c1","title":"middle"},{"id":"c2","title":"last"}]}}""")
        val rows = Chat.nodes(listOf("c2", "missing", "c1", "c2"))
        assertEquals(listOf("c2", "c1", "c2"), rows.map { it.id })
        assertEquals(listOf("last", "middle", "last"), rows.map { it.title })
        assertEquals("kept", Chat.find("c2").current()?.shareUrl)
    }

    @Test
    fun v3NullPathsClearAndMissingPathsPreserve() = bounded {
        val models = Json.parseToJsonElement(modelsJSON).jsonArray.map { element ->
            val model = element.jsonObject
            if (model["key"]?.jsonPrimitive?.content != "Chat") model else JsonObject(model + ("fields" to JsonArray(model.getValue("fields").jsonArray.map {
                val field = it.jsonObject
                if (field["name"]?.jsonPrimitive?.content == "shareUrl") JsonObject(field + ("from" to JsonPrimitive("profile.url"))) else field
            })))
        }
        val transport = open(JsonArray(models).toString())
        for ((patch, expected) in listOf(""""profile":{}""" to "kept", """"profile":null""" to null, """"profile":{"url":null}""" to null)) {
            transport.enqueue("ChatNode", """{"data":{"node":{"id":"c1","title":"chat","profile":{"url":"kept"}}}}""")
            assertEquals("kept", Chat.node("c1", force = true)?.shareUrl)
            transport.enqueue("ChatNode", """{"data":{"node":{"id":"c1",$patch}}}""")
            assertEquals(expected, Chat.node("c1", force = true)?.shareUrl)
        }
    }
}

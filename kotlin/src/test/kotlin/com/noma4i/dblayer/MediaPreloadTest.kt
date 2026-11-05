package com.noma4i.dblayer

import com.noma4i.dblayer.models.MediaMessage
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(DBlayerTestingApi::class)
class MediaPreloadTest {
    @Test
    fun aMediaRefetchAfterMutationReadsTheWholeStaleWindow() = bounded {
        try {
            DBlayer.close()
            val original = Json.parseToJsonElement(modelsJSON).jsonArray
            val moment = original.first { it.jsonObject["key"]?.jsonPrimitive?.content == "Moment" }.jsonObject
            val like = moment.getValue("actions").jsonArray.first { it.jsonObject["name"]?.jsonPrimitive?.content == "like" }.jsonObject
            val action = JsonObject(like + mapOf("refetch" to JsonArray(listOf(JsonPrimitive("visual_media"))), "optimistic" to JsonObject(emptyMap())))
            val media = Json.parseToJsonElement("""{"marker":"MediaMarker","from":"node.mediaLibrary","fields":["revision","visualCount","audioCount","giftCount"],"sequence":"latestSequence","tail":"MediaTail","after":"afterSequence","marker_variables":{"chatId":{"from":"param","name":"chatId"}},"tail_variables":{"chatId":{"from":"param","name":"chatId"}}}""")
            val updated = JsonArray(original.map { item ->
                val model = item.jsonObject
                if (model["key"]?.jsonPrimitive?.content != "MediaMessage") item else {
                    val relations = JsonArray(model.getValue("relations").jsonArray.map { entry ->
                        val relation = entry.jsonObject
                        if (relation["name"]?.jsonPrimitive?.content == "visual_media") JsonObject(relation + ("media" to media)) else entry
                    })
                    JsonObject(model + mapOf("relations" to relations, "actions" to JsonArray(listOf(action))))
                }
            }).toString()
            val transport = MockTransport()
            fun marker(count: Int) = """{"data":{"node":{"mediaLibrary":{"revision":1,"visualCount":$count,"audioCount":0,"giftCount":0,"latestSequence":1}}}}"""
            fun whole(id: String) = """{"data":{"node":{"messages":{"edges":[{"cursor":"$id","node":{"__typename":"MediaMessage","id":"$id","chatId":"c1","kind":"TEXT","mediaKind":"IMAGE","sequenceNumber":1}}],"pageInfo":{"hasNextPage":false,"endCursor":"$id"}}}}}"""
            listOf(0, 0, 1, 1, 1).forEach { transport.enqueue("MediaMarker", marker(it)) }
            transport.enqueue("ChatMedia", whole("v1"))
            transport.enqueue("ChatMedia", whole("v2"))
            transport.enqueue("LikeMoment", """{"data":{"likeMoment":{"__typename":"MediaMessage","id":"v1","chatId":"c1","kind":"TEXT","mediaKind":"IMAGE","sequenceNumber":1}}}""")
            val definitions = """[{"name":"MediaMarker","document":"query MediaMarker(${'$'}chatId: ID!, ${'$'}upToSequence: Int!) { node(id: ${'$'}chatId) { mediaLibrary(upToSequence: ${'$'}upToSequence) { revision visualCount audioCount giftCount latestSequence } } }"},{"name":"MediaTail","document":"query MediaTail(${'$'}chatId: ID!, ${'$'}afterSequence: Int!, ${'$'}first: Int, ${'$'}after: String) { node(id: ${'$'}chatId) { messages(afterSequence: ${'$'}afterSequence, first: ${'$'}first, after: ${'$'}after) { edges { cursor node { id chatId kind mediaKind sequenceNumber } } pageInfo { hasNextPage endCursor } } } }"},{"name":"LikeMoment","document":"mutation LikeMoment(${'$'}momentId: ID!) { likeMoment(momentId: ${'$'}momentId) { id chatId kind mediaKind sequenceNumber } }"}]"""
            DBlayer.openTesting(transport, updated, operations = listOf(ChatMedia))
            DBlayer.database.registerOperations(definitions)
            val query = MediaMessage.visualMedia("c1")
            awaitWithin("the initial media load") { query.loadAll() }
            val row = query.read().single()
            val receipt = row.act("like", JsonObject(emptyMap()), JsonObject(emptyMap()), MediaMessage)
            assertTrue(awaitWithin("the mutation outcome") { receipt.outcome() } is OperationOutcome.Confirmed)
            awaitWithin("the stale media load") { query.loadAll() }
            assertEquals(listOf("v2"), query.read().map { it.id })
            assertEquals(2, transport.calls.count { it.operationName == "ChatMedia" })
            awaitWithin("the fresh media load") { query.loadAll() }
            assertEquals(5, transport.calls.count { it.operationName == "MediaMarker" })
        } finally {
            DBlayer.close()
        }
    }

    private object PresenceOnline : GraphQLOperation {
        override val operationName = "PresenceOnline"
        override val document = "query PresenceOnline { presenceOnline { id userId room } }"
    }

    private object ChatMedia : GraphQLOperation {
        override val operationName = "ChatMedia"
        override val document = "query ChatMedia(\$chatId: ID!, \$first: Int, \$after: Cursor, \$mediaKinds: [String!], \$includeGifts: Boolean!) { node(id: \$chatId) { ... on Chat { messages(first: \$first, after: \$after, mediaKinds: \$mediaKinds, includeGifts: \$includeGifts) { edges { cursor node { id chatId kind mediaKind sequenceNumber } } pageInfo { hasNextPage endCursor } } } } }"
    }

    private fun page(id: String, kind: String, mediaKind: String) =
        """{"data":{"node":{"messages":{"edges":[{"cursor":"$id","node":{"__typename":"MediaMessage","id":"$id","chatId":"c1","kind":"$kind","mediaKind":$mediaKind,"sequenceNumber":1}}],"pageInfo":{"hasNextPage":true,"endCursor":"$id"}}}}}"""

    @Test
    fun preloadsAndThreeMediaLoadsFinishOnEmptyPages() = bounded {
        DBlayer.close()
        val transport = MockTransport()
        val preload = transport.hold("PresenceOnline", """{"data":{"presenceOnline":[]}}""")
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            DBlayer.openTesting(transport, modelsJSON, operations = listOf(PresenceOnline, ChatMedia))
            awaitWithin("the preload request") { preload.entered() }
            preload.release()
            val empty = """{"data":{"node":{"messages":{"edges":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}"""
            val cases = listOf(
                Triple(MediaMessage.visualMedia("c1"), "v1", page("v1", "TEXT", "\"IMAGE\"")),
                Triple(MediaMessage.audioMedia("c1"), "a1", page("a1", "TEXT", "\"AUDIO\"")),
                Triple(MediaMessage.giftMedia("c1"), "g1", page("g1", "GIFT", "null")),
            )
            for ((query, id, first) in cases) {
                transport.enqueue("ChatMedia", first)
                transport.enqueue("ChatMedia", empty)
                query.loadAll()
                assertEquals(listOf(id), query.read().map { it.id })
                val held = Projected(AccountScope.current().stream(query), MediaMessage, scope)
                held.loaded()
                assertTrue(held.projection.value.state?.complete == true)
                held.close()
            }
            val calls = transport.calls.filter { it.operationName == "ChatMedia" }
            assertEquals(6, calls.size)
            for ((index, id) in listOf("v1", "a1", "g1").withIndex()) {
                val first = Json.parseToJsonElement(calls[index * 2].variablesJson).jsonObject
                val second = Json.parseToJsonElement(calls[index * 2 + 1].variablesJson).jsonObject
                assertEquals(null, first["after"])
                assertEquals(id, second["after"]?.jsonPrimitive?.content)
            }
            assertEquals(1, transport.calls.count { it.operationName == "PresenceOnline" })
            transport.enqueue("ChatMedia", """{"data":{"node":{"messages":{"edges":[{"cursor":"x1","node":{"__typename":"MediaMessage","id":"x1","chatId":"c2","kind":"GIFT","mediaKind":null,"sequenceNumber":1}}],"pageInfo":{"hasNextPage":true,"endCursor":"x1"}}}}}""")
            val failedQuery = MediaMessage.giftMedia("c2")
            val held = Projected(AccountScope.current().stream(failedQuery), MediaMessage, scope)
            held.loaded()
            val down = transport.hold("ChatMedia", failure = DbException.Network("down"))
            val failing = async { runCatching { failedQuery.loadAll() }.exceptionOrNull() }
            networkDrops(during = down)
            assertTrue(failing.await() is DbException.Network)
            val failed = held.projection.first { it.error != null }
            assertEquals(listOf("x1"), failed.rows.map { it.id })
            assertEquals(LivePhase.LOADED, failed.phase)
            assertEquals(false, failed.state?.complete)
            assertTrue(failed.error is DbException.Network)
            held.close()
        } finally {
            preload.release()
            scope.cancel()
            DBlayer.close()
        }
    }
}

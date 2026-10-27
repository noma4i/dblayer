package com.noma4i.dblayer

import com.noma4i.dblayer.models.MediaEntry
import java.io.File
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertEquals
import org.junit.Test

class MediaMarkerTest {
    @Test
    fun mediaMarkerPlatforms() = bounded {
        val directory = File(System.getProperty("user.dir"))
        val root = if (directory.name == "kotlin") directory.parentFile else directory
        val document = File(root, "fixtures/operations/queries/media.graphql").readText()
        val transport = MockTransport()
        reopen(Config(null, null, 64u), File(root, "fixtures/media-marker.json").readText(), transport)
        val definitions = listOf("chatMedia", "chatMediaState").map { name -> buildJsonObject {
            put("name", JsonPrimitive(name))
            put("document", JsonPrimitive(document))
        } }
        DBlayer.database.registerOperations(JsonArray(definitions).toString())
        fun marker(count: Int, sequence: Int) {
            transport.enqueue("chatMediaState", """{"data":{"node":{"mediaLibrary":{"revision":"1","visualCount":$count,"audioCount":0,"giftCount":0,"latestSequence":$sequence}}}}""")
        }
        fun page(ids: List<Int>, hasNext: Boolean) {
            val nodes = ids.joinToString(",") { """{"id":"$it","text":"media","sequenceNumber":$it}""" }
            transport.enqueue("chatMedia", """{"data":{"node":{"messages":{"nodes":[$nodes],"pageInfo":{"hasNextPage":$hasNext,"endCursor":"${ids.last()}"}}}}}""")
        }
        marker(0, 2)
        page(listOf(1), true)
        page(listOf(2), false)
        marker(0, 2)
        marker(2, 2)
        val scope = CoroutineScope(Dispatchers.Default + Job())
        try {
            val media = LiveList(MediaEntry.media("c1"), scope)
            media.waitForInitialLoad()
            eventually("media initial snapshot") { media.snapshot.value.complete }
            assertEquals(listOf("1", "2"), media.rows.value.map { it.id })
            marker(2, 5)
            page(listOf(3, 4), true)
            page(listOf(5), false)
            marker(2, 5)
            marker(5, 5)
            media.refresh()
            eventually("media tail snapshot") { media.snapshot.value.complete && media.rows.value.size == 5 }
            assertEquals(listOf("1", "2", "3", "4", "5"), media.rows.value.map { it.id })
            assertEquals(listOf(
                """{"chatId":"c1","first":2}""",
                """{"chatId":"c1","first":2,"after":"1"}""",
                """{"chatId":"c1","first":2,"afterSequence":2}""",
                """{"chatId":"c1","first":2,"afterSequence":2,"after":"4"}""",
            ).map { Json.parseToJsonElement(it) }, transport.calls.filter { it.operationName == "chatMedia" }.map { Json.parseToJsonElement(it.variablesJson) })
            assertEquals(null, media.snapshot.value.error)
        } finally { scope.coroutineContext[Job]?.cancel() }
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class WindowPagesParityTest {
    @Test
    fun bridge_window_two_pages_parity_c9() = bounded {
        val transport = MockTransport()
        val document = "query ContainerThread(${'$'}containerId: ID!, ${'$'}first: Int, ${'$'}after: String) { container(id: ${'$'}containerId) { records(first: ${'$'}first, after: ${'$'}after) { nodes { id containerId authorId text createdAt sequenceNumber } pageInfo { hasNextPage endCursor } } } }"
        fun page(range: IntRange, more: Boolean): String {
            val rows = range.joinToString(",") { n -> """{"__typename":"Record","id":"m$n","containerId":"c1","authorId":"u1","text":"message $n","createdAt":"2026-10-27T00:00:00Z","sequenceNumber":$n}""" }
            return """{"data":{"container":{"records":{"nodes":[$rows],"pageInfo":{"hasNextPage":$more,"endCursor":"${range.first}"}}}}}"""
        }
        transport.respondWith("ContainerThread") { request ->
            when (Json.parseToJsonElement(request.variablesJson).jsonObject["after"]?.jsonPrimitive?.content) {
                null, "null" -> page(41..65, true)
                "41" -> page(16..40, false)
                else -> null
            }
        }
        openSigned(Config(null), modelsJSON, transport)
        val definition = kotlinx.serialization.json.JsonArray(listOf(kotlinx.serialization.json.buildJsonObject {
            put("name", kotlinx.serialization.json.JsonPrimitive("ContainerThread"))
            put("document", kotlinx.serialization.json.JsonPrimitive(document))
        })).toString()
        registerStandOperations(DBlayer.database, definition)
        val scope = CoroutineScope(SupervisorJob())
        try {
            val projected = Projected(AccountScope.current().stream(Record.thread("c1")), Record, scope)
            projected.until("first page") { it.rows.size == 25 && it.state?.settled == true }
            assertTrue(projected.projection.value.state!!.hasMore)
            assertEquals(1, transport.calls.count { it.operationName == "ContainerThread" })

            projected.loadMore()
            projected.until("second page") { it.rows.size == 50 && it.state?.settled == true }
            assertEquals(((41..65) + (16..40)).map { "m$it" }, projected.projection.value.rows.map { it.id })
            assertFalse(projected.projection.value.state!!.hasMore)
            assertEquals(2, transport.calls.count { it.operationName == "ContainerThread" })
        } finally {
            scope.cancel()
            closeForCleanup()
        }
    }
}

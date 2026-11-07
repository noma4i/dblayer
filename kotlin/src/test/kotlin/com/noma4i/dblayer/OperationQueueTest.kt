package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test

private const val QUEUE_FEED = """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","text":"m","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"""
private const val QUEUE_UPDATED = """{"data":{"updateMoment":{"__typename":"Moment","id":"1","uuid":"a","text":"edited","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"""
private val QUEUE_DEFS = """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid text likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"UpdateMoment","document":"mutation UpdateMoment(${'$'}id: ID!, ${'$'}text: String) { updateMoment(id: ${'$'}id, text: ${'$'}text) { id uuid text likedByMe media createdAt } }"},{"name":"DeleteMoment","document":"mutation DeleteMoment(${'$'}id: ID!) { deleteMoment(id: ${'$'}id) { id } }"}]"""

private fun momentsKeepingUpdates(): String = JsonArray(Json.parseToJsonElement(modelsJSON).jsonArray.map { model ->
    val fields = model.jsonObject
    if (fields["key"] != JsonPrimitive("Moment")) model else {
        val actions = fields.getValue("actions").jsonArray.map { action ->
            val properties = action.jsonObject
            if (properties["name"] == JsonPrimitive("update")) JsonObject(properties + ("on_failure" to JsonPrimitive("keep"))) else action
        }
        JsonObject(fields + ("actions" to JsonArray(actions)))
    }
}).toString()

/** The queue of operations through its public API: `operations()` shows each write with its status, and `retry` takes the fields of an `OperationInfo`. */
class OperationQueueTest {
    @Test
    fun anOfflineUpdateShowsInTheQueueAndLeavesItWhenTheNetworkReturns() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", QUEUE_FEED)
        val hold = transport.hold("UpdateMoment", QUEUE_UPDATED)
        reopen(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations(QUEUE_DEFS)
        Moment.feed.load()
        val moment = withTimeout(5000) { Moment.recent.live.flow.first { it.isNotEmpty() } }.first()
        val queue = AccountScope.current().operations()
        DBlayer.database.setOnline(false)
        val receipt = moment.update(text = GraphQLNullable.Value("edited"))
        val pending = withTimeout(5000) { queue.first { it.size == 1 && it[0].status == OperationQueueStatus.PENDING } }
        assertEquals(listOf("Moment", "update", moment.id), listOf(pending[0].model, pending[0].action, pending[0].rowId))
        DBlayer.database.setOnline(true)
        withTimeout(5000) { queue.first { it.firstOrNull()?.status == OperationQueueStatus.SENDING } }
        hold.release()
        assertTrue(withTimeout(5000) { receipt.outcome() } is OperationOutcome.Confirmed)
        assertEquals(emptyList<OperationInfo>(), withTimeout(5000) { queue.first { it.isEmpty() } })
    }

    @Test
    fun aRefusedUpdateStaysFailedWithItsErrorAndRetryByItsFieldsSendsItAgain() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", QUEUE_FEED)
        transport.enqueue("UpdateMoment", DbException.Http(422.toUShort(), "refused"))
        transport.enqueue("UpdateMoment", QUEUE_UPDATED)
        reopen(Config(null, null), momentsKeepingUpdates(), transport)
        DBlayer.database.registerOperations(QUEUE_DEFS)
        Moment.feed.load()
        val moment = withTimeout(5000) { Moment.recent.live.flow.first { it.isNotEmpty() } }.first()
        val queue = AccountScope.current().operations()
        moment.update(text = GraphQLNullable.Value("edited"))
        val failed = withTimeout(5000) { queue.first { it.firstOrNull()?.status == OperationQueueStatus.FAILED } }.first()
        assertNotNull(failed.error)
        AccountScope.current().retry(failed.model, failed.rowId, failed.action)
        withTimeout(5000) { queue.first { it.isEmpty() } }
        assertEquals(2, transport.calls.count { it.operationName == "UpdateMoment" })
    }
}

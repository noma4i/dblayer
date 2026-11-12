package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.toList
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

private fun momentsKeepingUpdates(): String = JsonArray(Json.parseToJsonElement(modelsArrayJSON).jsonArray.map { model ->
    val fields = model.jsonObject
    if (fields["key"] != JsonPrimitive("Moment")) model else {
        val actions = fields.getValue("actions").jsonArray.map { action ->
            val properties = action.jsonObject
            if (properties["name"] == JsonPrimitive("update")) JsonObject(properties + ("on_failure" to JsonPrimitive("keep"))) else action
        }
        JsonObject(fields + ("actions" to JsonArray(actions)))
    }
}).toString()

/** The one reader a queue stream has: it takes every value in order, `until` waits for the first value that fits. */
private class QueueReader(scope: kotlinx.coroutines.CoroutineScope, queue: kotlinx.coroutines.flow.Flow<List<OperationInfo>>) {
    private val values = kotlinx.coroutines.channels.Channel<List<OperationInfo>>(kotlinx.coroutines.channels.Channel.UNLIMITED)
    private val reading = scope.launch { queue.collect { values.send(it) } }

    suspend fun until(fits: (List<OperationInfo>) -> Boolean): List<OperationInfo> {
        while (true) values.receive().let { if (fits(it)) return it }
    }

    fun close() = reading.cancel()
}

/** The queue of operations through its public API: `operations()` shows each write with its status, and `retry` takes the fields of an `OperationInfo`. */
class OperationQueueTest {
    @Test
    fun bridge_discard_same_action_parity_c9() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", QUEUE_FEED)
        transport.enqueue("UpdateMoment", """{"errors":[{"message":"refused","extensions":{"code":"FORBIDDEN"}}]}""")
        val held = transport.hold("UpdateMoment", QUEUE_UPDATED.replace("edited", "second"))
        try {
            openSigned(Config(null, null), specs(momentsKeepingUpdates()), transport)
            registerStandOperations(DBlayer.database, QUEUE_DEFS)
            Moment.feed.load()
            val moment = awaitWithin("the loaded moment") { Moment.recent.live.flow.first { it.isNotEmpty() } }.first()
            val scope = AccountScope.current()
            val queue = QueueReader(this, scope.operations().mapNotNull { (it as? OperationQueueItem.Queue)?.items })
            val first = moment.update(text = GraphQLNullable.Value("first"))
            val rejected = first.rejection()
            assertTrue(rejected.kept)
            DBlayer.database.setOnline(false)
            val second = moment.update(text = GraphQLNullable.Value("second"))
            awaitWithin("both updates are present") { queue.until { it.size == 2 && it.any { op -> op.status == OperationQueueStatus.FAILED } } }
            moment.discard("update", scope)
            val remaining = awaitWithin("the failed update is gone") { queue.until { it.size == 1 && it[0].status == OperationQueueStatus.PENDING } }
            assertEquals("update", remaining.single().action)
            assertEquals("second", Moment.find(moment.id).current()?.text)
            DBlayer.database.setOnline(true)
            awaitWithin("the second update reaches the transport") { held.entered() }
            held.release()
            assertEquals("second", awaitWithin("the second update is confirmed") { second.confirmed() }.value?.text)
            assertEquals("second", Moment.find(moment.id).current()?.text)
            assertEquals(emptyList<OperationInfo>(), awaitWithin("the queue is empty") { queue.until { it.isEmpty() } })
            queue.close()
        } finally { held.release(); closeForCleanup() }
    }

    @Test
    fun theStatusAndQueueStreamsEndWithTheCauseOfTheCore() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", QUEUE_FEED)
        openSigned(Config(null, null), modelsJSON, transport)
        registerStandOperations(DBlayer.database, QUEUE_DEFS)
        Moment.feed.load()
        val moment = withTimeout(5000) { Moment.recent.live.flow.first { it.isNotEmpty() } }.first()
        val scope = AccountScope.current()
        val queueReady = CompletableDeferred<Unit>()
        val statusReady = CompletableDeferred<Unit>()
        val queue = async { scope.operations().onEach { queueReady.complete(Unit) }.toList() }
        val status = async { scope.uploadStatus(moment, "upload_clip").uploadStatus.onEach { statusReady.complete(Unit) }.toList() }
        withTimeout(5000) { queueReady.await() }
        withTimeout(5000) { statusReady.await() }

        closeForCleanup()

        assertEquals(EndCause.Closed, (withTimeout(5000) { queue.await() }.last() as OperationQueueItem.Ended).cause)
        assertEquals(EndCause.Closed, (withTimeout(5000) { status.await() }.last() as TransferItem.Ended).cause)
    }
}

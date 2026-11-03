package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

private class HeldTestingTransport(val mock: MockTransport = MockTransport()) : Transport by mock {
    val entered = CompletableDeferred<Unit>()
    val release = CompletableDeferred<Unit>()
    override suspend fun query(request: TransportRequest): String {
        entered.complete(Unit)
        release.await()
        return mock.query(request)
    }
}

private class SwitchableTestTransport(@Volatile var target: MockTransport?) : Transport {
    private fun selected(): MockTransport = target ?: throw DbException.Interrupted("transport unavailable during reset")
    override suspend fun openCable(lease: CableLease) = selected().openCable(lease)
    override suspend fun cableSend(token: String, text: String) = selected().cableSend(token, text)
    override suspend fun cableNext(token: String): EncryptedSocketEvent? = target?.cableNext(token)
    override suspend fun cableClose(token: String) { target?.cableClose(token) }
    override suspend fun query(request: TransportRequest): String = selected().query(request)
    override suspend fun mutate(request: TransportRequest): String = selected().mutate(request)
    override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink) = selected().subscribe(request, sink)
    override fun unsubscribe(token: String) { target?.unsubscribe(token) }
    override suspend fun put(url: String, headers: Map<String, String>, file: PutFile, tag: String, progress: ByteProgress): PutReply =
        selected().put(url, headers, file, tag, progress)
}

@OptIn(DBlayerTestingApi::class)
class TestingCallsTest {
    private object Feed : GraphQLOperation {
        override val operationName = "MomentsFeed"
        override val document = "query MomentsFeed { feed { nodes { id uuid createdAt } pageInfo { hasNextPage endCursor } } }"
    }
    private object Create : GraphQLOperation {
        override val operationName = "CreateMoment"
        override val document = "mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid createdAt } }"
    }

    @Test
    fun testingResetGivesTheNextTransportAFreshOpen() = bounded {
        DBlayer.close()
        val scope = CoroutineScope(SupervisorJob())
        try {
            val reply = """{"data":{"feed":{"nodes":[{"id":"m1","uuid":"first","createdAt":"2026-10-27T00:00:00Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"""
            val first = MockTransport()
            first.enqueue("MomentsFeed", reply)
            val switched = SwitchableTestTransport(first)
            DBlayer.openTesting(switched, modelsJSON, operations = listOf(Feed))
            val held = LiveList(Moment.feed, scope)
            held.waitForInitialLoad()
            DBlayer.database.setOnline(false)
            switched.target = null
            DBlayer.resetForTesting()
            DBlayer.settleForTesting()
            DBlayer.database.setOnline(true)
            val next = MockTransport()
            repeat(3) { next.enqueue("MomentsFeed", reply) }
            switched.target = next
            assertTrue(next.calls.isEmpty())
            val fresh = LiveList(Moment.feed, scope)
            fresh.waitForInitialLoad()
            assertEquals(listOf("m1"), fresh.ids.value)
            assertTrue(next.calls.isNotEmpty() && next.calls.all { it.operationName == "MomentsFeed" })
            val entity = Moment.entity("m1")
            entity.waitForInitialLoad()
            assertNotNull(entity.value.value)
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }

    @Test
    fun testingResetClearsRows() = bounded {
        DBlayer.close()
        try {
            val transport = MockTransport()
            val reply = """{"data":{"feed":{"nodes":[{"id":"m1","uuid":"first","createdAt":"2026-10-27T00:00:00Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"""
            transport.enqueue("MomentsFeed", reply)
            transport.enqueue("MomentsFeed", reply)
            transport.enqueue("CreateMoment", """{"data":{"createMoment":{"id":"created","uuid":"u","createdAt":"2026-10-27T00:00:00Z"}}}""")
            DBlayer.openTesting(transport, modelsJSON, operations = listOf(Feed, Create))
            Moment.feed.load()
            DBlayer.database.setOnline(false)
            val row = Moment.create(text = "before")
            val entity = Moment.entity(row.id)
            entity.waitForInitialLoad()
            assertEquals(2, Moment.recent.live.current().size)
            DBlayer.resetForTesting()
            DBlayer.settleForTesting()
            assertTrue(Moment.recent.live.current().isEmpty())
            assertNull(entity.value.value)
            DBlayer.database.setOnline(true)
            DBlayer.settleForTesting()
            assertEquals(listOf("MomentsFeed"), transport.calls.map { it.operationName })
            Moment.feed.load()
            assertEquals(listOf("MomentsFeed", "MomentsFeed"), transport.calls.map { it.operationName })
        } finally {
            DBlayer.close()
        }
    }

    @Test
    fun testingSettleWaitsForTheDeliveredLoad() = bounded {
        DBlayer.close()
        val scope = CoroutineScope(SupervisorJob())
        try {
            val transport = HeldTestingTransport()
            transport.mock.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"id":"m1","uuid":"first","createdAt":"2026-10-27T00:00:00Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
            DBlayer.openTesting(transport, modelsJSON, operations = listOf(Feed))
            val list = LiveList(Moment.feed, scope)
            awaitWithin("the load starts") { transport.entered.await() }
            transport.release.complete(Unit)
            DBlayer.settleForTesting()
            assertEquals(listOf("m1"), list.ids.value)
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }
}

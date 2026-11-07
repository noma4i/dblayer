package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

private const val FEED_PAGE = """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false}}}}"""
private const val FEED_DEFS = """[{"name":"MomentsFeed","document":"query MomentsFeed { feed { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]"""

class ReconnectTest {
    private fun count(transport: MockTransport, operation: String) = transport.calls.count { it.operationName == operation }

    /** K09: a list stream opened with `refreshOnConnect` loads again once each time the network returns, and the screen has no refresh of its own to write. */
    @Test
    fun aListThatRefreshesOnConnectLoadsAgainOnceWhenTheNetworkReturns() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", FEED_PAGE)
        transport.enqueue("MomentsFeed", FEED_PAGE)
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations(FEED_DEFS)
        val screen = CoroutineScope(SupervisorJob())
        try {
            val list = Projected(AccountScope.current().stream(Moment.feed, refreshOnConnect = true), Moment, screen)
            list.loaded()
            assertEquals(1, count(transport, "MomentsFeed"))
            DBlayer.database.setOnline(false)
            DBlayer.database.setOnline(true)
            eventually("the list loaded again") { count(transport, "MomentsFeed") == 2 }
            list.loaded()
            assertEquals("the list loaded more or less than once on reconnect", 2, count(transport, "MomentsFeed"))
        } finally {
            screen.cancel()
        }
    }

    /** K11: while the network is away `refresh` and `loadMore` fail at once with the network error, and the stream shows the error. Nothing reaches the transport. */
    @Test
    fun aRefreshAndALoadMoreFailAtOnceOfflineAndTheStreamShowsTheError() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", FEED_PAGE)
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations(FEED_DEFS)
        val screen = CoroutineScope(SupervisorJob())
        try {
            val list = Projected(AccountScope.current().stream(Moment.feed), Moment, screen)
            list.loaded()
            eventually("the events subscription of the stream opened") { count(transport, "MomentEvents") == 1 }
            val calls = transport.calls.size
            DBlayer.database.setOnline(false)
            try { list.refresh(); fail("a refresh offline did not fail") } catch (error: DbException.Network) {}
            try { list.loadMore(); fail("a load more offline did not fail") } catch (error: DbException.Network) {}
            eventually("the stream shows the network error") { list.projection.value.error is DbException.Network }
            assertEquals("a call offline reached the transport: ${transport.calls.map { "${it.operationName} ${it.variablesJson}" }}", calls, transport.calls.size)
        } finally {
            screen.cancel()
        }
    }

    /** K10: a durable send whose first attempt failed on the network goes out at once when the network returns. No clock step is taken. */
    @Test
    fun aDurableSendThatFailedOnTheNetworkGoesOutAtOnceWhenTheNetworkReturns() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", DbException.Network("down"))
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations(FEED_DEFS)
        val sent = AccountScope.current().send(Moment, buildJsonObject { put("text", JsonPrimitive("hello")) })
        eventually("the first attempt failed on the network") { count(transport, "CreateMoment") == 1 }
        DBlayer.database.setOnline(false)
        DBlayer.database.setOnline(true)
        assertEquals("m77", sent.confirmed().id)
        assertEquals(2, count(transport, "CreateMoment"))
    }
}

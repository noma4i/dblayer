package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test

@OptIn(DBlayerTestingApi::class)
class TestClockTest {
    private fun loadAfter(advanceMillis: Long): Int = runBlocking {
        val transport = MockTransport()
        repeat(2) { transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""") }
        val clock = DBlayer.openTesting(transport, modelsJSON)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
        Moment.feed.load()
        clock.advance(advanceMillis)
        Moment.feed.load()
        transport.calls.size
    }

    @Test
    fun aTestClockMovesFreshForWithoutSleeping() {
        assertEquals(2, loadAfter(5 * 60_000L + 1_000))
        assertEquals(1, loadAfter(5 * 60_000L - 1_000))
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

class RelatedLiveListTest {
    @Test
    fun aLiveListOverARelatedQueryLoadsItsSourceOnceWhenStaleAndStaysLiveWhileRead() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
        val authors = Moment.feed.related(Moment.Relations.author)
        val scope = CoroutineScope(SupervisorJob())
        try {
            val first = LiveList(authors, scope)
            withTimeout(5000) { first.waitForInitialLoad() }
            withTimeout(5000) { first.snapshot.first { it.loadedAt != null } }
            assertEquals(1, transport.calls.count { it.operationName == "MomentsFeed" })

            val second = LiveList(authors, scope)
            withTimeout(5000) { second.waitForInitialLoad() }
            withTimeout(5000) { second.snapshot.first { it.loadedAt != null } }
            assertEquals(1, transport.calls.count { it.operationName == "MomentsFeed" })
        } finally {
            scope.cancel()
        }
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

private val REFETCH_DEFS = """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { edges { impressionScore impressionUuid rank node { id uuid likedByMe media createdAt } } pageInfo { hasNextPage endCursor } } }"},{"name":"LikeMoment","document":"mutation LikeMoment(${'$'}momentId: ID!) { likeMoment(momentId: ${'$'}momentId) { id uuid likedByMe media createdAt } }"}]"""
private const val FEED_REPLY = """{"data":{"feed":{"edges":[{"impressionUuid":"imp-1","impressionScore":5,"node":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}],"pageInfo":{"hasNextPage":false,"endCursor":"c"}}}}"""
private const val LIKE_REPLY = """{"data":{"likeMoment":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":true,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"""

class MutationRefetchTest {
    /** A call that waits for the answer of a mutation does not wait for a load of the list the mutation refetches that was already in flight. The list is read again once that load ends. */
    @Test
    fun aMutationSettlesWhileALoadOfItsRefetchedListIsHeld() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 64u), modelsJSON, transport)
        DBlayer.database.registerOperations(REFETCH_DEFS)
        transport.enqueue("MomentsFeed", FEED_REPLY)
        val scope = CoroutineScope(SupervisorJob())
        val list = ListReader(Moment.feed, scope)
        list.waitForInitialLoad()
        val moment = Query("Moment", Moment).readRow("1")!!
        val hold = transport.hold("MomentsFeed", FEED_REPLY)
        val refreshing = async { list.refresh() }
        withTimeout(5000) { hold.entered() }
        transport.enqueue("LikeMoment", LIKE_REPLY)
        transport.enqueue("MomentsFeed", FEED_REPLY)

        moment.like()
        val settled = runCatching { awaitWithin("the like to settle while the list load is held") { moment.settled("like") } }
        hold.release()
        refreshing.await()

        settled.getOrThrow()
        eventually("the list is read again after the held load") { transport.calls.count { it.operationName == "MomentsFeed" } == 3 }
        assertEquals(3, transport.calls.count { it.operationName == "MomentsFeed" })
        scope.cancel()
    }
}

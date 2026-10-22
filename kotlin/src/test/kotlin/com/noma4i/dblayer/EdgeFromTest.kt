package com.noma4i.dblayer

import com.noma4i.dblayer.models.FeedRank
import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Test

class EdgeFromTest {
    private val document = "query MomentsFeed(\$first: Int, \$after: String) { feed(first: \$first, after: \$after) { edges { impressionScore impressionUuid rank node { id uuid likedByMe media createdAt } } pageInfo { hasNextPage endCursor } } }"

    private val reply = """{"data":{"feed":{"edges":[{"impressionUuid":"imp-1","impressionScore":9,"rank":"top","node":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z","text":"in a vibe"}}],"pageInfo":{"hasNextPage":false,"endCursor":"c"}}}}"""

    @Test
    fun anEdgeValueDeclaredWithFromAndAnEdgeEnumAreReadAfterTheLoad() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 64u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"$document"}]""")
        transport.enqueue("MomentsFeed", reply)
        Moment.vibeFeed("v1").refresh()
        val edge = Moment.vibeFeedEdge("v1", "1")
        assertEquals(9L, edge?.score)
        assertEquals(FeedRank.Top, edge?.rank)
        assertEquals("imp-1", edge?.impressionUuid)
    }
}

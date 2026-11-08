package com.noma4i.dblayer

import com.noma4i.dblayer.models.FeedRank
import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.MomentFeedEdge
import com.noma4i.dblayer.models.MomentVibeFeedEdge
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class EdgeValueTest {
    private val document = "query MomentsFeed(\$first: Int, \$after: String) { feed(first: \$first, after: \$after) { edges { impressionScore impressionUuid rank node { id uuid likedByMe media createdAt } } pageInfo { hasNextPage endCursor } } }"

    private fun reply(uuid: String?, score: Int?, text: String? = null): String {
        val body = text?.let { ""","text":"$it"""" }.orEmpty()
        return """{"data":{"feed":{"edges":[{"impressionUuid":${uuid?.let { "\"$it\"" } ?: "null"},"impressionScore":${score ?: "null"},"node":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"$body}}],"pageInfo":{"hasNextPage":false,"endCursor":"c"}}}}"""
    }

    private val rankedReply = """{"data":{"feed":{"edges":[{"impressionUuid":"imp-1","impressionScore":9,"rank":"top","node":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z","text":"in a vibe"}}],"pageInfo":{"hasNextPage":false,"endCursor":"c"}}}}"""

    @Test
    fun edgeCallsDecodeNullableFieldsAliasesAndEnums() = bounded {
        val transport = MockTransport()
        openSigned(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"$document"}]""")
        transport.enqueue("MomentsFeed", reply("imp-1", 5))
        Moment.feed.refresh()
        assertEquals(MomentFeedEdge("imp-1", 5L), Moment.feedEdge("1"))
        assertEquals("imp-1", Moment.feedEdge("1")?.impressionUuid)
        assertNull(Moment.feedEdge("unknown"))

        transport.enqueue("MomentsFeed", reply(null, null, "in a vibe"))
        Moment.vibeFeed("v1").refresh()
        assertEquals(MomentVibeFeedEdge(null), Moment.vibeFeedEdge("v1", "1"))

        transport.enqueue("MomentsFeed", rankedReply)
        Moment.vibeFeed("v1").refresh()
        val ranked = Moment.vibeFeedEdge("v1", "1")
        assertEquals(Triple(9L, FeedRank.Top, "imp-1"), Triple(ranked?.score, ranked?.rank, ranked?.impressionUuid))
    }
}

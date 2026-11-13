package com.noma4i.dblayer

import com.noma4i.dblayer.models.Container
import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.RecordReactionNode
import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class BridgeProbeTest {
    @Test
    fun updateAndDestroyReachTypedLiveRows() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","text":"m","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
        transport.enqueue("UpdateMoment", """{"data":{"updateMoment":{"__typename":"Moment","id":"1","uuid":"a","text":"edited","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        transport.enqueue("DeleteMoment", """{"data":{"deleteMoment":{"id":"1"}}}""")
        openSigned(Config(null), modelsJSON, transport)
        registerStandOperations(DBlayer.database, """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid text likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"UpdateMoment","document":"mutation UpdateMoment(${'$'}id: ID!, ${'$'}text: String) { updateMoment(id: ${'$'}id, text: ${'$'}text) { id uuid text likedByMe media createdAt } }"},{"name":"DeleteMoment","document":"mutation DeleteMoment(${'$'}id: ID!) { deleteMoment(id: ${'$'}id) { id } }"}]""")
        val scope = CoroutineScope(SupervisorJob())
        val deliveries = java.util.concurrent.CopyOnWriteArrayList<List<String?>>()
        scope.launch { Moment.recent.live.flow.collect { rows -> deliveries.add(rows.map { it.text }); TestPulse.pulse() } }
        try {
            eventually("the open list shows its first value") { deliveries.isNotEmpty() }
            Moment.feed.load()
            eventually("the open list shows the loaded row") { deliveries.lastOrNull() == listOf<String?>("m") }
            val moment = Moment.recent.current().first()
            moment.update(text = GraphQLNullable.Value("edited"))
            eventually("the same open list shows the edited row") { deliveries.lastOrNull() == listOf<String?>("edited") }
            moment.destroy()
            eventually("the same open list loses the row") { deliveries.lastOrNull() == emptyList<String?>() }
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun aFailedLoadThrowsTheTransportErrorAndMarksTheRelationFailed() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", DbException.Http(400u, "bad request"))
        transport.enqueue("MomentsFeed", DbException.Http(400u, "bad request"))
        openSigned(Config(null), modelsJSON, transport)
        registerStandOperations(DBlayer.database, """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")

        val thrown = runCatching { Moment.feed.refresh() }.exceptionOrNull()

        assertEquals(true, thrown is DbException)
        val failed = withTimeout(5000) { Moment.feed.snapshots.flow.first { it.phase == LoadPhase.Failed } }
        assertEquals(true, failed.error != null)
    }

    @Test
    fun feedLoadsFromTheTransportIntoLiveRowsAndReportsItsState() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":true,"endCursor":"c1"},"totalCount":9}}}""")
        openSigned(Config(null), modelsJSON, transport)
        registerStandOperations(DBlayer.database, """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")

        val listScope = CoroutineScope(SupervisorJob())
        val list = ListReader(Moment.feed, listScope)
        withTimeout(5000) { list.rows.first { it.isNotEmpty() } }
        val state = withTimeout(5000) { list.snapshot.first { it.loaded != null } }
        assertEquals("a", list.rows.value.first().uuid)
        assertEquals("MomentsFeed", transport.calls.first().operationName)
        assertEquals(true, state.hasMore)
        assertEquals(9L, state.total)
        assertEquals(LoadPhase.Loaded, state.phase)
        val entity = Projected(AccountScope.current().entity(Moment, "1"), Moment, listScope)
        withTimeout(5000) { entity.projection.first { it.rows.isNotEmpty() } }
        assertEquals("a", entity.value?.uuid)
        listScope.cancel()
    }

}

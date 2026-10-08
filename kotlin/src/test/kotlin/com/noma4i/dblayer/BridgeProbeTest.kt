package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

class BridgeProbeTest {
    @Test
    fun writesReachTypedLiveRows() = runBlocking {
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, null)
        val live = Moment.recent.live.flow
        assertEquals(emptyList<Moment>(), withTimeout(5000) { live.first() })
        val moment = Moment.create(text = "m")
        assertEquals("m", withTimeout(5000) { Moment.recent.live.flow.first { it.isNotEmpty() } }.first().text)
        moment.update(text = "edited")
        assertEquals("edited", withTimeout(5000) { Moment.recent.live.flow.first { it.firstOrNull()?.text == "edited" } }.first().text)
        moment.destroy()
        assertEquals(emptyList<Moment>(), withTimeout(5000) { Moment.recent.live.flow.first { it.isEmpty() } })
    }

    @Test
    fun feedLoadsFromTheTransportIntoLiveRows() = runBlocking {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")

        Moment.feed.refresh()

        assertEquals("a", withTimeout(5000) { Moment.feed.live.flow.first { it.isNotEmpty() } }.first().uuid)
        assertEquals("MomentsFeed", transport.calls.first().operationName)
        val state = withTimeout(5000) { Moment.feed.state.flow.first() }
        assertEquals(LoadPhase.Idle, state.phase)
        assertEquals(false, state.hasMore)
        assertEquals(true, state.loadedAt != null)
    }

    @Test
    fun aFailedLoadThrowsTheTransportErrorAndMarksTheRelationFailed() = runBlocking {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", DbException.Network("offline"))
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")

        val thrown = runCatching { Moment.feed.refresh() }.exceptionOrNull()

        assertEquals(true, thrown is DbException)
        val state = withTimeout(5000) { Moment.feed.state.flow.first { it.phase == LoadPhase.Failed } }
        assertEquals(true, state.errorKind != null)
    }

    @Test
    fun liveListLoadsTheRelationAndReportsItsState() = runBlocking {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":true,"endCursor":"c1"}}}}""")
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")

        val list = LiveList(Moment.feed, this)
        withTimeout(5000) { list.rows.first { it.isNotEmpty() } }
        val state = withTimeout(5000) { list.state.first { it.loadedAt != null } }
        assertEquals("a", list.rows.value.first().uuid)
        assertEquals(true, state.hasMore)
        assertEquals(LoadPhase.Idle, state.phase)
        val rows = LiveRows(Moment.Companion::ids, this)
        rows.track(list.rows.value.map { it.id })
        withTimeout(5000) { rows.byId.first { it.isNotEmpty() } }
        assertEquals("a", rows.byId.value["1"]?.uuid)
        rows.stop()
        list.stop()
    }

    @Test
    fun confirmedIdWaitsForTheServerIdOfALocalCreate() = runBlocking {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")

        val local = Moment.create(text = "hello", media = null)
        assertEquals(true, local.id.startsWith("temp"))
        withTimeout(5000) { local.settled() }
        assertEquals("m77", withTimeout(5000) { local.confirmedId() })
    }
}

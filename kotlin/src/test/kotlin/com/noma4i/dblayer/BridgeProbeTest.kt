package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

class BridgeProbeTest {
    @Test
    fun liveRowsFollowAnEditAndDropRowsWhenTheTrackedIdsChange() = runBlocking {
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, null)
        val first = Moment.create(text = "one")
        val second = Moment.create(text = "two")
        val rows = LiveRows(Moment.Companion::ids, this)
        rows.track(listOf(first.id))
        assertEquals("one", withTimeout(5000) { rows.byId.first { it[first.id] != null } }[first.id]?.text)
        first.update(text = "edited")
        assertEquals("edited", withTimeout(5000) { rows.byId.first { it[first.id]?.text == "edited" } }[first.id]?.text)
        rows.track(listOf(second.id))
        assertEquals(setOf(second.id), withTimeout(5000) { rows.byId.first { it.keys == setOf(second.id) } }.keys)
        rows.stop()
    }

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
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":true,"endCursor":"c1"},"totalCount":9}}}""")
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")

        val list = LiveList(Moment.feed, this)
        withTimeout(5000) { list.rows.first { it.isNotEmpty() } }
        val state = withTimeout(5000) { list.state.first { it.loadedAt != null } }
        assertEquals("a", list.rows.value.first().uuid)
        assertEquals(true, state.hasMore)
        assertEquals(9L, state.total)
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

    @Test
    fun aCreateWithAnUploadShowsTheRecordAtOnceAndTheServerIdReplacesIt() = runBlocking {
        val transport = MockTransport()
        transport.enqueue("CreateBlob", """{"data":{"createBlob":{"url":"https://s3/x","headers":{},"signedId":"sid","blobId":"b1"}}}""")
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateBlob","document":"mutation CreateBlob(${'$'}filename: String!, ${'$'}contentType: String!, ${'$'}byteSize: Int!, ${'$'}checksum: String!) { createBlob(filename: ${'$'}filename, contentType: ${'$'}contentType, byteSize: ${'$'}byteSize, checksum: ${'$'}checksum) { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")
        val file = java.io.File.createTempFile("clip", ".mp4").apply { writeText("abcdef") }

        val values = kotlinx.serialization.json.buildJsonObject { put("text", kotlinx.serialization.json.JsonPrimitive("hello")) }
        val request = UploadRequest(file.path, "CreateMoment", createBlob = "CreateBlob", variables = values)
        val local = uploadCreate("Moment", values, request, Moment.serializer())

        assertEquals(true, local.id.startsWith("temp"))
        assertEquals(true, local.upload != null)
        assertEquals("m77", withTimeout(5000) { local.confirmedId() })
        val sent = transport.calls.filter { it.operationName == "CreateMoment" }
        assertEquals(1, sent.size)
        assertEquals(true, sent.first().variablesJson.contains("\"signedId\":\"sid\""))
    }

    @Test
    fun resumeReachesTheCoreAndReloadsAWatchedRelationWithoutAFreshWindow() = runBlocking {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", DbException.Http(400u, "bad request"))
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
        val list = LiveList(Moment.feed, this)
        withTimeout(5000) { list.state.first { it.phase == LoadPhase.Failed } }
        val before = transport.calls.size

        DBlayer.resume()

        assertEquals(true, transport.calls.size > before)
        list.stop()
    }

    @Test
    fun aReloadAfterACommitReadsTheServerAfterTheLoadThatWasInFlight() = runBlocking {
        val transport = MockTransport()
        val empty = """{"data":{"feed":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"""
        transport.enqueue("MomentsFeed", empty)
        transport.enqueue("MomentsFeed", empty, 1000)
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")
        val list = LiveList(Moment.feed, this)
        withTimeout(5000) { list.state.first { it.phase == LoadPhase.Idle && it.loadedAt != null } }
        val older = launch { list.refresh() }
        withTimeout(5000) { while (transport.calls.count { it.operationName == "MomentsFeed" } < 2) delay(10) }

        val local = Moment.create(text = "hello", media = null)
        withTimeout(5000) { local.settled() }
        withTimeout(5000) { list.rows.first { rows -> rows.map { it.id } == listOf("m77") } }

        assertEquals(listOf("m77"), list.rows.value.map { it.id })
        assertEquals(3, transport.calls.count { it.operationName == "MomentsFeed" })
        older.join()
        list.stop()
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Chat
import com.noma4i.dblayer.models.Message
import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class BridgeProbeTest {
    @Test
    fun anEntityFollowsAnEditAndLetsGoOfTheRecordWhenItIsDropped() = runBlocking {
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, null)
        val first = Moment.create(text = "one")
        var entity: Entity<Moment>? = Moment.entity(first.id)
        assertEquals("one", withTimeout(5000) { entity!!.value.first { it != null } }?.text)
        first.update(text = "edited")
        assertEquals("edited", withTimeout(5000) { entity!!.value.first { it?.text == "edited" } }?.text)
        assertEquals(1uL, DBlayer.subscriptionCensus().subscriptions)
        entity = null
        withTimeout(10000) { while (EntityRegistries.of("Moment", Moment.serializer()).liveCount > 0) { System.gc(); delay(50) } }
        assertEquals(0, EntityRegistries.of("Moment", Moment.serializer()).liveCount)
    }

    @Test
    fun anEntityOfARecordThatAnotherRecordRefersToFollowsItsCreateAndDestroy() = runBlocking {
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, null)
        val message = Message.create(chatId = "c", authorId = "u1", text = "hi")
        val entity = Message.entity(message.id)
        assertEquals("hi", withTimeout(5000) { entity.value.first { it != null } }?.text)
        message.destroy()
        withTimeout(5000) { entity.value.first { it == null } }
        assertNull(entity.value.value)
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
    fun loadRequestsOnlyAWindowThatIsNotFreshAndKeepsNoSubscription() = runBlocking {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
        val before = DBlayer.subscriptionCensus()

        Moment.feed.load()
        Moment.feed.load()

        assertEquals(1, transport.calls.size)
        val after = DBlayer.subscriptionCensus()
        assertEquals(before.subscriptions to before.observedQueries, after.subscriptions to after.observedQueries)
    }

    @Test
    fun aFailedLoadThrowsLikeRefresh() = runBlocking {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", DbException.Network("offline"))
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")

        assertEquals(true, runCatching { Moment.feed.load() }.exceptionOrNull() is DbException)
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

        val listScope = CoroutineScope(SupervisorJob())
        val list = LiveList(Moment.feed, listScope)
        withTimeout(5000) { list.rows.first { it.isNotEmpty() } }
        val state = withTimeout(5000) { list.state.first { it.loadedAt != null } }
        assertEquals("a", list.rows.value.first().uuid)
        assertEquals(true, state.hasMore)
        assertEquals(9L, state.total)
        assertEquals(LoadPhase.Idle, state.phase)
        val entity = Moment.entity("1")
        withTimeout(5000) { entity.value.first { it != null } }
        assertEquals("a", entity.value.value?.uuid)
        listScope.cancel()
    }

    @Test
    fun aCreateWhoseMutationIsNotRegisteredFailsOnTheRecordAndSendsAfterRegisteringAndRetrying() = runBlocking {
        val transport = MockTransport()
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)

        val local = Moment.create(text = "hello", media = null)
        val entity = Moment.entity(local.id)
        val failed = withTimeout(5000) { entity.value.first { it?.sync?.state == SyncState.Failed } }!!
        assertEquals(true, failed.needsAttention)
        assertEquals("operation CreateMoment is not registered", failed.sync.error?.message)
        assertEquals(0, transport.calls.size)

        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")
        DBlayer.database.retry("Moment", local.id)
        assertEquals("m77", withTimeout(5000) { local.confirmedId() })
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
    fun aSnapshotCarriesTheTemporaryIdThatTheServerIdReplaced() = runBlocking {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")
        val seen = kotlinx.coroutines.flow.MutableSharedFlow<Snapshot<Moment>>(replay = 64)
        val collecting = launch { Moment.recent.snapshots.flow.collect { seen.emit(it) } }
        withTimeout(5000) { seen.first() }

        val local = Moment.create(text = "hello", media = null)
        val snapshot = withTimeout(5000) { seen.first { it.replaced.containsKey("m77") } }

        assertEquals(local.id, snapshot.replaced["m77"])
        collecting.cancel()
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
        assertEquals(false, sent.first().variablesJson.contains("blobId"))
    }

    @Test
    fun resumeReachesTheCoreAndReloadsAWatchedRelationWithoutAFreshWindow() = runBlocking {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", DbException.Http(400u, "bad request"))
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
        val listScope = CoroutineScope(SupervisorJob())
        val list = LiveList(Moment.feed, listScope)
        withTimeout(5000) { list.state.first { it.phase == LoadPhase.Failed } }
        val before = transport.calls.size

        DBlayer.resume()

        assertEquals(true, transport.calls.size > before)
        listScope.cancel()
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
        val listScope = CoroutineScope(SupervisorJob())
        val list = LiveList(Moment.feed, listScope)
        withTimeout(5000) { list.state.first { it.phase == LoadPhase.Idle && it.loadedAt != null } }
        val older = launch { list.refresh() }
        withTimeout(5000) { while (transport.calls.count { it.operationName == "MomentsFeed" } < 2) delay(10) }

        val local = Moment.create(text = "hello", media = null)
        withTimeout(5000) { local.settled() }
        withTimeout(5000) { list.rows.first { rows -> rows.map { it.id } == listOf("m77") } }

        assertEquals(listOf("m77"), list.rows.value.map { it.id })
        assertEquals(3, transport.calls.count { it.operationName == "MomentsFeed" })
        older.join()
        listScope.cancel()
    }
}

class EntityBridgeTest {
    private class Collector : EntityListener {
        val received = java.util.concurrent.CopyOnWriteArrayList<EntityDelivery>()
        override fun changed(delivery: EntityDelivery) { received.add(delivery) }
        suspend fun wait(count: Int): List<EntityDelivery> = withTimeout(5000) {
            while (received.size < count) delay(10)
            received.toList()
        }
    }

    @Test
    fun retainedRowsReachTheBridgeAsChangedRowsAndReleasedRowsStayQuiet() = runBlocking {
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, null)
        val first = Moment.create(text = "one")
        val second = Moment.create(text = "two")
        val collector = Collector()
        val entities = DBlayer.database.liveEntities(first.modelName, collector)
        entities.retain(listOf(first.id, second.id))
        assertEquals(2, collector.wait(1)[0].rows.size)

        first.update(text = "edited")
        val changed = collector.wait(2)
        assertEquals(1, changed[1].rows.size)
        assertEquals(true, changed[1].rows[0].contains("edited"))

        entities.release(listOf(first.id))
        first.update(text = "unseen")
        second.update(text = "seen")
        val last = collector.wait(3)
        assertEquals(3, last.size)
        assertEquals(true, last[2].rows[0].contains("seen"))

        second.destroy()
        assertEquals(listOf(second.id), collector.wait(4).last().removed)
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Container
import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.RecordReactionNode
import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class BridgeProbeTest {
    @Test
    fun aReactionUsesItsDeclaredKeyAcrossGeneratedValuesAndCells() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val initial = Record.create(containerId = "c1", authorId = "u1", text = "hello")
        val row = initial.update(reactions = listOf(RecordReactionNode(body = "A", userId = "u1")))
        val changed = row.react(reactionBody = "C", userId = "u1")
        assertEquals(listOf("u1"), changed.reactions.map { it.userId })
        assertEquals(listOf("C"), changed.reactions.map { it.body })
    }

    @Test
    fun anEntityFollowsAnEditAndLetsGoOfTheRecordWhenItIsDropped() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val first = Moment.create(text = "one")
        var entity: Entity<Moment>? = Moment.entity(first.id)
        assertEquals("one", withTimeout(5000) { entity!!.value.first { it != null } }?.text)
        first.update(text = GraphQLNullable.Value("edited"))
        assertEquals("edited", withTimeout(5000) { entity!!.value.first { it?.text == "edited" } }?.text)
        assertEquals(1uL, DBlayer.subscriptionCensus().subscriptions)
        entity = null
        eventually("the registry lets go of the record", 10_000) { System.gc(); EntityRegistries.of("Moment", Moment).liveCount == 0 }
        assertEquals(0, EntityRegistries.of("Moment", Moment).liveCount)
    }

    @Test
    fun aSearchPassesWholeWordsToTheCore() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        Moment.create(text = "hello world")
        Moment.create(text = "hel")
        val whole = Query("Moment", Moment).search("hel", wholeWords = true)
        assertEquals(listOf("hel"), withTimeout(5000) { whole.live.flow.first { it.isNotEmpty() } }.map { it.text })
    }

    @Test
    fun aWarmQueryAnswersFromMemoryAndALiveReadLeavesItWarm() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        assertNull(Moment.recent.cached())
        val moment = Moment.create(text = "m")
        Moment.recent.warm()
        val warm = Moment.recent.cached()!!
        assertEquals(listOf(moment.id), warm.value.map { it.id })
        assertEquals("m", warm.value.first().text)
        assertNull(Moment.recent.cachedRow("ghost"))
        reopen(Config(null, null, 32u), modelsJSON, null)
        assertNull(Moment.recent.cached())
        Moment.create(text = "n")
        assertEquals(1, withTimeout(5000) { Moment.recent.live.flow.first { it.isNotEmpty() } }.size)
        assertEquals("n", Moment.recent.cached()!!.value.first().text)
    }

    @Test
    fun writesReachTypedLiveRows() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val live = Moment.recent.live.flow
        assertEquals(emptyList<Moment>(), withTimeout(5000) { live.first() })
        val moment = Moment.create(text = "m")
        assertEquals("m", withTimeout(5000) { Moment.recent.live.flow.first { it.isNotEmpty() } }.first().text)
        moment.update(text = GraphQLNullable.Value("edited"))
        assertEquals("edited", withTimeout(5000) { Moment.recent.live.flow.first { it.firstOrNull()?.text == "edited" } }.first().text)
        moment.setLocal("tone", kotlinx.serialization.json.JsonPrimitive("warm"))
        assertEquals("warm", withTimeout(5000) { Moment.recent.live.flow.first { it.firstOrNull()?.tone == "warm" } }.first().tone)
        moment.setLocal("tone", kotlinx.serialization.json.JsonNull)
        assertNull(withTimeout(5000) { Moment.recent.live.flow.first { it.firstOrNull()?.tone == null } }.first().tone)
        moment.destroy()
        assertEquals(emptyList<Moment>(), withTimeout(5000) { Moment.recent.live.flow.first { it.isEmpty() } })
    }

    @Test
    fun loadRequestsOnlyAWindowThatIsNotFreshAndKeepsNoSubscription() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
        val before = DBlayer.subscriptionCensus()

        Moment.feed.load()
        Moment.feed.load()

        assertEquals(1, transport.calls.size)
        val after = DBlayer.subscriptionCensus()
        assertEquals(before.subscriptions to before.observedQueries, after.subscriptions to after.observedQueries)
    }

    @Test
    fun aFailedLoadThrowsTheTransportErrorAndMarksTheRelationFailed() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", DbException.Http(400u, "bad request"))
        transport.enqueue("MomentsFeed", DbException.Http(400u, "bad request"))
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")

        val thrown = runCatching { Moment.feed.refresh() }.exceptionOrNull()

        assertEquals(true, thrown is DbException)
        val failed = withTimeout(5000) { Moment.feed.snapshots.flow.first { it.phase == LoadPhase.Failed } }
        assertEquals(true, failed.errorKind != null)
    }

    @Test
    fun feedLoadsFromTheTransportIntoLiveRowsAndReportsItsState() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":true,"endCursor":"c1"},"totalCount":9}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")

        val listScope = CoroutineScope(SupervisorJob())
        val list = LiveList(Moment.feed, listScope)
        withTimeout(5000) { list.rows.first { it.isNotEmpty() } }
        val state = withTimeout(5000) { list.snapshot.first { it.loadedAt != null } }
        assertEquals("a", list.rows.value.first().uuid)
        assertEquals("MomentsFeed", transport.calls.first().operationName)
        assertEquals(true, state.hasMore)
        assertEquals(9L, state.total)
        assertEquals(LoadPhase.Loaded, state.phase)
        val entity = Moment.entity("1")
        withTimeout(5000) { entity.value.first { it != null } }
        assertEquals("a", entity.value.value?.uuid)
        listScope.cancel()
    }

    @Test
    fun eachNewListCanRefreshAndAwaitItsOwnInitialRequest() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed { feed { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
        repeat(3) { index ->
            if (index == 2) {
                transport.enqueue("MomentsFeed", DbException.Http(400u, "rejected"))
            } else {
                transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false}}}}""")
            }
            val listScope = CoroutineScope(SupervisorJob())
            try {
                val list = LiveList(Moment.feed, listScope, refreshOnConnect = true)
                val failure = runCatching { withTimeout(5000) { list.waitForInitialLoad() } }.exceptionOrNull()
                if (index == 2) assertEquals(true, failure is DbException.Http) else assertNull(failure)
                assertEquals(index + 1, transport.calls.count { it.operationName == "MomentsFeed" })
                assertEquals("a", withTimeout(5000) { list.rows.first { it.isNotEmpty() } }.first().uuid)
            } finally {
                listScope.cancel()
            }
        }
    }

    @Test
    fun aCreateWhoseMutationIsNotRegisteredFailsOnTheRecordAndNeedsAttention() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, MockTransport())

        val local = Moment.create(text = "hello", media = null)
        val entity = Moment.entity(local.id)
        val failed = withTimeout(5000) { entity.value.first { it?.sync?.state == SyncState.Failed } }!!

        assertEquals(true, failed.needsAttention)
    }

    @Test
    fun confirmedIdWaitsForTheServerIdOfALocalCreate() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")

        val local = Moment.create(text = "hello", media = null)
        assertEquals(true, local.id.startsWith("temp"))
        withTimeout(5000) { local.settled() }
        assertEquals("m77", withTimeout(5000) { local.confirmedId() })
    }

    @Test
    fun aSnapshotCarriesTheTemporaryIdThatTheServerIdReplaced() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
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
    fun aCreateWithAnUploadShowsTheRecordAtOnceAndTheServerIdReplacesIt() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateBlob", """{"data":{"createBlob":{"url":"https://s3/x","headers":{},"signedId":"sid","blobId":"b1"}}}""")
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateBlob","document":"mutation CreateBlob(${'$'}filename: String!, ${'$'}contentType: String!, ${'$'}byteSize: Int!, ${'$'}checksum: String!) { createBlob(filename: ${'$'}filename, contentType: ${'$'}contentType, byteSize: ${'$'}byteSize, checksum: ${'$'}checksum) { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")
        val file = java.io.File.createTempFile("clip", ".mp4").apply { writeText("abcdef") }

        val values = kotlinx.serialization.json.buildJsonObject { put("text", kotlinx.serialization.json.JsonPrimitive("hello")) }
        val request = UploadRequest(file.path, "CreateMoment", createBlob = "CreateBlob", variables = values)
        val local = uploadCreate("Moment", values, request, Moment)

        assertEquals(true, local.id.startsWith("temp"))
        assertEquals(true, local.upload != null)
        assertEquals("m77", withTimeout(5000) { local.confirmedId() })
        val sent = transport.calls.filter { it.operationName == "CreateMoment" }
        assertEquals(1, sent.size)
        assertEquals(true, sent.first().variablesJson.contains("\"signedId\":\"sid\""))
        assertEquals(false, sent.first().variablesJson.contains("blobId"))
    }

    @Test
    fun resumeReachesTheCoreAndReloadsAWatchedRelationWithoutAFreshWindow() = bounded {
        val transport = MockTransport()
        repeat(2) { transport.enqueue("MomentsFeed", DbException.Http(400u, "bad request")) }
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
        val listScope = CoroutineScope(SupervisorJob())
        val list = LiveList(Moment.feed, listScope)
        withTimeout(5000) { list.snapshot.first { it.phase == LoadPhase.Failed } }
        val before = transport.calls.size

        DBlayer.resume()

        assertEquals(true, transport.calls.size > before)
        listScope.cancel()
    }

}

class EntityBridgeTest {
    @Test
    fun droppingAnEntitySourceReleasesItsObserver() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val row = Moment.create(text = "held")
        val delivered = java.util.concurrent.atomic.AtomicInteger()
        var source: CoreEntitySource<Moment>? = CoreEntitySource("Moment", Moment) { delivered.incrementAndGet() }
        val released = java.lang.ref.WeakReference(source)
        source!!.retain(listOf(row.id))
        eventually("the entity source delivers its row") { delivered.get() > 0 }
        source = null
        eventually("the entity source releases its observer") { System.gc(); released.get() == null && DBlayer.subscriptionCensus().subscriptions == 0uL }
    }

    private class Collector : EntityListener {
        val received = java.util.concurrent.CopyOnWriteArrayList<EntityDelivery>()
        override fun changed(delivery: EntityDelivery) { received.add(delivery) }
        suspend fun wait(count: Int): List<EntityDelivery> {
            eventually("$count entity deliveries") { received.size >= count }
            return received.toList()
        }
    }

    @Test
    fun retainedRowsReachTheBridgeAsChangedRowsAndReleasedRowsStayQuiet() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val first = Moment.create(text = "one")
        val second = Moment.create(text = "two")
        val collector = Collector()
        DBlayer.database.liveEntities(first.modelName, collector).use { entities ->
            entities.retain(listOf(first.id, second.id))
            assertEquals(2, collector.wait(1)[0].rows.size)

            first.update(text = GraphQLNullable.Value("edited"))
            val changed = collector.wait(2)
            assertEquals(1, changed[1].rows.size)
            assertEquals(true, changed[1].rows[0].cells.contains(Cell.Text("edited")))

            entities.release(listOf(first.id))
            first.update(text = GraphQLNullable.Value("unseen"))
            System.gc()
            second.update(text = GraphQLNullable.Value("seen"))
            val last = collector.wait(3)
            assertEquals(3, last.size)
            assertEquals(true, last[2].rows[0].cells.contains(Cell.Text("seen")))

            second.destroy()
            assertEquals(listOf(second.id), collector.wait(4).last().removed)
        }
    }
}

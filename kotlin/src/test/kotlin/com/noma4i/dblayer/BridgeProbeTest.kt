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
        val initial = Record.send(AccountScope.current(), containerId = "c1", authorId = "u1", text = "hello").row
        val row = initial.update(reactions = listOf(RecordReactionNode(body = "A", userId = "u1")))
        val changed = row.react(reactionBody = "C", userId = "u1")
        assertEquals(listOf("u1"), changed.reactions.map { it.userId })
        assertEquals(listOf("C"), changed.reactions.map { it.body })
    }

    @Test
    fun anOptimisticReactionCarriesTheNestedUserOfTheLocalRow() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val ann = User.create(name = "Ann")
        val record = Record.send(AccountScope.current(), containerId = "c1", authorId = ann.id, text = "hello").row
        val withUser = record.react(reactionBody = "A", userId = ann.id)
        assertEquals("Ann", withUser.reactions.first().user?.name)
        val withoutUser = record.react(reactionBody = "B", userId = "nobody")
        assertNull(withoutUser.reactions.first { it.userId == "nobody" }.user)
    }

    @Test
    fun aSearchPassesWholeWordsToTheCore() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        Moment.send(AccountScope.current(), text = "hello world").row
        Moment.send(AccountScope.current(), text = "hel").row
        val whole = Query("Moment", Moment).search("hel", wholeWords = true)
        assertEquals(listOf("hel"), withTimeout(5000) { whole.live.flow.first { it.isNotEmpty() } }.map { it.text })
    }

    @Test
    fun aWarmQueryAnswersFromMemoryAndALiveReadLeavesItWarm() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        assertNull(Moment.recent.cached())
        val moment = Moment.send(AccountScope.current(), text = "m").row
        Moment.recent.warm()
        val warm = Moment.recent.cached()!!
        assertEquals(listOf(moment.id), warm.value.map { it.id })
        assertEquals("m", warm.value.first().text)
        assertNull(Moment.recent.cachedRow("ghost"))
        reopen(Config(null, null, 32u), modelsJSON, null)
        assertNull(Moment.recent.cached())
        Moment.send(AccountScope.current(), text = "n").row
        assertEquals(1, withTimeout(5000) { Moment.recent.live.flow.first { it.isNotEmpty() } }.size)
        assertEquals("n", Moment.recent.cached()!!.value.first().text)
    }

    @Test
    fun writesReachTypedLiveRows() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val live = Moment.recent.live.flow
        assertEquals(emptyList<Moment>(), withTimeout(5000) { live.first() })
        val moment = Moment.send(AccountScope.current(), text = "m").row
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
        val entity = Projected(AccountScope.current().entity(Moment, "1"), Moment, listScope)
        withTimeout(5000) { entity.projection.first { it.rows.isNotEmpty() } }
        assertEquals("a", entity.value?.uuid)
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
    fun confirmedIdWaitsForTheServerIdOfALocalCreate() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")

        val local = Moment.send(AccountScope.current(), text = "hello", media = null).row
        assertEquals(true, local.id.startsWith("temp"))
        withTimeout(5000) { local.settled(null) }
        assertEquals("m77", withTimeout(5000) { local.confirmedId() })
    }

    @Test
    fun aSnapshotCarriesTheTemporaryIdThatTheServerIdReplaced() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")
        val listScope = CoroutineScope(SupervisorJob())
        val list = Projected(AccountScope.current().stream(Moment.recent), Moment, listScope)
        list.loaded()

        val local = Moment.send(AccountScope.current(), text = "hello", media = null).row
        val projection = withTimeout(5000) { list.projection.first { it.temporaryIds.containsKey("m77") } }

        assertEquals(local.id, projection.temporaryIds["m77"])
        listScope.cancel()
    }

    @Test
    fun aCreateWithAnUploadShowsTheRecordAtOnceAndTheServerIdReplacesIt() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateBlob", """{"data":{"createBlob":{"url":"https://s3/x","headers":{},"signedId":"sid","blobId":"b1"}}}""")
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob(${'$'}filename: String!, ${'$'}contentType: String!, ${'$'}byteSize: Int!, ${'$'}checksum: String!) { createBlob(filename: ${'$'}filename, contentType: ${'$'}contentType, byteSize: ${'$'}byteSize, checksum: ${'$'}checksum) { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""", uploads = UploadOperations(createBlob = UploadOperation("CreateBlob")))
        val file = java.io.File.createTempFile("clip", ".mp4").apply { writeText("abcdef") }

        val values = kotlinx.serialization.json.buildJsonObject { put("text", kotlinx.serialization.json.JsonPrimitive("hello")) }
        val request = UploadRequest(file.path, variables = values)
        val local = AccountScope.current().send(Moment, values, upload = UploadCommand("upload_clip", request)).row

        assertEquals(true, local.id.startsWith("temp"))
        assertEquals(true, local.uploads["upload_clip"] != null)
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

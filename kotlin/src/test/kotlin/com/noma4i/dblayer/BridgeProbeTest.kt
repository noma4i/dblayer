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
    fun aReactionUsesItsDeclaredKeyAcrossGeneratedValuesAndCells() = bounded {
        openRecordSession()
        val initial = settledRecord()
        val row = initial.update(reactions = listOf(RecordReactionNode(body = "A", userId = "u1"))).optimisticValue!!
        val changed = row.react(reactionBody = "C", userId = "u1").optimisticValue!!
        assertEquals(listOf("u1"), changed.reactions.map { it.userId })
        assertEquals(listOf("C"), changed.reactions.map { it.body })
    }

    @Test
    fun anOptimisticReactionCarriesTheNestedUserOfTheLocalRow() = bounded {
        openRecordSession()
        val ann = User.create(name = "Ann").optimisticValue!!
        val record = settledRecord()
        val withUser = record.react(reactionBody = "A", userId = ann.id).optimisticValue!!
        assertEquals("Ann", withUser.reactions.first().user?.name)
        val withoutUser = record.react(reactionBody = "B", userId = "nobody").optimisticValue!!
        assertNull(withoutUser.reactions.first { it.userId == "nobody" }.user)
    }

    @Test
    fun aSearchPassesWholeWordsToTheCore() = bounded {
        openSignedOffline(Config(null, null), modelsJSON)
        Moment.create(text = "hello world", account = AccountScope.current()).optimisticValue!!
        Moment.create(text = "hel", account = AccountScope.current()).optimisticValue!!
        val whole = Query("Moment", Moment).search("hel", wholeWords = true)
        assertEquals(listOf("hel"), withTimeout(5000) { whole.live.flow.first { it.isNotEmpty() } }.map { it.text })
    }

    @Test
    fun updateAndDestroyReachTypedLiveRows() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","text":"m","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
        transport.enqueue("UpdateMoment", """{"data":{"updateMoment":{"__typename":"Moment","id":"1","uuid":"a","text":"edited","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        transport.enqueue("DeleteMoment", """{"data":{"deleteMoment":{"id":"1"}}}""")
        openSigned(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid text likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"UpdateMoment","document":"mutation UpdateMoment(${'$'}id: ID!, ${'$'}text: String) { updateMoment(id: ${'$'}id, text: ${'$'}text) { id uuid text likedByMe media createdAt } }"},{"name":"DeleteMoment","document":"mutation DeleteMoment(${'$'}id: ID!) { deleteMoment(id: ${'$'}id) { id } }"}]""")
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
    fun anOfflineUpdateIsSentOnceWhenTheNetworkReturns() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","text":"m","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
        transport.enqueue("UpdateMoment", """{"data":{"updateMoment":{"__typename":"Moment","id":"1","uuid":"a","text":"edited","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        openSigned(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid text likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"UpdateMoment","document":"mutation UpdateMoment(${'$'}id: ID!, ${'$'}text: String) { updateMoment(id: ${'$'}id, text: ${'$'}text) { id uuid text likedByMe media createdAt } }"},{"name":"DeleteMoment","document":"mutation DeleteMoment(${'$'}id: ID!) { deleteMoment(id: ${'$'}id) { id } }"}]""")
        val scope = CoroutineScope(SupervisorJob())
        val deliveries = java.util.concurrent.CopyOnWriteArrayList<List<String?>>()
        scope.launch { Moment.recent.live.flow.collect { rows -> deliveries.add(rows.map { it.text }); TestPulse.pulse() } }
        try {
            eventually("the open list shows its first value") { deliveries.isNotEmpty() }
            Moment.feed.load()
            eventually("the open list shows the loaded row") { deliveries.lastOrNull() == listOf<String?>("m") }
            val moment = Moment.recent.current().first()
            DBlayer.database.setOnline(false)
            val receipt = moment.update(text = GraphQLNullable.Value("edited"))
            eventually("the same open list shows the edited row") { deliveries.lastOrNull() == listOf<String?>("edited") }
            assertEquals(0, transport.calls.count { it.operationName == "UpdateMoment" })
            DBlayer.database.setOnline(true)
            assertTrue(withTimeout(5000) { receipt.outcome() } is OperationOutcome.Confirmed)
            assertEquals(1, transport.calls.count { it.operationName == "UpdateMoment" })
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun writesReachTypedLiveRows() = bounded {
        openSignedOffline(Config(null, null), modelsJSON)
        val live = Moment.recent.live.flow
        assertEquals(emptyList<Moment>(), withTimeout(5000) { live.first() })
        val moment = Moment.create(text = "m", account = AccountScope.current()).optimisticValue!!
        assertEquals("m", withTimeout(5000) { Moment.recent.live.flow.first { it.isNotEmpty() } }.first().text)
        moment.setLocal("tone", kotlinx.serialization.json.JsonPrimitive("warm"))
        assertEquals("warm", withTimeout(5000) { Moment.recent.live.flow.first { it.firstOrNull()?.tone == "warm" } }.first().tone)
        moment.setLocal("tone", kotlinx.serialization.json.JsonNull)
        assertNull(withTimeout(5000) { Moment.recent.live.flow.first { it.firstOrNull()?.tone == null } }.first().tone)
    }

    @Test
    fun loadRequestsOnlyAWindowThatIsNotFreshAndKeepsNoSubscription() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
        openSigned(Config(null, null), modelsJSON, transport)
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
        openSigned(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")

        val thrown = runCatching { Moment.feed.refresh() }.exceptionOrNull()

        assertEquals(true, thrown is DbException)
        val failed = withTimeout(5000) { Moment.feed.snapshots.flow.first { it.phase == LoadPhase.Failed } }
        assertEquals(true, failed.error != null)
    }

    @Test
    fun feedLoadsFromTheTransportIntoLiveRowsAndReportsItsState() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":true,"endCursor":"c1"},"totalCount":9}}}""")
        openSigned(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")

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

    @Test
    fun theOutcomeOfASentCreateCarriesTheServerId() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        openSigned(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")

        val sent = Moment.create(text = "hello", media = null, account = AccountScope.current())
        assertEquals(true, sent.optimisticValue?.id?.startsWith("temp"))
        assertEquals("m77", withTimeout(5000) { sent.confirmed() }.id)
    }

    @Test
    fun aRewriteCarriesTheTemporaryIdThatTheServerIdReplaced() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        openSigned(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")
        val stream = AccountScope.current().stream(Moment.recent)

        val local = Moment.create(text = "hello", media = null, account = AccountScope.current()).optimisticValue!!
        val settled = withTimeout(5000) {
            stream.values.mapNotNull { value -> (value as? ReadValue.Delta)?.edits?.filterIsInstance<RowEdit.Rewrite>()?.firstOrNull { it.row.id == "m77" }?.temporary }.first()
        }

        assertEquals(local.id, settled)
        stream.close()
    }

    @Test
    fun aCreateWithAnUploadShowsTheRecordAtOnceAndTheServerIdReplacesIt() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateBlob", """{"data":{"blobCreate":{"url":"https://s3/x","headers":{},"signedId":"sid","blobId":"b1"}}}""")
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        openSigned(Config(null, null), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob(${'$'}filename: String!, ${'$'}contentType: String!, ${'$'}byteSize: Int!, ${'$'}checksum: String!) { createBlob(filename: ${'$'}filename, contentType: ${'$'}contentType, byteSize: ${'$'}byteSize, checksum: ${'$'}checksum) { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")
        val file = java.io.File.createTempFile("clip", ".mp4").apply { writeText("abcdef") }

        val values = kotlinx.serialization.json.buildJsonObject { put("text", kotlinx.serialization.json.JsonPrimitive("hello")) }
        val request = UploadRequest(file.path, variables = values)
        val receipt = AccountScope.current().send(Moment, values, upload = UploadCommand("upload_clip", request))
        val local = receipt.optimisticValue!!

        assertEquals(true, local.id.startsWith("temp"))
        assertEquals(true, local.uploads["upload_clip"] != null)
        assertEquals("m77", withTimeout(5000) { receipt.confirmed() }.id)
        val sent = transport.calls.filter { it.operationName == "CreateMoment" }
        assertEquals(1, sent.size)
        assertEquals(true, sent.first().variablesJson.contains("\"signedId\":\"sid\""))
        assertEquals(false, sent.first().variablesJson.contains("blobId"))
    }

}

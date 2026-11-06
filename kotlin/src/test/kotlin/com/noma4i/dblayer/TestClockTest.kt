package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.CurrentUser
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(DBlayerTestingApi::class)
class TestClockTest {
    @Test
    fun cancellingOneLoadInterestKeepsTheOtherCallAlive() = bounded {
        try {
            closeForCleanup()
            val transport = MockTransport()
            val hold = transport.hold("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
            reopenClocked(modelsJSON, transport, """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
            val first = Moment.feed.beginLoad()
            try {
                awaitWithin("the shared load starts") { hold.entered() }
                val second = Moment.feed.beginLoad()
                try {
                    first.cancel()
                    hold.release()
                    awaitWithin("the second load finishes") { second.result() }
                    assertEquals(1, transport.calls.count { it.operationName == "MomentsFeed" })
                    assertEquals(listOf("1"), Moment.feed.read().map { it.id })
                } finally {
                    second.cancel()
                }
            } finally {
                first.cancel()
                hold.release()
            }
        } finally {
            closeForCleanup()
        }
    }

    @Test
    fun aMutationDuringTheFirstPageKeepsTheWholeChainStale() = bounded {
        try {
            closeForCleanup()
            val transport = MockTransport()
            val row1 = """{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}"""
            val row2 = """{"__typename":"Moment","id":"2","uuid":"b","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}"""
            fun page(rows: String, more: Boolean, cursor: String) = """{"data":{"feed":{"nodes":[$rows],"pageInfo":{"hasNextPage":$more,"endCursor":"$cursor"}}}}"""
            transport.enqueue("MomentsFeed", page(row1, false, "seed"))
            val firstPage = transport.hold("MomentsFeed", page(row1, true, "first"))
            transport.enqueue("MomentsFeed", page(row2, false, "second"))
            transport.enqueue("MomentsFeed", page(row2, false, "fresh"))
            transport.enqueue("LikeMoment", """{"data":{"likeMoment":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":true,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
            val definitions = """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"LikeMoment","document":"mutation LikeMoment(${'$'}momentId: ID!) { likeMoment(momentId: ${'$'}momentId) { id uuid likedByMe media createdAt } }"}]"""
            reopenClocked(modelsJSON, transport, definitions)
            Moment.feed.load()
            val row = Moment.feed.read().single()
            val loading = async { Moment.feed.loadAll(force = true) }
            try {
                awaitWithin("the first page is held") { firstPage.entered() }
                val receipt = row.like()
                assertTrue(awaitWithin("the mutation is confirmed") { receipt.outcome() } is OperationOutcome.Confirmed)
                firstPage.release()
                awaitWithin("the old chain finishes") { loading.await() }
                assertEquals(3, transport.calls.count { it.operationName == "MomentsFeed" })
                Moment.feed.load()
                assertEquals(4, transport.calls.count { it.operationName == "MomentsFeed" })
                assertEquals(listOf("2"), Moment.feed.read().map { it.id })
                Moment.feed.load()
                assertEquals(4, transport.calls.count { it.operationName == "MomentsFeed" })
            } finally {
                firstPage.release()
                loading.cancel()
            }
        } finally {
            closeForCleanup()
        }
    }

    @Test
    fun aRefetchOwedWithoutAReaderSurvivesReopen() = bounded {
        val directory = kotlin.io.path.createTempDirectory().toFile()
        try {
            closeForCleanup()
            val path = directory.resolve("cache.sqlite").absolutePath
            val transport = MockTransport()
            for ((id, uuid) in listOf("1" to "a", "2" to "b")) {
                transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"$id","uuid":"$uuid","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
            }
            transport.enqueue("LikeMoment", """{"data":{"likeMoment":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":true,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
            val definitions = """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"LikeMoment","document":"mutation LikeMoment(${'$'}momentId: ID!) { likeMoment(momentId: ${'$'}momentId) { id uuid likedByMe media createdAt } }"}]"""
            reopenClocked(modelsJSON, transport, definitions, path = path, startMillis = 1_000_000L)
            Moment.feed.load()
            val row = Moment.feed.read().single()
            assertEquals("1", row.id)
            val receipt = row.like()
            assertTrue(awaitWithin("the mutation is confirmed") { receipt.outcome() } is OperationOutcome.Confirmed)
            assertEquals(1, transport.calls.count { it.operationName == "MomentsFeed" })
            closeForCleanup()
            reopenClocked(modelsJSON, transport, definitions, path = path, startMillis = 1_000_000L)
            Moment.feed.load()
            assertEquals(listOf("2"), Moment.feed.read().map { it.id })
            assertEquals(2, transport.calls.count { it.operationName == "MomentsFeed" })
        } finally {
            closeForCleanup()
            directory.deleteRecursively()
        }
    }

    @Test
    fun aRefreshAfterReopenAndClockRollbackKeepsTheNewResponse() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val directory = kotlin.io.path.createTempDirectory().toFile()
        try {
            closeForCleanup()
            val path = directory.resolve("cache.sqlite").absolutePath
            val transport = MockTransport()
            transport.enqueue("CurrentUserQuery", """{"data":{"user":{"id":"u1","name":"A"}}}""")
            transport.enqueue("CurrentUserQuery", """{"data":{"user":{"id":"u1","name":"B"}}}""")
            val definitions = """[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]"""
            reopenClocked(modelsJSON, transport, definitions, path = path, startMillis = 1_000_000L)
            CurrentUser.current.refresh()
            val first = Projected(AccountScope.current().stream(CurrentUser.current), CurrentUser, scope)
            awaitWithin("the initial profile") { first.projection.first { it.rows.isNotEmpty() } }
            assertEquals("A", first.value?.name)
            first.close()
            closeForCleanup()

            reopenClocked(modelsJSON, transport, definitions, path = path, startMillis = 1_000L)
            CurrentUser.current.refresh()
            val second = Projected(AccountScope.current().stream(CurrentUser.current), CurrentUser, scope)
            awaitWithin("the reopened profile") { second.projection.first { it.rows.isNotEmpty() } }
            assertEquals("B", second.value?.name)
            assertEquals(2, transport.calls.count { it.operationName == "CurrentUserQuery" })
            second.close()
        } finally {
            scope.cancel()
            closeForCleanup()
            directory.deleteRecursively()
        }
    }

    @Test
    fun aTestClockMovesFreshForWithoutSleeping() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val transport = MockTransport()
            repeat(2) { transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""") }
            val down = transport.hold("MomentsFeed", failure = DbException.Network("down"))
            closeForCleanup()
            val clock = reopenClocked(modelsJSON, transport, """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
            Moment.feed.load()
            val held = Projected(AccountScope.current().stream(Moment.feed), Moment, scope)
            held.loaded()
            clock.advance(5 * 60_000L - 1_000)
            Moment.feed.load()
            assertEquals(1, transport.calls.count { it.operationName == "MomentsFeed" })
            clock.advance(2_000)
            Moment.feed.load()
            assertEquals(2, transport.calls.count { it.operationName == "MomentsFeed" })
            transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
            transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
            clock.advance(5 * 60_000L + 1_000)
            val failing = async { Moment.feed.load() }
            networkDrops(during = down)
            down.release()
            eventually("the third request reached the transport") { transport.calls.count { it.operationName == "MomentsFeed" } == 3 }
            assertTrue("a load without network keeps waiting", !failing.isCompleted)
            DBlayer.database.setOnline(true)
            awaitWithin("the load after the network returned") { failing.await() }
            assertEquals(4, transport.calls.count { it.operationName == "MomentsFeed" })
            val state = held.projection.first { it.error == null && it.phase == ReadPhase.LOADED }
            assertEquals(listOf("1"), state.rows.map { it.id })
            held.close()
        } finally {
            scope.cancel()
            closeForCleanup()
        }
    }
}

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
            openSignedClocked(modelsJSON, transport, definitions)
            Moment.feed.load()
            val row = Moment.feed.read().single()
            val loading = async { Moment.feed.loadAll(force = true) }
            try {
                awaitWithin("the first page is held") { firstPage.entered() }
                val receipt = row.like()
                awaitWithin("the mutation is confirmed") { receipt.confirmed() }
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
            openSignedClocked(modelsJSON, transport, definitions, path = path, startMillis = 1_000_000L)
            Moment.feed.load()
            val row = Moment.feed.read().single()
            assertEquals("1", row.id)
            val receipt = row.like()
            awaitWithin("the mutation is confirmed") { receipt.confirmed() }
            assertEquals(1, transport.calls.count { it.operationName == "MomentsFeed" })
            closeForCleanup()
            openSignedClocked(modelsJSON, transport, definitions, path = path, startMillis = 1_000_000L)
            Moment.feed.load()
            assertEquals(listOf("2"), Moment.feed.read().map { it.id })
            assertEquals(2, transport.calls.count { it.operationName == "MomentsFeed" })
        } finally {
            closeForCleanup()
            directory.deleteRecursively()
        }
    }
}

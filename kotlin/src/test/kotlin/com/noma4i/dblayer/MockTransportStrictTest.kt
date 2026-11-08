package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch

class MockTransportStrictTest {
    private val request = TransportRequest("query Ping { ping }", "Ping", """{"page":2}""", NetworkPurpose.STANDARD)

    private fun outcome(transport: MockTransport): Throwable? = bounded {
        try { transport.query(request); null } catch (error: Throwable) { error }
    }

    @Test
    fun aCallWithoutAReplyFailsNamingTheOperationAndVariables() {
        val transport = MockTransport()
        val error = outcome(transport)
        assertTrue(error.toString(), error is DbException.Invalid)
        assertTrue(error?.message, error?.message?.contains("Ping") == true && error.message?.contains("""{"page":2}""") == true)
        assertEquals(listOf("Ping"), transport.claimUnscripted().map { it.operationName })
        assertEquals(listOf("Ping"), transport.calls.map { it.operationName })
    }

    private class IdleProgress : ByteProgress(NoHandle) {
        override fun isCancelled(): Boolean = false
        override fun sent(bytes: ULong) = Unit
    }

    private suspend fun put(transport: MockTransport, url: String): PutReply = transport.scriptedPut(url, emptyMap(), PutFile("unused", 0u, 4u), TransferIdentity(1L, "a", TransferStep.Put), IdleProgress())

    @Test
    fun bridge_prop_strict_scripts_eachPutConsumesOneScriptAndAnUnscriptedPutFails() = bounded {
        val transport = MockTransport(Unanswered.NetworkError)
        transport.enqueuePut("u", DbException.Http(503.toUShort(), "down"))
        transport.enqueuePut("u", "ok")
        assertTrue(runCatching { put(transport, "u") }.exceptionOrNull() is DbException.Http)
        assertEquals("ok", put(transport, "u").etag)
        assertTrue(runCatching { put(transport, "u") }.exceptionOrNull() is DbException.Network)
        assertEquals(listOf("u", "u", "u"), transport.putCalls)
    }

    class UnansweredLoad {
        fun loadsWithoutAReply() = bounded {
            val transport = MockTransport()
            openSigned(Config(null, null), modelsJSON, transport, """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
            runCatching { Moment.feed.refresh() }
            eventually("the load reaches the mock", 5000) { transport.unscripted.isNotEmpty() }
        }
    }

    @Test
    fun anUnansweredLoadOfTheCoreFailsTheNextTouchOfAMockOnTheTestThread() {
        UnansweredLoad().loadsWithoutAReply()
        val error = try { MockTransport(); null } catch (error: AssertionError) { error }
        assertTrue(error.toString(), error?.message?.contains("MomentsFeed") == true && error.message?.contains("UnansweredLoad") == true)
        MockTransport()
    }

    @Test
    fun aCallWithoutAReplyIsANetworkErrorOnlyWhenTheTestSaysSo() {
        val transport = MockTransport(Unanswered.NetworkError)
        assertTrue(outcome(transport) is DbException.Network)
        assertEquals(emptyList<TransportRequest>(), transport.claimUnscripted())
    }

    @Test
    fun aRepeatOfACancelledRequestGetsTheSameReplyAndAnotherRequestTheNextOne() = bounded {
        val transport = MockTransport(Unanswered.NetworkError)
        val held = transport.hold("Ping", reply = "first")
        transport.enqueue("Ping", "second")
        val same = TransportRequest("query Ping { ping }", "Ping", """{"page":2}""", NetworkPurpose.STANDARD, Author.WINDOW, 1uL, 1u)
        val repeated = TransportRequest("query Ping { ping }", "Ping", """{"page":2}""", NetworkPurpose.STANDARD, Author.WINDOW, 1uL, 2u)
        val other = TransportRequest("query Ping { ping }", "Ping", """{"page":2}""", NetworkPurpose.STANDARD, Author.WINDOW, 2uL, 1u)
        val cancelled = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.Dispatchers.Default).launch { transport.mutate(same) }
        held.entered()
        cancelled.cancelAndJoin()
        assertEquals("first", transport.mutate(repeated))
        assertEquals("first", transport.mutate(repeated))
        assertEquals("second", transport.mutate(other))
        assertEquals("second", transport.mutate(other))
        val unrecorded = TransportRequest("query Ping { ping }", "Ping", """{"page":2}""", NetworkPurpose.STANDARD, Author.WINDOW, 3uL, 1u)
        assertTrue(runCatching { transport.mutate(unrecorded) }.exceptionOrNull() is DbException.Network)
    }

    @Test
    fun aRepeatOfAFailedRequestGetsTheSameFailureAndAnotherRequestTheNextReply() = bounded {
        val transport = MockTransport(Unanswered.NetworkError)
        val down = DbException.Http(503.toUShort(), "down")
        transport.enqueue("Ping", down)
        transport.enqueue("Ping", "second")
        val first = TransportRequest("query Ping { ping }", "Ping", """{"page":2}""", NetworkPurpose.STANDARD, Author.WINDOW, 1uL, 1u)
        val repeated = TransportRequest("query Ping { ping }", "Ping", """{"page":2}""", NetworkPurpose.STANDARD, Author.WINDOW, 1uL, 2u)
        val other = TransportRequest("query Ping { ping }", "Ping", """{"page":2}""", NetworkPurpose.STANDARD, Author.WINDOW, 2uL, 1u)
        val third = TransportRequest("query Ping { ping }", "Ping", "{}", NetworkPurpose.STANDARD, Author.WINDOW, 3uL, 1u)
        for (attempt in listOf(first, repeated)) {
            val error = runCatching { transport.mutate(attempt) }.exceptionOrNull()
            assertTrue(error.toString(), error === down)
        }
        assertEquals("second", transport.mutate(other))
        assertTrue(runCatching { transport.mutate(third) }.exceptionOrNull() is DbException.Network)
    }

    @Test
    fun aFailureOfTheTransportIsNotRecordedForThePairAndTheRepeatTakesTheNextReply() = bounded {
        val transport = MockTransport(Unanswered.NetworkError)
        transport.enqueue("Ping", DbException.Network("down"))
        transport.enqueue("Ping", "second")
        val first = TransportRequest("query Ping { ping }", "Ping", """{"page":2}""", NetworkPurpose.STANDARD, Author.WINDOW, 1uL, 1u)
        val repeated = TransportRequest("query Ping { ping }", "Ping", """{"page":2}""", NetworkPurpose.STANDARD, Author.WINDOW, 1uL, 2u)
        assertTrue(runCatching { transport.mutate(first) }.exceptionOrNull() is DbException.Network)
        assertEquals("second", transport.mutate(repeated))
        assertEquals("second", transport.mutate(first))
    }

    @Test
    fun twoWindowsOfOneOperationEachGetTheirOwnReplyInAnyOrder() = bounded {
        val transport = MockTransport(Unanswered.NetworkError)
        transport.respondWith("Ping") { request -> if (request.variablesJson.contains("\"page\":1")) "one" else null }
        transport.enqueue("Ping", "queued")
        val first = TransportRequest("query Ping { ping }", "Ping", """{"page":1}""", NetworkPurpose.STANDARD, Author.WINDOW, 1uL, 1u)
        val second = TransportRequest("query Ping { ping }", "Ping", """{"page":2}""", NetworkPurpose.STANDARD, Author.WINDOW, 2uL, 1u)
        assertEquals("queued", transport.query(second))
        assertEquals("one", transport.query(first))
        assertEquals("one", transport.query(first))
        assertEquals("queued", transport.query(second))
        val third = TransportRequest("query Ping { ping }", "Ping", """{"page":3}""", NetworkPurpose.STANDARD, Author.WINDOW, 3uL, 1u)
        assertTrue(runCatching { transport.query(third) }.exceptionOrNull() is DbException.Network)
    }
}

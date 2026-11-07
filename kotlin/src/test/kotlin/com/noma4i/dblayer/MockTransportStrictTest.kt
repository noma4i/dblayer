package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

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

    class UnansweredLoad {
        fun loadsWithoutAReply() = bounded {
            val transport = MockTransport()
            reopen(Config(null, null), modelsJSON, transport, """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
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
}

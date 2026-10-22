package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

class MockTransportStrictTest {
    private val request = TransportRequest("query Ping { ping }", "Ping", """{"page":2}""")

    private fun outcome(transport: MockTransport): Throwable? = bounded {
        try { transport.query(request); null } catch (error: Throwable) { error }
    }

    @Test
    fun aCallWithoutAReplyFailsNamingTheOperationAndVariables() {
        val transport = MockTransport()
        val error = outcome(transport)
        assertTrue(error.toString(), error is DbException.Invalid)
        assertTrue(error?.message, error?.message?.contains("Ping") == true && error.message?.contains("""{"page":2}""") == true)
        assertEquals(listOf("Ping"), transport.unscripted.map { it.operationName })
        assertEquals(listOf("Ping"), transport.calls.map { it.operationName })
    }

    @Test
    fun aCallWithoutAReplyIsANetworkErrorOnlyWhenTheTestSaysSo() {
        val transport = MockTransport(Unanswered.NetworkError)
        assertTrue(outcome(transport) is DbException.Network)
        assertEquals(emptyList<TransportRequest>(), transport.unscripted)
    }

    @Test
    fun anUnknownTokenIsRecordedWithoutThrowing() {
        val transport = MockTransport()
        try { transport.unsubscribe("never-issued") } catch (error: Throwable) { fail("unsubscribe threw $error") }
        assertEquals(listOf("never-issued"), transport.unknownTokens.toList())
    }
}

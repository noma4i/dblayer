package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.Operations
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class SealedErrorsTest {
    @Test fun modelRetriesPropagateErrorsToTheirCaller() = bounded {
        openSignedOffline(Config(null), modelsJSON)
        val row = Moment.create(text = "saved", account = AccountScope.current()).optimisticValue!!
        DBlayer.close()
        org.junit.Assert.assertTrue(runCatching { row.retry(null) }.exceptionOrNull() is DbException.Closed)
    }
    private val codes = listOf(
        SessionCode.SEQUENCE_CONFLICT, SessionCode.REPLAYED_REQUEST, SessionCode.RATE_LIMITED, SessionCode.ACCOUNT_UNAVAILABLE,
        SessionCode.UNAUTHENTICATED, SessionCode.SESSION_ENDED, SessionCode.INVALID_CODE, SessionCode.INVALID_PHONE,
        SessionCode.INVALID_PROVIDER, SessionCode.PROVIDER_REJECTED, SessionCode.ALREADY_AUTHENTICATED,
    )

    private val operations = """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"},{"name":"QrCodeCreate","document":"mutation QrCodeCreate(${'$'}input: CreateQrCodeInput!) { qrCodeCreate(input: ${'$'}input) { url } }"}]"""

    private fun codeOf(error: Throwable?, case: String): SessionCode {
        val session = error as? DbException.Session ?: throw AssertionError("$case: surfaced as $error, not as a session error")
        return session.code
    }

    @Test
    fun aSessionCodeArrivesTypedThroughALoadAnOperationCallAndAMutationOutcome() = bounded {
        for (code in codes) {
            val transport = MockTransport()
            openSigned(Config(null), modelsJSON, transport)
            registerStandOperations(DBlayer.database, operations)
            val failure = DbException.Session(code, code.name)

            if (code == SessionCode.SESSION_ENDED) {
                transport.enqueue("MomentsFeed", failure)
                val load = runCatching { withTimeout(5000) { Moment.feed.load() } }.exceptionOrNull()
                org.junit.Assert.assertTrue("load $code: $load", load is DbException.NoSession)
                continue
            }
            if (code != SessionCode.REPLAYED_REQUEST) {
                transport.enqueue("MomentsFeed", failure)
                val load = runCatching { withTimeout(5000) { Moment.feed.load() } }.exceptionOrNull()
                if (code == SessionCode.UNAUTHENTICATED) org.junit.Assert.assertTrue("load $code: $load", load is DbException.Unauthorized)
                else assertEquals(code, codeOf(load, "load $code"))

                transport.enqueue("QrCodeCreate", failure)
                val call = runCatching { withTimeout(5000) { Operations.qrCodeCreate(kind = "container", size = 3) } }.exceptionOrNull()
                if (code == SessionCode.UNAUTHENTICATED) org.junit.Assert.assertTrue("call $code: $call", call is DbException.Unauthorized)
                else assertEquals(code, codeOf(call, "call $code"))
            }

            transport.enqueue("CreateMoment", failure)
            val sent = Moment.create(text = "hello", media = null, account = AccountScope.current())
            assertEquals(code, withTimeout(5000) { sent.refusal() }.sessionCode)
        }
    }

}

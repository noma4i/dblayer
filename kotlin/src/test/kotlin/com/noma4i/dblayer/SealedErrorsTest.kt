package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.Operations
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class SealedErrorsTest {
    private class RefusedRetry : Database(NoHandle) {
        override suspend fun retry(epoch: ULong, model: String, rowId: String, action: String?, source: WireUploadRequest?): WireOperationReceipt = throw DbException.Invalid("retry refused")
        override fun admit(accountEpoch: ULong?): ULong = accountEpoch ?: 1uL
    }

    @Test fun modelRetriesPropagateErrorsToTheirCaller() = bounded {
        reopenSigned(Config(null, null), modelsJSON)
        val row = Moment.create(text = "saved", account = AccountScope.current()).optimisticValue!!
        val real = DBlayer.database
        DBlayer.database = RefusedRetry()
        try {
            org.junit.Assert.assertTrue(runCatching { row.retry(null) }.exceptionOrNull() is DbException.Invalid)
        } finally {
            DBlayer.database = real
        }
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
            reopen(Config(null, null), modelsJSON, transport)
            DBlayer.database.registerOperations(operations)
            val failure = DbException.Session(code, code.name)

            transport.enqueue("MomentsFeed", failure)
            if (code == SessionCode.REPLAYED_REQUEST) transport.enqueue("MomentsFeed", failure)
            assertEquals(code, codeOf(runCatching { withTimeout(5000) { Moment.feed.load() } }.exceptionOrNull(), "load $code"))

            transport.enqueue("QrCodeCreate", failure)
            assertEquals(code, codeOf(runCatching { withTimeout(5000) { Operations.qrCodeCreate(kind = "container", size = 3) } }.exceptionOrNull(), "call $code"))

            transport.enqueue("CreateMoment", failure)
            val sent = Moment.create(text = "hello", media = null, account = AccountScope.current())
            assertEquals(code, withTimeout(5000) { sent.refusal() }.sessionCode)
        }
    }

}

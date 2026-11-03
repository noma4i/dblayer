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
        override suspend fun retry(epoch: ULong, model: String, rowId: String, action: String?): ULong = throw DbException.Invalid("retry refused")
        override suspend fun retryAll(): ULong = throw DbException.Invalid("retry refused")
    }

    @Test fun modelRetriesPropagateErrorsToTheirCaller() = bounded {
        reopenSigned(Config(null, null, 8u), modelsJSON)
        val row = Moment.send(AccountScope.current(), text = "saved").row
        val real = DBlayer.database
        DBlayer.database = RefusedRetry()
        try {
            org.junit.Assert.assertTrue(runCatching { row.retry(null) }.exceptionOrNull() is DbException.Invalid)
            org.junit.Assert.assertTrue(runCatching { DBlayer.retryAll() }.exceptionOrNull() is DbException.Invalid)
        } finally {
            DBlayer.database = real
        }
    }
    private val codes = listOf(
        SessionCode.SEQUENCE_CONFLICT, SessionCode.REPLAYED_REQUEST, SessionCode.RATE_LIMITED, SessionCode.ACCOUNT_UNAVAILABLE,
        SessionCode.UNAUTHENTICATED, SessionCode.SESSION_ENDED, SessionCode.INVALID_CODE, SessionCode.INVALID_PHONE,
        SessionCode.INVALID_PROVIDER, SessionCode.PROVIDER_REJECTED, SessionCode.ALREADY_AUTHENTICATED, SessionCode.STATE_LOST,
    )

    private val operations = """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"},{"name":"QrCodeCreate","document":"mutation QrCodeCreate(${'$'}input: CreateQrCodeInput!) { qrCodeCreate(input: ${'$'}input) { url } }"}]"""

    private fun codeOf(error: Throwable?, case: String): SessionCode {
        val session = error as? DbException.Session ?: throw AssertionError("$case: surfaced as $error, not as a session error")
        return session.code
    }

    @Test
    fun aSessionCodeArrivesTypedThroughALoadAnOperationCallAndASettledMutation() = bounded {
        for (code in codes) {
            val transport = MockTransport()
            reopen(Config(null, null, 32u), modelsJSON, transport)
            DBlayer.database.registerOperations(operations)
            val failure = DbException.Session(code, code.name)

            transport.enqueue("MomentsFeed", failure)
            assertEquals(code, codeOf(runCatching { withTimeout(5000) { Moment.feed.load() } }.exceptionOrNull(), "load $code"))

            transport.enqueue("QrCodeCreate", failure)
            assertEquals(code, codeOf(runCatching { withTimeout(5000) { Operations.qrCodeCreate(kind = "container", size = 3) } }.exceptionOrNull(), "call $code"))

            transport.enqueue("CreateMoment", failure)
            val local = Moment.send(AccountScope.current(), text = "hello", media = null).row
            assertEquals(code, codeOf(runCatching { withTimeout(5000) { local.settled(null) } }.exceptionOrNull(), "settled $code"))
        }
    }

}

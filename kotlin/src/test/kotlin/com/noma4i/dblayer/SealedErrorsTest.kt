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
        override suspend fun retry(model: String, rowId: String): ULong = throw DbException.Invalid("retry refused")
        override suspend fun retryAll(): ULong = throw DbException.Invalid("retry refused")
    }

    @Test fun modelRetriesPropagateErrorsToTheirCaller() = bounded {
        DBlayer.database = Database.open(Config(null, null, 8u), modelsJSON, null)
        val row = Moment.create(text = "saved")
        DBlayer.database = RefusedRetry()
        org.junit.Assert.assertTrue(runCatching { row.retry() }.exceptionOrNull() is DbException.Invalid)
        org.junit.Assert.assertTrue(runCatching { retryAll() }.exceptionOrNull() is DbException.Invalid)
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
    fun aSessionCodeArrivesTypedThroughALoadAnOperationCallAndASettledMutation() = bounded {
        for (code in codes) {
            val transport = MockTransport()
            DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
            DBlayer.database.registerOperations(operations)
            val failure = DbException.Session(code, code.name)

            transport.enqueue("MomentsFeed", failure)
            assertEquals(code, codeOf(runCatching { withTimeout(5000) { Moment.feed.load() } }.exceptionOrNull(), "load $code"))

            transport.enqueue("QrCodeCreate", failure)
            assertEquals(code, codeOf(runCatching { withTimeout(5000) { Operations.qrCodeCreate(kind = "container", size = 3) } }.exceptionOrNull(), "call $code"))

            transport.enqueue("CreateMoment", failure)
            val local = Moment.create(text = "hello", media = null)
            assertEquals(code, codeOf(runCatching { withTimeout(5000) { local.settled() } }.exceptionOrNull(), "settled $code"))
        }
    }

    @Test
    fun aLostSessionKeepsTheQueuedMutationFailedAndRetrySendsItOnce() = bounded {
        for (code in listOf(SessionCode.UNAUTHENTICATED, SessionCode.ACCOUNT_UNAVAILABLE)) {
            val transport = MockTransport()
            DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
            DBlayer.database.registerOperations(operations)
            transport.enqueue("CreateMoment", DbException.Session(code, code.name))
            val local = Moment.create(text = "hello", media = null)
            assertEquals(code, codeOf(runCatching { withTimeout(5000) { local.settled() } }.exceptionOrNull(), "settled $code"))
            assertEquals("hello", Moment.find(local.id).current()?.text)

            transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
            val retried = withTimeout(5000) { if (code == SessionCode.UNAUTHENTICATED) local.retry() else retryAll() }
            assertEquals(1uL, retried)
            withTimeout(5000) { local.settled() }

            assertEquals(2, transport.calls.count { it.operationName == "CreateMoment" })
        }
    }

    @Test
    fun anotherSessionCodeRollsTheQueuedMutationBack() = bounded {
        val transport = MockTransport()
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations(operations)
        transport.enqueue("CreateMoment", DbException.Session(SessionCode.RATE_LIMITED, "RATE_LIMITED"))
        val local = Moment.create(text = "hello", media = null)

        assertEquals(SessionCode.RATE_LIMITED, codeOf(runCatching { withTimeout(5000) { local.settled() } }.exceptionOrNull(), "settled"))

        assertNull(Moment.find(local.id).current())
    }
}

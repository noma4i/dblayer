package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.Serializable
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@Serializable private data class PhoneBody(val phone: String)
@Serializable private data class TicketAnswer(val ticket: String)

class SessionBridgeTest {
    private class RecordingSession : Database(NoHandle) {
        val calls = java.util.concurrent.CopyOnWriteArrayList<String>()

        override suspend fun sessionCall(operation: String, bodyJson: String): String {
            calls.add("call $operation $bodyJson")
            return """{"ticket":"t-1"}"""
        }
        override suspend fun sessionReset() { calls.add("reset") }
        override suspend fun retry(epoch: ULong, model: String, rowId: String, action: String?, source: WireUploadRequest?): WireOperationReceipt {
            calls.add("retry $model $rowId ${action ?: "null"}")
            throw DbException.Invalid("recorded")
        }
        override fun sessionEpoch(): ULong = 1uL
    }

    private suspend fun withRecordingSession(block: suspend (RecordingSession) -> Unit) {
        reopen(Config(null, null, 8u), modelsJSON, null)
        val real = DBlayer.database
        val recording = RecordingSession()
        DBlayer.database = recording
        try {
            block(recording)
        } finally {
            DBlayer.database = real
        }
    }

    @Test fun aSessionCallSendsTheEncodedBodyAndDecodesTheAnswer() = bounded {
        withRecordingSession { recording ->
            val answer: TicketAnswer = DBlayer.sessionCall("otp", PhoneBody("+61400000000"))
            assertEquals(TicketAnswer("t-1"), answer)
            DBlayer.sessionSend("otp", PhoneBody("+61411111111"))
            assertEquals(listOf("""call otp {"phone":"+61400000000"}""", """call otp {"phone":"+61411111111"}"""), recording.calls)
        }
    }

    @Test fun sessionResetReachesTheCore() = bounded {
        withRecordingSession { recording ->
            DBlayer.sessionReset()
            assertEquals(listOf("reset"), recording.calls)
        }
    }

    @Test fun retryByIdReachesTheCore() = bounded {
        withRecordingSession { recording ->
            assertTrue(runCatching { Moment.retry("42", "like") }.exceptionOrNull() is DbException.Invalid)
            assertTrue(runCatching { Moment.retry("43", null) }.exceptionOrNull() is DbException.Invalid)
            assertEquals(listOf("retry Moment 42 like", "retry Moment 43 null"), recording.calls)
        }
    }
}

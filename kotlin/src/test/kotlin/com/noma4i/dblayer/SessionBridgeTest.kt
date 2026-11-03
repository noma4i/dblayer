package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.Serializable
import org.junit.Assert.assertEquals
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
        override suspend fun sessionResume(): String? {
            calls.add("resume")
            return """{"ticket":"t-2"}"""
        }
        override suspend fun sessionReset() { calls.add("reset") }
        override suspend fun retry(model: String, rowId: String, action: String?): ULong {
            calls.add("retry $model $rowId ${action ?: "null"}")
            return 3u
        }
        override suspend fun retryAll(): ULong {
            calls.add("retryAll")
            return 7u
        }
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

    @Test fun sessionResumeDecodesTheAnswerAndResetReachesTheCore() = bounded {
        withRecordingSession { recording ->
            assertEquals(TicketAnswer("t-2"), DBlayer.sessionResume<TicketAnswer>())
            DBlayer.sessionReset()
            assertEquals(listOf("resume", "reset"), recording.calls)
        }
    }

    @Test fun retryByIdAndRetryAllReachTheCoreWithTheirCounts() = bounded {
        withRecordingSession { recording ->
            assertEquals(3uL, Moment.retry("42", "like"))
            assertEquals(3uL, Moment.retry("43", null))
            assertEquals(7uL, DBlayer.retryAll())
            assertEquals(listOf("retry Moment 42 like", "retry Moment 43 null", "retryAll"), recording.calls)
        }
    }
}

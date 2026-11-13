package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.Serializable
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@Serializable private data class AccountBody(val account: String)
@Serializable private data class SessionAnswer(val sessionId: String)

class SessionBridgeTest {
    @Test fun aSessionCallSendsTheEncodedBodyAndDecodesTheAnswerAfterAResetOfTheSession() = bounded {
        openSignedOffline(Config(null), modelsJSON)
        DBlayer.sessionReset()
        DBlayer.database.setOnline(true)
        val answer: SessionAnswer = DBlayer.sessionCall("login_link", AccountBody("stand"))
        assertEquals(SessionAnswer("stand"), answer)
        val repeated = DBlayer.sessionCall<AccountBody, kotlinx.serialization.json.JsonElement>("login_link", AccountBody("stand"))
        assertEquals(SessionAnswer("stand").sessionId, (repeated as kotlinx.serialization.json.JsonObject).getValue("sessionId").toString().trim('"'))
    }

    @Test fun retryByIdReachesTheCoreAndItsErrorComesBack() = bounded {
        openSignedOffline(Config(null), modelsJSON)
        DBlayer.close()
        assertTrue(runCatching { Moment.retry("42", "like") }.exceptionOrNull() is DbException.Closed)
        assertTrue(runCatching { Moment.retry("43", null) }.exceptionOrNull() is DbException.Closed)
    }
}

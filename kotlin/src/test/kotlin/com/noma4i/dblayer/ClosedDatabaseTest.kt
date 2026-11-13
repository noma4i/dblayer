package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ClosedDatabaseTest {
    private suspend fun refusal(call: suspend () -> Unit): Throwable? = runCatching { call() }.exceptionOrNull()

    @Test
    fun twoConcurrentClosesBothSucceedAndThenTheCoreAnswersClosed() = bounded {
        openSignedOffline(Config(null), modelsJSON)
        val closes = List(2) { async(Dispatchers.Default) { runCatching { DBlayer.close() } } }
        val results = awaitWithin("both closes to return") { closes.awaitAll() }
        for (result in results) {
            assertTrue("${result.exceptionOrNull()}", result.isSuccess)
        }
        assertTrue(refusal { DBlayer.sessionReset() } is DbException.Closed)
    }

    @Test
    fun life_prop_not_open_typed() = bounded {
        suspend fun entries(): List<String> {
            val calls: List<suspend () -> Unit> = listOf(
                { DBlayer.call("Probe", JsonObject(emptyMap())) },
                { DBlayer.sessionCall<JsonNull, JsonElement>("Probe", JsonNull) },
                { DBlayer.sessionReset() },
                { DBlayer.subscriptionCensus() },
                { DBlayer.openReport() },
                { AccountScope.current() },
                { Moment.recent.live.current() },
                { DBlayer.close() },
            )
            return calls.map { call ->
                when (val error = runCatching { call() }.exceptionOrNull()) {
                    null -> "none"
                    is DbException.NotOpen -> "not_open"
                    is DbException.Closed -> "closed"
                    else -> "other $error"
                }
            }
        }
        closeForCleanup()
        assertTrue(refusal { OpenDatabase().opened() } is DbException.NotOpen)
        openSignedOffline(Config(null), modelsJSON)
        DBlayer.close()
        val afterClose = entries()
        assertEquals(List(7) { "closed" }, (0..6).map { afterClose[it] })
        assertEquals("none", afterClose[7])
    }

}

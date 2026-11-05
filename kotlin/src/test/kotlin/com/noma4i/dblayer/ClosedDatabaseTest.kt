package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import org.junit.Assert.assertTrue
import org.junit.Test

class ClosedDatabaseTest {
    private suspend fun refusal(call: suspend () -> Unit): Throwable? = runCatching { call() }.exceptionOrNull()

    @Test
    fun aClosedDatabaseRefusesEveryCallWithClosedFromTheCore() = bounded {
        reopenSigned(Config(null, null, 8u), modelsJSON)
        DBlayer.close()
        val errors = listOf(
            refusal { DBlayer.sessionReset() },
            refusal { Moment.send(AccountScope.current(), text = "late").optimisticValue!! },
            refusal { Moment.recent.live.current() },
        )
        for (error in errors) {
            assertTrue(error is DbException.Closed)
        }
    }

    @Test
    fun twoConcurrentClosesBothSucceedAndThenTheCoreAnswersClosed() = bounded {
        reopenSigned(Config(null, null, 8u), modelsJSON)
        val closes = List(2) { async(Dispatchers.Default) { runCatching { DBlayer.close() } } }
        val results = awaitWithin("both closes to return") { closes.awaitAll() }
        for (result in results) {
            assertTrue(result.isSuccess)
        }
        assertTrue(refusal { DBlayer.sessionReset() } is DbException.Closed)
    }
}

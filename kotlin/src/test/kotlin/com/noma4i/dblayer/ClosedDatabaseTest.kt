package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ClosedDatabaseTest {
    private val message = "database is not open; call DBlayer.open once at app start"

    private suspend fun refusal(call: suspend () -> Unit): Throwable? = runCatching { call() }.exceptionOrNull()

    @Test
    fun aClosedDatabaseRefusesEveryCallWithTheNotOpenError() = bounded {
        reopen(Config(null, null, 8u), modelsJSON)
        DBlayer.close()
        val errors = listOf(
            refusal { DBlayer.database },
            refusal { DBlayer.subscriptionCensus() },
            refusal { Moment.create(text = "late") },
            refusal { Moment.recent.warm() },
            refusal { Moment.recent.live.flow.first() },
            refusal { Moment.recent.changes.first() },
        )
        for (error in errors) {
            assertTrue(error is IllegalStateException)
            assertEquals(message, error?.message)
        }
        assertEquals(emptyList<Moment>(), Moment.recent.live.current())
    }

    @Test
    fun aSingletonOfAClosedDatabaseIsClosed() = bounded {
        DBlayer.close()
        val entity = User.entity()
        withTimeout(5000) { while (!entity.isClosed) delay(20) }
        assertEquals(null, entity.value.value)
    }
}

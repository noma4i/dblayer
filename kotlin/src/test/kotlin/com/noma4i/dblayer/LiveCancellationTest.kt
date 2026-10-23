package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class LiveCancellationTest {
    @Test
    fun currentDoesNotTurnCancellationIntoTheInitialValue() = bounded {
        val outcome = CompletableDeferred<String>()
        val reader = launch(Dispatchers.Default) {
            try {
                LiveSource<Int>(0) { _ -> {} }.current()
                outcome.complete("returned")
            } catch (cancelled: kotlinx.coroutines.CancellationException) {
                outcome.complete("cancelled")
                throw cancelled
            }
        }
        delay(200)
        reader.cancel()

        assertEquals("cancelled", withTimeout(3000) { outcome.await() })
    }

    @Test
    fun cancellingTheScopeIsNotAFailureOfTheList() = bounded {
        reopen(Config(null, null, 64u), modelsJSON, null)
        val scope = CoroutineScope(Dispatchers.Default + Job())
        val list = LiveList(Query("Moment", Moment), scope)
        delay(300)
        scope.cancel()
        delay(300)

        assertNull(list.snapshot.value.error)
    }
}

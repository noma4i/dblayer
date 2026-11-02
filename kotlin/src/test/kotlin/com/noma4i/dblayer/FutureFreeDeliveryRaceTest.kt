package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class FutureFreeDeliveryRaceTest {
    @Test
    fun kotlinFutureFreeDeliveryRace() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val database = DBlayer.database
        val handles = uniffiContinuationHandleMap.size
        val queue = database.changes("""{"model":"Moment"}""")
        val pending = CompletableDeferred<Unit>()
        val reader = launch(Dispatchers.Default) {
            pending.complete(Unit)
            while (queue.next() != null) { }
        }
        withTimeout(10_000) { pending.await() }
        val producer = launch(Dispatchers.Default) { repeat(20) { Moment.create(text = "n$it") } }
        val canceller = launch(Dispatchers.Default) { queue.cancel() }
        withTimeout(30_000) { reader.join(); producer.join(); canceller.join() }
        assertNull(withTimeout(10_000) { queue.next() })

        val second = database.changes("""{"model":"Moment"}""")
        val waiting = launch(Dispatchers.Default) { while (second.next() != null) { } }
        val writer = launch(Dispatchers.Default) { repeat(20) { Moment.create(text = "m$it") } }
        waiting.cancel()
        withTimeout(30_000) { waiting.join(); writer.join() }
        second.cancel()

        eventually("no continuation handle stays registered") { uniffiContinuationHandleMap.size == handles }
        eventually("no subscription remains") { DBlayer.subscriptionCensus().subscriptions == 0uL }
        assertEquals(handles, uniffiContinuationHandleMap.size)
    }
}

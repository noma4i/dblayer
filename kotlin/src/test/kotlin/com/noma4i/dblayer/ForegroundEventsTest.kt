package com.noma4i.dblayer

import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.LifecycleRegistry
import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

class ForegroundEventsTest {
    private val owner = object : LifecycleOwner {
        override val lifecycle: LifecycleRegistry = LifecycleRegistry.createUnsafe(this)
    }

    private fun returnToForeground() {
        owner.lifecycle.currentState = Lifecycle.State.CREATED
        owner.lifecycle.currentState = Lifecycle.State.STARTED
    }

    @Test
    fun aReturnToTheForegroundReachesTheCoreUntilTheRegistrationEnds() = bounded {
        val transport = MockTransport()
        repeat(8) { transport.enqueue("MomentsFeed", DbException.Http(400u, "bad request")) }
        openSigned(Config(null, null), modelsJSON.replace("\"name\":\"feed\",\"updates\"", "\"name\":\"feed\",\"refresh_on_foreground\":true,\"updates\""), transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
        val scope = CoroutineScope(SupervisorJob())
        val list = ListReader(Moment.feed, scope)
        withTimeout(5000) { list.snapshot.first { it.phase == LoadPhase.Failed } }
        owner.lifecycle.currentState = Lifecycle.State.CREATED
        val sink = DBlayer.database.onlineSink()
        val delivered = java.util.concurrent.atomic.AtomicInteger()
        val stop = ForegroundEvents.observe(owner.lifecycle, post = { it() }) {
            delivered.incrementAndGet()
            sink.enterForeground()
        }
        val before = transport.calls.size

        owner.lifecycle.currentState = Lifecycle.State.STARTED
        assertEquals(1, delivered.get())
        eventually("the delivered start reloads the failed window") { transport.calls.size > before }

        stop()
        returnToForeground()
        assertEquals(1, delivered.get())
        scope.cancel()
    }

    /** boundary_observer_stop_orders (P-BND-09, D-177): a registration queued on the main thread and stopped before it ran never delivers, and the stop needs no main thread. */
    @Test
    fun aStopBeforeTheQueuedRegistrationRanVoidsIt() {
        owner.lifecycle.currentState = Lifecycle.State.CREATED
        val queue = java.util.concurrent.LinkedBlockingQueue<() -> Unit>()
        val delivered = java.util.concurrent.atomic.AtomicInteger()
        val stop = ForegroundEvents.observe(owner.lifecycle, post = { queue.put(it) }) { delivered.incrementAndGet() }
        stop()
        while (queue.isNotEmpty()) queue.take()()
        owner.lifecycle.currentState = Lifecycle.State.STARTED
        assertEquals(0, delivered.get())
    }
}

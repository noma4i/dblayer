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

    /** The OS of the test: the observer the core starts reports online and a foreground registration, and the main thread is a queue the test drains. */
    private inner class FakeOs : PlatformObserver {
        val queue = java.util.concurrent.LinkedBlockingQueue<() -> Unit>()
        @Volatile var sink: OnlineSink? = null
        @Volatile var registration = 0uL
        private var stopObserving: () -> Unit = {}

        override fun start(sink: OnlineSink) {
            this.sink = sink
            sink.setOnline(true)
            registration = sink.registerForeground()
            stopObserving = ForegroundEvents.observe(owner.lifecycle, post = { queue.put(it) }) { sink.enterForeground(registration) }
        }

        override fun stop() = stopObserving()

        fun runMain() { while (queue.isNotEmpty()) queue.take()() }
    }

    private suspend fun openFailedFeed(transport: MockTransport, os: FakeOs): ListReader<Moment> {
        repeat(16) { transport.enqueue("MomentsFeed", DbException.Http(400u, "bad request")) }
        openSignedClocked(modelsJSON.replace("\"name\":\"feed\",\"updates\"", "\"name\":\"feed\",\"refresh_on_foreground\":true,\"updates\""), transport, FEED_OPERATIONS, observers = os)
        os.runMain()
        val list = ListReader(Moment.feed, CoroutineScope(SupervisorJob()))
        withTimeout(5000) { list.snapshot.first { it.phase == LoadPhase.Failed } }
        return list
    }

    private val FEED_OPERATIONS = """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"""

    /** boundary_observer_stop_orders (P-BND-09, D-177), delivery before stop: a start of the process lifecycle reaches the core with the token the core issued. */
    @Test
    fun aDeliveryOfTheLiveRegistrationReloadsTheFailedWindow() = bounded {
        val transport = MockTransport()
        val os = FakeOs()
        openFailedFeed(transport, os)
        owner.lifecycle.currentState = Lifecycle.State.CREATED
        val before = transport.calls.size

        owner.lifecycle.currentState = Lifecycle.State.STARTED
        eventually("the delivery reloads the failed window") { transport.calls.size == before + 1 }
    }

    /** boundary_observer_stop_orders, stop before delivery: after `close` acknowledged the stop of the observers, a start of the lifecycle and a delivery with the old token reach nothing. */
    @Test
    fun noDeliveryReachesTheCoreAfterTheStopOfTheObserversIsAcknowledged() = bounded {
        val transport = MockTransport()
        val os = FakeOs()
        openFailedFeed(transport, os)
        owner.lifecycle.currentState = Lifecycle.State.CREATED
        val sink = checkNotNull(os.sink)
        val registration = os.registration
        val before = transport.calls.size

        DBlayer.close()
        owner.lifecycle.currentState = Lifecycle.State.STARTED
        os.runMain()
        sink.enterForeground(registration)
        assertEquals(before, transport.calls.size)
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

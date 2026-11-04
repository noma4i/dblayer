package com.noma4i.dblayer

import com.noma4i.dblayer.models.Container
import com.noma4i.dblayer.models.CurrentUser
import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

@OptIn(DBlayerTestingApi::class)
class ProjectedLoadedTest {
    /** The order of the app: a plain transport, no sign-in call, a held stream, `loaded()`, then the value at once. */
    @Test
    fun theValueOfAHeldStreamIsThereWhenLoadedReturnsAndARefreshStillAsksTheServer() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            DBlayer.close()
            val transport = MockTransport()
            repeat(2) { transport.enqueue("CurrentUserQuery", """{"data":{"user":{"id":"u1","name":"u1"}}}""") }
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]""")
            val held = Projected(AccountScope.current().stream(CurrentUser.current), CurrentUser, scope)

            held.loaded()
            assertEquals("u1", held.value?.id)
            held.loaded()
            assertEquals("u1", held.value?.id)
            assertEquals(1, transport.calls.count { it.operationName == "CurrentUserQuery" })
            CurrentUser.current.refresh()
            assertEquals(2, transport.calls.count { it.operationName == "CurrentUserQuery" })
            held.close()
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }

    /** Requirement: a delivery that applied no rows (a count of missed events, a delta that waits for the snapshot, a state after a gap) does not make the rows of its number visible: after a gap only the recovery snapshot does. */
    @Test
    fun aProjectionShowsTheRowsOfADeliveryOnlyWhenItAppliedThem() {
        val state = StreamState(LivePhase.LOADED, false, false, false, false, true, null, null, null, null)
        fun delta(base: Long, revision: Long) = StreamValue.Delta(base, revision, 0u, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), state)
        val steps = listOf(
            Triple(1uL, StreamValue.Snapshot(1, emptyList(), state), 1uL),
            Triple(2uL, delta(1, 2), 2uL),
            Triple(9uL, StreamValue.Missed(6uL), 2uL),
            Triple(9uL, delta(2, 3), 2uL),
            Triple(10uL, delta(3, 4), 2uL),
            Triple(11uL, StreamValue.State(state), 2uL),
            Triple(12uL, StreamValue.Snapshot(5, emptyList(), state), 12uL),
            Triple(13uL, StreamValue.Ended(StreamEnd.CLOSED), 12uL),
        )
        var projection = ListProjection<Moment>()
        for ((seq, value, shown) in steps) {
            projection = projection.apply(StreamDelivery(1uL, 1uL, seq, value), Moment).projection
            assertEquals("after $value at $seq", shown, projection.shownSeq)
        }
    }

    /** Requirement: when the first deliveries are a count of missed events and a state (the snapshot was dropped), nothing is shown and the recovery snapshot is asked once; the snapshot then shows its rows. */
    @Test
    fun aMissedThenAStateBeforeAnySnapshotShowsNothingAndAsksTheRecoverySnapshot() {
        val state = StreamState(LivePhase.LOADED, false, false, false, false, true, null, null, null, null)
        val steps = listOf(
            Triple(StreamValue.Missed(5uL) as StreamValue, 0uL, Applied.Ignored),
            Triple(StreamValue.State(state) as StreamValue, 6uL, Applied.Resync),
            Triple(StreamValue.State(state) as StreamValue, 7uL, Applied.Changed),
            Triple(StreamValue.Snapshot(2, emptyList(), state) as StreamValue, 8uL, Applied.Changed),
        )
        val shown = listOf(null, null, null, 8uL)
        var projection = ListProjection<Moment>()
        for ((index, step) in steps.withIndex()) {
            val (value, seq, applied) = step
            val result = projection.apply(StreamDelivery(1uL, 1uL, seq, value), Moment)
            projection = result.projection
            assertEquals("applied after $value", applied, result.applied)
            assertEquals("shown after $value", shown[index], projection.shownSeq)
        }
    }

    /** Requirement: `loaded()` throws `Interrupted` when the reader ends before the projection showed the rows of the loaded revision. The reader cannot run until the stream is closed. */
    @Test
    fun loadedThrowsWhenTheReaderEndsBeforeItShowedTheRows() = bounded {
        val gate = CountDownLatch(1)
        val executor = Executors.newSingleThreadExecutor()
        val blocked = CoroutineScope(SupervisorJob() + executor.asCoroutineDispatcher())
        try {
            DBlayer.close()
            val transport = MockTransport()
            transport.enqueue("CurrentUserQuery", """{"data":{"user":{"id":"u1","name":"u1"}}}""")
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]""")
            executor.execute { gate.await() }
            val held = Projected(AccountScope.current().stream(CurrentUser.current), CurrentUser, blocked)
            held.stream.loaded()
            val waiting = async(Dispatchers.Default) { runCatching { held.loaded() }.exceptionOrNull() }
            held.close()
            gate.countDown()
            assertTrue("loaded() ended with ${waiting.await()}", waiting.await() is DbException.Interrupted)
            assertNull(held.value)
        } finally {
            gate.countDown()
            blocked.cancel()
            executor.shutdown()
            DBlayer.close()
        }
    }

    /** Requirement: for a stream of one entity that the library loads from the server, the row is visible when `loaded()` returns. */
    @Test
    fun theRowOfAnEntityStreamThatLoadsFromTheServerIsThereWhenLoadedReturns() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            DBlayer.close()
            val transport = MockTransport()
            transport.enqueue("ContainerNode", """{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}""")
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"ContainerNode","document":"query ContainerNode(${'$'}id: ID!) { node(id: ${'$'}id) { ... on Container { id title shareUrl isPublic } } }"}]""")
            val held = Projected(AccountScope.current().entity(Container, "c1", EntityFetch.FORCE), Container, scope)

            held.loaded()
            assertEquals("one", held.value?.title)
            held.close()
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }
}

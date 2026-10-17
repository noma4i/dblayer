package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Test

class CableIdleTest {
    @Test
    fun aConnectionThatReceivesFramesWakesNoTimer() = bounded {
        val clock = ManualTimers()
        val sockets = mutableListOf<TestCableSocket>()
        val connection = CableConnection("/test", "token", { _, _ -> TestCableSocket(emptyList()).also { sockets.add(it) } }, clock.timers)
        connection.open()
        sockets[0].deliver("""{"type":"welcome"}""")
        eventually("the welcome frame") { connection.lastFrameAt != null }
        eventually("the welcome deadline") { clock.pendingDues() == listOf(CableHeartbeat.STALE_AFTER) }

        for (step in 1..10) {
            clock.advance(3.0)
            sockets[0].deliver("""{"type":"ping","message":$step}""")
            eventually("frame $step") { connection.lastFrameAt == 3.0 * step }
            eventually("the deadline of frame $step") { clock.pendingDues() == listOf(3.0 * step + CableHeartbeat.STALE_AFTER) }
        }

        assertEquals(0, clock.fired)
        assertEquals(1, sockets.size)

        clock.advance(CableHeartbeat.STALE_AFTER)
        eventually("the reopen delay") { clock.pending(CableHeartbeat.REOPEN_DELAY) == 1 }
        clock.advance(CableHeartbeat.REOPEN_DELAY)
        eventually("the reopen after silence") { sockets.size == 2 }
        connection.close()
        Unit
    }

    @Test
    fun failedOpensBackOffByTheFormulaAndAWelcomeResetsIt() = bounded {
        val clock = ManualTimers()
        val sockets = mutableListOf<CableSocket>()
        val factory: (String, String) -> CableSocket = { _, _ ->
            (if (sockets.size < 3) FailingSocket() else TestCableSocket(emptyList())).also { sockets.add(it) }
        }
        val connection = CableConnection("/test", "token", factory, clock.timers)
        connection.open()

        val waits = mutableListOf<Double>()
        repeat(3) {
            eventually("the next reconnect deadline") { clock.pendingSeconds().any { it != CableHeartbeat.REOPEN_DELAY } }
            val wait = clock.pendingSeconds().first { it != CableHeartbeat.REOPEN_DELAY }
            waits.add(wait)
            clock.advance(wait)
            eventually("the reopen delay") { clock.pending(CableHeartbeat.REOPEN_DELAY) == 1 }
            clock.advance(CableHeartbeat.REOPEN_DELAY)
        }

        assertEquals((0 until 3).map { CableHeartbeat.pollInterval(it, 0.0) }, waits)
        eventually("the open that succeeds") { connection.state == CableConnectionState.Open }
        (sockets.last() as TestCableSocket).deliver("""{"type":"welcome"}""")
        eventually("the welcome frame") { connection.lastFrameAt != null }
        eventually("the stale deadline after the welcome") { clock.pendingSeconds() == listOf(CableHeartbeat.STALE_AFTER) }

        (sockets.last() as TestCableSocket).close()
        eventually("the closed socket") { connection.state == CableConnectionState.Closed }
        eventually("the reconnect deadline") { clock.pendingSeconds().size == 1 }
        assertEquals(listOf(CableHeartbeat.pollInterval(0, 0.0)), clock.pendingSeconds())
        connection.close()
        Unit
    }

    @Test
    fun aWallClockThatMovedBackDoesNotHideSilence() = bounded {
        val clock = ManualTimers()
        val sockets = mutableListOf<TestCableSocket>()
        val connection = CableConnection("/test", "token", { _, _ -> TestCableSocket(emptyList()).also { sockets.add(it) } }, clock.timers)
        connection.open()
        eventually("the deadline of the open") { clock.pendingDues() == listOf(CableHeartbeat.STALE_AFTER) }
        clock.advance(1.0)
        val seenAtPublish = java.util.concurrent.CopyOnWriteArrayList<List<Double>>()
        clock.nowHook = { seenAtPublish.add(clock.pendingDues()) }
        sockets[0].deliver("""{"type":"welcome"}""")
        eventually("the welcome frame") { connection.lastFrameAt != null }
        clock.nowHook = null
        assertEquals(0, clock.fired)
        assertEquals(true, seenAtPublish.isNotEmpty())
        assertEquals(emptyList<List<Double>>(), seenAtPublish.filter { it.contains(CableHeartbeat.STALE_AFTER) })
        eventually("the deadline of the frame") { clock.pendingDues() == listOf(1.0 + CableHeartbeat.STALE_AFTER) }
        clock.skewWallClock(-100.0)

        clock.advance(CableHeartbeat.STALE_AFTER)
        eventually("the reopen delay") { clock.pending(CableHeartbeat.REOPEN_DELAY) == 1 }
        clock.advance(CableHeartbeat.REOPEN_DELAY)
        eventually("the reopen after silence") { sockets.size == 2 }

        assertEquals(2, clock.fired)
        eventually("the deadline of the new socket") { clock.pendingSeconds() == listOf(CableHeartbeat.STALE_AFTER) }
        connection.close()
        Unit
    }
}

private class FailingSocket : CableSocket {
    override suspend fun open() = throw CableSocketException(CableSocketException.Reason.Closed)
    override fun close() {}
    override suspend fun send(text: String) {}
    override suspend fun receive(): String = throw CableSocketException(CableSocketException.Reason.Closed)
}

package com.noma4i.dblayer

import kotlinx.coroutines.CompletableDeferred
import org.junit.Assert.assertEquals
import org.junit.Test

private class ManualTimers {
    private class Sleeper(val due: Double, val seconds: Double, val done: CompletableDeferred<Unit>)

    private val lock = Any()
    private var elapsed = 0.0
    private var skew = 0.0
    private val sleepers = mutableListOf<Sleeper>()
    var fired = 0
        private set

    val timers = CableTimers(
        now = { synchronized(lock) { elapsed + skew } },
        sleep = { seconds ->
            val sleeper = Sleeper(synchronized(lock) { elapsed } + seconds, seconds, CompletableDeferred())
            synchronized(lock) { sleepers.add(sleeper) }
            try {
                sleeper.done.await()
            } finally {
                synchronized(lock) { sleepers.remove(sleeper) }
            }
        },
        jitter = { 0.0 },
    )
    fun advance(seconds: Double) {
        val due = synchronized(lock) {
            elapsed += seconds
            sleepers.filter { it.due <= elapsed }.also { fired += it.size }
        }
        due.forEach { it.done.complete(Unit) }
    }

    fun skewWallClock(seconds: Double) = synchronized(lock) { skew += seconds }

    fun pendingSeconds(): List<Double> = synchronized(lock) { sleepers.map { it.seconds } }

    fun pending(seconds: Double): Int = synchronized(lock) { sleepers.count { it.seconds == seconds } }
}

class CableIdleTest {
    @Test
    fun aConnectionThatReceivesFramesWakesNoTimer() = bounded {
        val clock = ManualTimers()
        val sockets = mutableListOf<TestCableSocket>()
        val connection = CableConnection("/test", "token", { _, _ -> TestCableSocket(emptyList()).also { sockets.add(it) } }, clock.timers)
        connection.open()
        sockets[0].deliver("""{"type":"welcome"}""")
        eventually("the welcome frame") { connection.lastFrameAt != null }

        for (step in 1..10) {
            clock.advance(3.0)
            sockets[0].deliver("""{"type":"ping","message":$step}""")
            eventually("frame $step") { connection.lastFrameAt == 3.0 * step }
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
        assertEquals(listOf(CableHeartbeat.STALE_AFTER), clock.pendingSeconds())
        connection.close()
        Unit
    }

    @Test
    fun aWallClockThatMovedBackDoesNotHideSilence() = bounded {
        val clock = ManualTimers()
        val sockets = mutableListOf<TestCableSocket>()
        val connection = CableConnection("/test", "token", { _, _ -> TestCableSocket(emptyList()).also { sockets.add(it) } }, clock.timers)
        connection.open()
        sockets[0].deliver("""{"type":"welcome"}""")
        eventually("the welcome frame") { connection.lastFrameAt != null }
        clock.skewWallClock(-100.0)

        clock.advance(CableHeartbeat.STALE_AFTER)
        eventually("the reopen delay") { clock.pending(CableHeartbeat.REOPEN_DELAY) == 1 }
        clock.advance(CableHeartbeat.REOPEN_DELAY)
        eventually("the reopen after silence") { sockets.size == 2 }

        assertEquals(2, clock.fired)
        assertEquals(listOf(CableHeartbeat.STALE_AFTER), clock.pendingSeconds())
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

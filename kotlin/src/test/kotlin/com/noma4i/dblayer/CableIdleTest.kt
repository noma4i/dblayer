package com.noma4i.dblayer

import kotlinx.coroutines.CompletableDeferred
import org.junit.Assert.assertEquals
import org.junit.Test

private class ManualTimers {
    private class Sleeper(val due: Double, val seconds: Double, val done: CompletableDeferred<Unit>)

    private val lock = Any()
    private var elapsed = 0.0
    private val sleepers = mutableListOf<Sleeper>()
    var fired = 0
        private set

    val timers = CableTimers(
        now = { synchronized(lock) { elapsed } },
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
}

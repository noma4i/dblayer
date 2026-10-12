package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList

private const val WELCOME_FRAME = """{"type":"welcome"}"""

private fun confirmed(identifier: String) = """{"type":"confirm_subscription","identifier":${CableCommand.quoted(identifier)}}"""

private fun TestCableSocket.pings() = texts().count { it.contains("ping") }

private class LifecycleRig {
    val clock = ManualTimers()
    val sockets = CopyOnWriteArrayList<TestCableSocket>()
    val calls = CopyOnWriteArrayList<String>()
    val client = CableClient("/test", "token", { _, _ -> TestCableSocket(emptyList()).also { sockets.add(it) } }, { "abc" }, 3600.0, clock.timers)
    val lifecycle = AppLifecycle(client, { calls.add("presence $it") }, { calls.add("sync $it") })
    val restore = NetworkRestore(client, { calls.add("sync $it") }, clock.timers)
}

class CableLifecycleTest {
    @Test
    fun backgroundPausesThePingAndForegroundResumesItWithoutAReconnect() = bounded {
        val rig = LifecycleRig()
        rig.client.openPresence("54")
        val start = launch { rig.client.start() }
        eventually("the socket") { rig.sockets.size == 1 }
        rig.sockets[0].deliver(WELCOME_FRAME)
        eventually("the presence subscribe") { rig.sockets[0].texts().size == 1 }
        rig.sockets[0].deliver(confirmed(OnlineChannel.identifier("54")))
        eventually("the first ping") { rig.sockets[0].pings() == 1 && rig.clock.pending(3600.0) == 1 }

        rig.lifecycle.phaseChanged(AppLifecycle.Phase.Inactive)
        rig.lifecycle.phaseChanged(AppLifecycle.Phase.Background)
        rig.lifecycle.phaseChanged(AppLifecycle.Phase.Background)
        eventually("the offline report") { rig.calls.isNotEmpty() }
        assertEquals(listOf("presence Offline"), rig.calls.toList())
        assertFalse(rig.sockets[0].isClosed)
        assertEquals(0, rig.clock.pending(3600.0))

        val active = launch { rig.lifecycle.phaseChanged(AppLifecycle.Phase.Active) }
        eventually("the online report and the sync") { rig.calls.toSet().containsAll(listOf("presence Online", "sync true")) }
        eventually("the ping again") { rig.sockets[0].pings() == 2 && rig.clock.pending(3600.0) == 1 }
        eventually("the foreground check") { rig.clock.pending(CableHeartbeat.FOREGROUND_DELAY) == 1 }
        rig.clock.advance(CableHeartbeat.FOREGROUND_DELAY)
        active.join()

        assertEquals(setOf("presence Offline", "presence Online", "sync true"), rig.calls.toSet())
        assertEquals(1, rig.sockets.size)
        rig.client.stop()
        start.join()
    }

    @Test
    fun foregroundAfterALostSocketReconnectsOnceAndSyncs() = bounded {
        val rig = LifecycleRig()
        rig.client.subscribeGraphQL("subscription X { x }", "X")
        val start = launch { rig.client.start() }
        eventually("the socket") { rig.sockets.size == 1 }
        rig.sockets[0].deliver(WELCOME_FRAME)
        eventually("the subscribe") { rig.sockets[0].texts().size == 1 }

        rig.lifecycle.phaseChanged(AppLifecycle.Phase.Background)
        rig.sockets[0].close()
        eventually("the lost socket") { !rig.client.isConnected() }

        val active = launch { rig.lifecycle.phaseChanged(AppLifecycle.Phase.Active) }
        eventually("the merge delay") { rig.clock.pending(CableReconnect.MERGE_DELAY) == 1 }
        assertFalse(rig.calls.contains("sync true"))
        assertEquals(1, rig.sockets.size)
        rig.clock.advance(CableReconnect.MERGE_DELAY)
        active.join()

        assertEquals(2, rig.sockets.size)
        assertTrue(rig.calls.contains("sync true"))
        assertEquals(listOf("app_foregrounded"), rig.clock.recycles.map { it.cause })
        rig.client.stop()
        start.join()
    }

    private suspend fun CoroutineScope.restoreSequence(socketLost: Boolean) {
        val rig = LifecycleRig()
        rig.client.subscribeGraphQL("subscription X { x }", "X")
        val start = launch { rig.client.start() }
        eventually("the socket") { rig.sockets.size == 1 }
        rig.sockets[0].deliver(WELCOME_FRAME)
        eventually("the subscribe") { rig.sockets[0].texts().size == 1 }
        if (socketLost) {
            rig.sockets[0].close()
            eventually("the lost socket") { !rig.client.isConnected() }
        }

        rig.restore.update(false, true)
        rig.restore.update(true, true)
        eventually("the alive delay") { rig.clock.pending(NetworkRestore.ALIVE_DELAY) == 1 }
        assertEquals(1, rig.sockets.size)
        rig.clock.advance(NetworkRestore.ALIVE_DELAY)
        eventually("the foreground check and the sync delay") {
            rig.clock.pending(CableHeartbeat.FOREGROUND_DELAY) == 1 && rig.clock.pending(NetworkRestore.SYNC_DELAY) == 1
        }
        rig.clock.advance(CableHeartbeat.FOREGROUND_DELAY)
        if (socketLost) eventually("the reopened socket") { rig.sockets.size == 2 }
        assertTrue(rig.calls.isEmpty())
        rig.clock.advance(NetworkRestore.SYNC_DELAY - CableHeartbeat.FOREGROUND_DELAY)
        eventually("the sync") { rig.calls.isNotEmpty() }

        assertEquals(if (socketLost) 2 else 1, rig.sockets.size)
        assertEquals(listOf("sync false"), rig.calls.toList())
        rig.client.stop()
        start.join()
    }

    @Test
    fun networkBackWithALiveSocketChecksItThenSyncsOnce() = bounded {
        restoreSequence(socketLost = false)
        Unit
    }

    @Test
    fun networkBackWithALostSocketReopensItThenSyncsOnce() = bounded {
        restoreSequence(socketLost = true)
        Unit
    }

    @Test
    fun networkBackWithoutSubscriptionsDoesNothingAndANewOfflineCancelsTheRestore() = bounded {
        val rig = LifecycleRig()
        rig.restore.update(false, false)
        rig.restore.update(true, false)
        assertEquals(0, rig.clock.pending(NetworkRestore.ALIVE_DELAY))

        rig.restore.update(false, true)
        rig.restore.update(true, true)
        eventually("the alive delay") { rig.clock.pending(NetworkRestore.ALIVE_DELAY) == 1 }
        rig.restore.update(false, true)
        eventually("the cancelled delay") { rig.clock.pending(NetworkRestore.ALIVE_DELAY) == 0 }
        assertTrue(rig.calls.isEmpty())
        Unit
    }
}

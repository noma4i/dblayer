package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

internal fun finishesElsewhere(probe: () -> Unit): Boolean {
    val done = CountDownLatch(1)
    Thread { probe(); done.countDown() }.apply { isDaemon = true }.start()
    return done.await(10, TimeUnit.SECONDS)
}

class PlatformLockExitTest {
    @Test
    fun theOnlineRelayDeliversToItsReceiversWithNoRelayLockHeld() {
        val seen = CopyOnWriteArrayList<String>()
        val probes = CopyOnWriteArrayList<Boolean>()
        lateinit var relay: OnlineRelay
        relay = OnlineRelay { online ->
            seen += "connectivity:$online"
            if (online) probes += finishesElsewhere { relay.set(false) }
        }
        relay.attach { seen += "sink:$it" }
        relay.set(true)

        assertEquals(listOf(true), probes.toList())
        assertEquals("sink:false", seen.last())
        assertEquals("connectivity:false", seen.last { it.startsWith("connectivity") })
    }
}

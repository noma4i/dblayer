package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
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
    private fun wire(id: String) = WireRow(id, 1L, 1uL, Moment.layout,
        listOf(Cell.Text(id), Cell.Null, Cell.Text(id), Cell.Bool(false), Cell.Keyed(emptyList()), Cell.Null, Cell.Text("x")),
        WireSync(WireSyncState.SYNCED, emptyList(), emptyList(), null, 0u, 0L), emptyList())

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

    @Test
    fun aMergeRunsWithNoLatestLockHeldAndALateWriterIsNotLost() = bounded {
        val probes = CopyOnWriteArrayList<Boolean>()
        lateinit var latest: Latest<Int>
        latest = Latest(merge = { older, newer ->
            if (probes.isEmpty()) {
                probes += finishesElsewhere { latest.hasPending }
                latest.put(100)
            }
            older + newer
        })
        latest.put(1)
        latest.put(2)

        assertEquals(listOf(true), probes.toList())
        assertEquals(103, latest.next())
    }

    @Test
    fun aRowDecoderRunsWithNoFeedLockHeld() {
        val probes = CopyOnWriteArrayList<Boolean>()
        lateinit var feed: ListFeed<Moment>
        val decoder = object : ModelDecoder<Moment> by Moment {
            override fun decode(row: WireRow): Moment {
                probes += finishesElsewhere { feed.rows }
                return Moment.decode(row)
            }
        }
        feed = ListFeed(decoder)
        val applied = feed.apply(LiveChange(1L, true, 2u, listOf(wire("a"), wire("b")), emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), null, null))

        assertEquals(listOf(true, true), probes.toList())
        assertEquals(listOf("a", "b"), applied.rows.map { it.id })
    }
}

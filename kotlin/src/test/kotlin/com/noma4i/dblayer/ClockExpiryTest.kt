package com.noma4i.dblayer

import com.noma4i.dblayer.models.Notice
import com.noma4i.dblayer.models.Presence
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(DBlayerTestingApi::class)
class ClockExpiryTest {
    @Test
    fun advancingTheTestClockExpiresAMemoryRowWithoutWaiting() = bounded {
        val transport = MockTransport()
        val clock = reopenClocked(schema("BulletinAdded"), transport, """[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]""")
        val shown = async { Notice.recent.live.flow.first { it.isNotEmpty() } }
        eventually("the notice subscription opens") { transport.calls.any { it.operationName == "BulletinAdded" } }
        transport.emit("BulletinAdded", """{"data":{"bulletinAdded":{"body":"hello","occurredAt":"2026-10-18T00:00:00Z"}}}""")
        shown.await()

        clock.advance(61_000L)

        Notice.recent.live.flow.first { it.isEmpty() }
        closeForCleanup()
    }

    @Test
    fun writingEqualValuesRenewsAMemoryRowAndStaysSilent() = bounded {
        val clock = reopenClocked(modelsJSON, MockTransport())
        val inbox = Channel<List<Presence>>(Channel.UNLIMITED)
        val collection = launch { Presence.where(Presence.room eq "lobby").live.flow.collect { inbox.send(it) } }
        assertTrue(inbox.receive().isEmpty())
        val presence = Presence.create(userId = "u1", room = "lobby").optimisticValue!!
        assertEquals(1, inbox.receive().size)

        clock.advance(40_000L)
        presence.update(room = "lobby")
        clock.advance(40_000L)

        assertEquals(presence.id, Presence.find(presence.id).current()?.id)
        clock.advance(21_000L)
        assertTrue(inbox.receive().isEmpty())
        collection.cancelAndJoin()
        closeForCleanup()
    }
}

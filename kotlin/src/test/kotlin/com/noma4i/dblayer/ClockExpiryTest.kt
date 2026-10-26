package com.noma4i.dblayer

import com.noma4i.dblayer.models.Notice
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import org.junit.Test

@OptIn(DBlayerTestingApi::class)
class ClockExpiryTest {
    @Test
    fun advancingTheTestClockExpiresAMemoryRowWithoutWaiting() = bounded {
        DBlayer.close()
        val transport = MockTransport()
        val clock = DBlayer.openTesting(transport, modelsJSON)
        DBlayer.database.registerOperations("""[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]""")
        val shown = async { Notice.recent.live.flow.first { it.isNotEmpty() } }
        eventually("the notice subscription opens") { transport.calls.size == 1 }
        transport.emit("BulletinAdded", """{"data":{"bulletinAdded":{"body":"hello","occurredAt":"2026-10-18T00:00:00Z"}}}""")
        shown.await()

        clock.advance(61_000L)

        Notice.recent.live.flow.first { it.isEmpty() }
        DBlayer.close()
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(DBlayerTestingApi::class)
class PreloadTestingTest {
    private object PresenceOnline : GraphQLOperation {
        override val operationName = "PresenceOnline"
        override val document = "query PresenceOnline { presenceOnline { id userId room } }"
    }

    @Test
    fun openTestingKeepsScopePreloadsQuietUnlessAskedAndLoadsThemWhenAsked() = bounded {
        for (preload in listOf(false, true)) {
            DBlayer.close()
            val transport = MockTransport()
            transport.enqueue("PresenceOnline", """{"data":{"presenceOnline":[]}}""")
            DBlayer.openTesting(transport, modelsJSON, operations = listOf(PresenceOnline), preload = preload)
            if (preload) eventually("the queued preload reply is consumed") { transport.calls.size == 1 } else assertEquals(0, transport.calls.size)
            assertTrue(transport.unscripted.isEmpty())
        }
        DBlayer.close()
    }
}

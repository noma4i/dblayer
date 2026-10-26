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

    private object PresenceHistory : GraphQLOperation {
        override val operationName = "PresenceHistory"
        override val document = "query PresenceHistory(\$presenceId: ID!) { presenceHistory(presenceId: \$presenceId) { id userId room } }"
    }

    private object PresenceOnChannel : GraphQLOperation {
        override val operationName = "PresenceOnChannel"
        override val document = "query PresenceOnChannel(\$channel: String!) { presenceOnChannel(channel: \$channel) { id userId room } }"
    }

    @Test
    fun refreshPreloadsReloadsEveryPreloadScopeEvenWhenItIsFresh() = bounded {
        DBlayer.close()
        val transport = MockTransport()
        val online = """{"data":{"presenceOnline":[{"id":"p1","userId":"u1","room":"a"},{"id":"p2","userId":"u2","room":"b"}]}}"""
        transport.enqueue("PresenceOnline", online)
        transport.enqueue("PresenceOnline", online)
        repeat(4) {
            transport.enqueue("PresenceHistory", """{"data":{"presenceHistory":[]}}""")
            transport.enqueue("PresenceOnChannel", """{"data":{"presenceOnChannel":[]}}""")
        }
        DBlayer.openTesting(transport, modelsJSON, operations = listOf(PresenceOnline, PresenceHistory, PresenceOnChannel), preload = true)
        eventually("the preload loads every scope") { transport.calls.size == 5 }
        DBlayer.refreshPreloads()
        assertEquals(10, transport.calls.size)
        assertTrue(transport.unscripted.isEmpty())
        DBlayer.close()
    }

    @Test
    fun aPreloadScopeDerivedFromAnotherScopeLoadsOnceForEachRowOfIt() = bounded {
        DBlayer.close()
        val transport = MockTransport()
        transport.enqueue("PresenceOnline", """{"data":{"presenceOnline":[{"id":"p1","userId":"u1","room":"a"},{"id":"p2","userId":"u2","room":"b"}]}}""")
        transport.enqueue("PresenceHistory", """{"data":{"presenceHistory":[]}}""")
        transport.enqueue("PresenceHistory", """{"data":{"presenceHistory":[]}}""")
        DBlayer.openTesting(transport, modelsJSON, operations = listOf(PresenceOnline, PresenceHistory), preload = true)
        eventually("one history load for each online row") { transport.calls.size == 3 }
        val loaded = transport.calls.filter { it.operationName == "PresenceHistory" }.map { Regex("\"presenceId\":\"([^\"]+)\"").find(it.variablesJson)?.groupValues?.get(1) }
        assertEquals(listOf("p1", "p2"), loaded.sortedBy { it })
        assertTrue(transport.unscripted.isEmpty())
        DBlayer.close()
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

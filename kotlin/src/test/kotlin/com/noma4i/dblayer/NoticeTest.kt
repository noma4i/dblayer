package com.noma4i.dblayer

import com.noma4i.dblayer.models.Notice
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class NoticeTest {
    @Test
    fun memoryNoticesReachTheGeneratedLiveQueryWithoutAReadRequest() = bounded {
        val transport = MockTransport()
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"ViewerNotice","document":"subscription ViewerNotice { viewerNotice { body occurredAt } }","rootPath":"viewerNotice"}]""")
        val notices = async { Notice.recent.live.flow.first { it.isNotEmpty() } }
        eventually("the notice subscription opens") { transport.calls.size == 1 }
        transport.emit("ViewerNotice", """{"data":{"viewerNotice":{"body":"hello","occurredAt":"2026-10-18T00:00:00Z"}}}""")
        val delivered = notices.await().first()
        assertEquals("hello", delivered.body)
        assertTrue(delivered.id.isNotEmpty())
        assertEquals(listOf("ViewerNotice"), transport.calls.map { it.operationName })
    }
}

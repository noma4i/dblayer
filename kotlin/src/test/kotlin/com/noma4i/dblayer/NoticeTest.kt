package com.noma4i.dblayer

import com.noma4i.dblayer.models.Notice
import com.noma4i.dblayer.models.ChatAction
import com.noma4i.dblayer.models.ChatActivityNotice
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class NoticeTest {
    @Test
    fun oneSignalRoutesTypedActionsAndNotices() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"ChatActionSignal","document":"subscription ChatActionSignal { chatActionSignal { chatId userId kind active } }"}]""")
        val events = ChatActivityNotice.events()
        val typing = async { ChatAction.byChat("c").live.flow.first { it.isNotEmpty() } }
        transport.emit("ChatActionSignal", """{"data":{"chatActionSignal":{"chatId":"c","userId":"u","kind":"TYPING","active":true}}}""")
        assertEquals("TYPING", typing.await().first().kind)
        transport.emit("ChatActionSignal", """{"data":{"chatActionSignal":{"chatId":"c","userId":"u","kind":"SCREENSHOT","active":true}}}""")
        val notice = (events.first() as ModelEventItem.Event).event
        assertEquals("SCREENSHOT", notice.after?.kind)
        transport.emit("ChatActionSignal", """{"data":{"chatActionSignal":{"chatId":"c","userId":"u","kind":"TYPING","active":false}}}""")
        ChatAction.byChat("c").live.flow.first { it.isEmpty() }
        assertEquals(1, transport.calls.count { it.operationName == "ChatActionSignal" })
    }

    @Test
    fun memoryNoticesReachTheGeneratedLiveQueryWithoutAReadRequest() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]""")
        val notices = async { Notice.recent.live.flow.first { it.isNotEmpty() } }
        eventually("the notice subscription opens") { transport.calls.size == 1 }
        transport.emit("BulletinAdded", """{"data":{"bulletinAdded":{"body":"hello","occurredAt":"2026-10-18T00:00:00Z"}}}""")
        val delivered = notices.await().first()
        assertEquals("hello", delivered.body)
        assertTrue(delivered.id.isNotEmpty())
        assertEquals(listOf("BulletinAdded"), transport.calls.map { it.operationName })
    }
}

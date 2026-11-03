package com.noma4i.dblayer

import com.noma4i.dblayer.models.Chat
import com.noma4i.dblayer.models.Message
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.flow.first
import org.junit.Assert.assertEquals
import org.junit.Test

class EventPayloadTest {
    @Test
    fun chatEventPayloadPlatforms() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"ChatList","document":"query ChatList { chats { id title revision lists } }"},{"name":"ChatNode","document":"query ChatNode { node { ... on Chat { id title revision lists } } }"},{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId chat { id title revision lists } message { id text chatId } } } }"}]""")
        transport.enqueue("ChatList", """{"data":{"chats":[]}}""")
        val list = Chat.list("PRIMARY")
        list.load()
        transport.enqueue("ChatNode", """{"data":{"node":{"__typename":"Chat","id":"c1","title":"before","revision":9007199254740993,"lists":["SECONDARY"]}}}""")
        assertEquals(9007199254740993L, Chat.node("c1", force = true)?.serverRevision)
        val scope = AccountScope.current()
        val chats = scope.events(Chat).events(Chat)
        val messages = scope.events(Message).events(Message)
        transport.emit("RecordEvents", """{"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"m1","chat":{"id":"c1","title":"after","revision":"9007199254740994","lists":["PRIMARY"]},"message":{"id":"m1","text":"received","chatId":"c1"}}]}}}""")
        val changed = (chats.first() as ModelEventItem.Event).event
        val received = (messages.first() as ModelEventItem.Event).event
        assertEquals("c1", changed.id)
        assertEquals("before", changed.before?.title)
        assertEquals("after", changed.after?.title)
        assertEquals(9007199254740994L, changed.after?.serverRevision)
        assertEquals("received", received.after?.text)
        assertEquals("c1", received.after?.chatId)
        val rows = list.live.current()
        assertEquals(listOf("c1"), rows.map { it.id })
        assertEquals("after", rows.first().title)
        assertEquals(listOf("ChatList", "ChatNode", "RecordEvents"), transport.calls.map { it.operationName })
    }
}

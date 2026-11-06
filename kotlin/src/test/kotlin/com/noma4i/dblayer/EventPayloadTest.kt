package com.noma4i.dblayer

import com.noma4i.dblayer.models.Chat
import com.noma4i.dblayer.models.Message
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.junit.Assert.assertEquals
import org.junit.Test

class EventPayloadTest {
    @Test
    fun chatEventPayloadPlatforms() = bounded {
        for (eventsFirst in listOf(false, true)) {
            DBlayer.close()
            val screen = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try { checkOrder(screen, eventsFirst) } finally { screen.cancel(); DBlayer.close() }
        }
    }

    private fun <Model : DBModel> CoroutineScope.hear(stream: ValueStream, decoder: ModelDecoder<Model>): Channel<ModelEventItem<Model>> {
        val heard = Channel<ModelEventItem<Model>>(Channel.UNLIMITED)
        launch { stream.events(decoder).collect { heard.trySend(it) } }
        return heard
    }

    /** Collectors of chat and message events whose sources are open. */
    private suspend fun opened(screen: CoroutineScope, scope: AccountScope): Pair<Channel<ModelEventItem<Chat>>, Channel<ModelEventItem<Message>>> {
        val chats = screen.hear(scope.events(Chat), Chat)
        val messages = screen.hear(scope.events(Message), Message)
        assertEquals(ModelEventItem.Ready, awaitWithin("the chat events source opens") { chats.receive() })
        assertEquals(ModelEventItem.Ready, awaitWithin("the message events source opens") { messages.receive() })
        return chats to messages
    }

    private suspend fun checkOrder(screen: CoroutineScope, eventsFirst: Boolean) {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport, """[{"name":"ChatList","document":"query ChatList { chats { id title revision lists } }"},{"name":"ChatNode","document":"query ChatNode { node { ... on Chat { id title revision lists } } }"},{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId chat { id title revision lists } message { id text chatId } } } }"}]""")
        val scope = AccountScope.current()
        var streams: Pair<Channel<ModelEventItem<Chat>>, Channel<ModelEventItem<Message>>>? = null
        if (eventsFirst) streams = opened(screen, scope)
        transport.enqueue("ChatList", """{"data":{"chats":[]}}""")
        val list = Chat.list("PRIMARY")
        list.load()
        val secondary = if (eventsFirst) {
            transport.enqueue("ChatList", """{"data":{"chats":[{"__typename":"Chat","id":"c1","title":"before","revision":9007199254740993,"lists":["SECONDARY"]}]}}""")
            Chat.list("SECONDARY").also { it.load() }
        } else null
        transport.enqueue("ChatNode", """{"data":{"node":{"__typename":"Chat","id":"c1","title":"before","revision":9007199254740993,"lists":["SECONDARY"]}}}""")
        assertEquals(9007199254740993L, Chat.node("c1", force = true)?.serverRevision)
        if (secondary != null) assertEquals(listOf("c1"), secondary.read().map { it.id })
        if (!eventsFirst) streams = opened(screen, scope)
        val (chats, messages) = requireNotNull(streams)
        transport.emit("RecordEvents", """{"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"m1","chat":{"id":"c1","title":"after","revision":"9007199254740994","lists":["PRIMARY"]},"message":{"id":"m1","text":"received","chatId":"c1"}}]}}}""")
        val changed = (awaitWithin("the chat event") { chats.receive() } as ModelEventItem.Event).event
        val received = (awaitWithin("the message event") { messages.receive() } as ModelEventItem.Event).event
        assertEquals("c1", changed.id)
        assertEquals("before", changed.before?.title)
        assertEquals("after", changed.after?.title)
        assertEquals(9007199254740994L, changed.after?.serverRevision)
        assertEquals("received", received.after?.text)
        assertEquals("c1", received.after?.chatId)
        val rows = list.live.current()
        assertEquals(listOf("c1"), rows.map { it.id })
        assertEquals("after", rows.first().title)
        if (secondary != null) assertEquals(emptyList<String>(), secondary.read().map { it.id })
        val expected = if (eventsFirst) listOf("RecordEvents", "ChatList", "ChatList", "ChatNode") else listOf("ChatList", "ChatNode", "RecordEvents")
        assertEquals(expected, transport.calls.map { it.operationName })
    }
}

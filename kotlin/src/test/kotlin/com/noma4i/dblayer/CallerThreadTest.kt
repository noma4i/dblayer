package com.noma4i.dblayer

import com.noma4i.dblayer.models.Chat
import com.noma4i.dblayer.models.ChatActivityNotice
import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Test
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors

/** Property P-BR-08: the library decodes a model on its own thread, never on the thread of the collector or of the caller. The observable is the thread on which the decoder of the app runs. */
class CallerThreadTest {
    private val executor = Executors.newSingleThreadExecutor { Thread(it, "caller-main") }
    private val caller = executor.asCoroutineDispatcher()
    private val decoded = ConcurrentLinkedQueue<String>()

    @After fun stop() = caller.close()

    private fun <Model : DBModel> recording(real: ModelDecoder<Model>): ModelDecoder<Model> = object : ModelDecoder<Model> by real {
        override fun decode(row: WireRow): Model {
            decoded.add(Thread.currentThread().name)
            return real.decode(row)
        }
    }

    private fun assertDecodedOffTheCaller(case: String) {
        assertEquals("$case: the decoder never ran", false, decoded.isEmpty())
        assertEquals("$case: the decoder ran on the thread of the caller: ${decoded.toList()}", false, decoded.any { it.startsWith("caller-main") })
    }

    /** Requirement: every entry that hands a model of the app to the caller decodes it off the caller thread: the rows of a stream, the events and the value of a write receipt. Observable: the thread of the decoder and the value the caller receives. */
    @Test
    fun everyEntryDecodesTheModelOffTheCallerThread() = bounded {
        val transport = MockTransport()
        openSigned(Config(null), modelsJSON, transport, """[{"name":"ChatActionSignal","document":"subscription ChatActionSignal { chatActionSignal { chatId userId kind active } }"},{"name":"ChatList","document":"query ChatList { chats { id title revision lists } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"}]""")
        val screen = CoroutineScope(SupervisorJob() + caller)
        try {
            decoded.clear()
            transport.enqueue("ChatList", """{"data":{"chats":[{"__typename":"Chat","id":"c1","title":"t","revision":1,"lists":["SECONDARY"]}]}}""")
            val projected = Projected(AccountScope.current().stream(Chat.list("SECONDARY")), recording(Chat), screen)
            screen.launch { Chat.list("SECONDARY").load() }
            projected.until("the collector received the row") { it.rows.any { row -> row.id == "c1" } }
            assertDecodedOffTheCaller("Projected")

            decoded.clear()
            val heard = Channel<ModelEventItem<ChatActivityNotice>>(Channel.UNLIMITED)
            screen.launch { AccountScope.current().events(ChatActivityNotice).events(recording(ChatActivityNotice)).collect { heard.trySend(it) } }
            assertEquals(ModelEventItem.Ready, awaitWithin("the events stream opens") { heard.receive() })
            eventually("the signal subscription opens") { transport.calls.any { it.operationName == "ChatActionSignal" } }
            transport.emit("ChatActionSignal", """{"data":{"chatActionSignal":{"chatId":"c1","userId":"u","kind":"SCREENSHOT","active":true}}}""")
            val item = awaitWithin("the event reaches the collector") { heard.receive() }
            assertEquals("c1", ((item as ModelEventItem.Event<*>).event.after as ChatActivityNotice).chatId)
            assertDecodedOffTheCaller("events")

            decoded.clear()
            transport.enqueue("CreateMoment", """{"data":{"createMoment":{"id":"1"}}}""")
            val sent = withContext(caller) { AccountScope.current().send(recording(Moment), buildJsonObject { put("text", "hello") }) }
            assertNotEquals("the receipt carries no optimistic value", null, sent.optimisticValue)
            assertDecodedOffTheCaller("send")
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Chat
import com.noma4i.dblayer.models.ChatActivityNotice
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/** Model events through the generated API: what a collector of `AccountScope.events(Model)` hears, and in which order. */
class ModelEventsTest {
    private fun screenshot(chat: String) = """{"data":{"chatActionSignal":{"chatId":"$chat","userId":"u","kind":"SCREENSHOT","active":true}}}"""

    private suspend fun openNoticeRig(): MockTransport {
        val transport = MockTransport()
        openSigned(Config(null, null), schema("ChatActionSignal"), transport, """[{"name":"ChatActionSignal","document":"subscription ChatActionSignal { chatActionSignal { chatId userId kind active } }"}]""")
        return transport
    }

    private object Ended

    /** Collects every item of one stream in the order it arrives. The end of the stream arrives as [Ended], never as an item. */
    private fun CoroutineScope.hear(stream: ValueStream): Channel<Any> {
        val heard = Channel<Any>(Channel.UNLIMITED)
        launch {
            stream.events(ChatActivityNotice).collect { item -> heard.trySend(item) }
            heard.trySend(Ended)
        }
        return heard
    }

    private fun chat(item: Any): String? = ((item as? ModelEventItem.Event<*>)?.event?.after as? ChatActivityNotice)?.chatId

    /** Requirement: a collector hears every event after it started, in the order the server sent them, and none from before. Observable: the chat ids the collector received. */
    @Test
    fun aModelEventsCollectorHearsEventsInOrderAndNothingFromBeforeItStarted() = bounded {
        val transport = openNoticeRig()
        val screen = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val early = screen.hear(AccountScope.current().events(ChatActivityNotice))
            assertEquals(ModelEventItem.Ready, awaitWithin("the source of the early collector opens") { early.receive() })
            eventually("the signal subscription opens") { transport.calls.size == 1 }
            transport.emit("ChatActionSignal", screenshot("c1"))
            assertEquals("c1", chat(awaitWithin("the first event") { early.receive() }))

            val late = screen.hear(AccountScope.current().events(ChatActivityNotice))
            assertEquals(ModelEventItem.Ready, awaitWithin("the source of the late collector opens") { late.receive() })
            transport.emit("ChatActionSignal", screenshot("c2"))
            transport.emit("ChatActionSignal", screenshot("c3"))

            assertEquals("the late collector got the past event or lost the next one", "c2", chat(awaitWithin("the first event of the late collector") { late.receive() }))
            assertEquals("c3", chat(awaitWithin("the second event of the late collector") { late.receive() }))
            assertEquals("c2", chat(awaitWithin("the second event of the early collector") { early.receive() }))
            assertEquals("c3", chat(awaitWithin("the third event of the early collector") { early.receive() }))
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }

    @Test
    fun dlv_reader_cancel_ends_kotlin() = bounded {
        openNoticeRig()
        val screen = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val items = Channel<ModelEventItem<ChatActivityNotice>>(Channel.UNLIMITED)
            val reader = screen.launch { AccountScope.current().events(ChatActivityNotice).events(ChatActivityNotice).collect { items.trySend(it) } }
            assertEquals(ModelEventItem.Ready, awaitWithin("the source of the reader opens") { items.receive() })
            reader.cancel()
            awaitWithin("the cancelled reader ends") { reader.join() }
            assertTrue("the cancelled reader heard an item", items.tryReceive().isFailure)
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }

    @Test
    fun dlv_reader_cancel_keeps_other_collector_kotlin() = bounded {
        val transport = MockTransport()
        openSigned(Config(null, null), modelsJSON, transport, """[{"name":"ChatList","document":"query ChatList { chats { id title revision lists } }"}]""")
        val failures = java.util.concurrent.CopyOnWriteArrayList<Throwable>()
        val screen = CoroutineScope(SupervisorJob() + Dispatchers.Default + kotlinx.coroutines.CoroutineExceptionHandler { _, error -> failures.add(error) })
        try {
            val hold = transport.hold("ChatList", """{"data":{"chats":[]}}""")
            val projected = Projected(AccountScope.current().stream(Chat.list("SECONDARY")), Chat, screen)
            screen.launch { Chat.list("SECONDARY").load() }
            awaitWithin("the window request is held") { hold.entered() }
            projected.close()
            awaitWithin("the reading job ends") { projected.reading.join() }
            hold.release()
            assertEquals(true, projected.reading.isCompleted)
            assertEquals("the closed projection failed", 0, failures.size)
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }
}

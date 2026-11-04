package com.noma4i.dblayer

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
import org.junit.Test

/** Model events through the generated API: what a collector of `AccountScope.events(Model)` hears, and in which order. */
class ModelEventsTest {
    private fun screenshot(chat: String) = """{"data":{"chatActionSignal":{"chatId":"$chat","userId":"u","kind":"SCREENSHOT","active":true}}}"""

    private suspend fun openNoticeRig(): MockTransport {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"ChatActionSignal","document":"subscription ChatActionSignal { chatActionSignal { chatId userId kind active } }"}]""")
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
            eventually("the signal subscription opens") { transport.calls.size == 1 }
            transport.emit("ChatActionSignal", screenshot("c1"))
            assertEquals("c1", chat(awaitWithin("the first event") { early.receive() }))

            val lateStream = AccountScope.current().events(ChatActivityNotice)
            lateStream.loaded()
            val late = screen.hear(lateStream)
            transport.emit("ChatActionSignal", screenshot("c2"))
            transport.emit("ChatActionSignal", screenshot("c3"))

            assertEquals("the late collector got the past event or lost the next one", "c2", chat(awaitWithin("the first event of the late collector") { late.receive() }))
            assertEquals("c3", chat(awaitWithin("the second event of the late collector") { late.receive() }))
            assertEquals("c2", chat(awaitWithin("the second event of the early collector") { early.receive() }))
            assertEquals("c3", chat(awaitWithin("the third event of the early collector") { early.receive() }))
        } finally {
            screen.cancel()
            DBlayer.close()
        }
    }

    /** Requirement: a session reset ends the old events stream and nothing of the old session follows. A stream of the next session needs a real session with a model-event subscription, which the wrapper rigs cannot start: the core covers it at Sync level. Observable: the end of the old stream. */
    @Test
    fun aSessionResetEndsTheOldEventsStream() = bounded {
        val transport = openNoticeRig()
        val screen = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val first = screen.hear(AccountScope.current().events(ChatActivityNotice))
            eventually("the signal subscription opens") { transport.calls.size == 1 }
            transport.emit("ChatActionSignal", screenshot("a1"))
            assertEquals("a1", chat(awaitWithin("the event of the first session") { first.receive() }))

            DBlayer.sessionReset()
            assertEquals("the old stream delivered an item instead of ending after the session reset", Ended, awaitWithin("the end of the old stream") { first.receive() })
        } finally {
            screen.cancel()
            DBlayer.close()
        }
    }
}

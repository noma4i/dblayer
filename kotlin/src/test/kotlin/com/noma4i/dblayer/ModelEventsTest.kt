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
            DBlayer.close()
        }
    }

    /** S209: a collector that reads nothing while 400 events arrive takes the events the journal kept and lost ranges that add up to the 400 sent. Observed: the items of the collector. Breaks when the journal drops an event without keeping its position. */
    @Test
    fun aSlowCollectorReadsKeptEventsAndLostRangesThatAddUpToTheEventsSent() = bounded {
        val transport = openNoticeRig()
        val screen = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val items = Channel<ModelEventItem<ChatActivityNotice>>(Channel.UNLIMITED)
            val gate = kotlinx.coroutines.CompletableDeferred<Unit>()
            val stream = AccountScope.current().events(ChatActivityNotice)
            screen.launch { stream.events(ChatActivityNotice).collect { item -> items.send(item); if (item is ModelEventItem.Ready) gate.await() } }
            assertEquals(ModelEventItem.Ready, awaitWithin("the source opens") { items.receive() })
            eventually("the signal subscription opens") { transport.calls.size == 1 }
            for (burst in listOf(0 until 200, 200 until 400)) {
                for (index in burst) transport.emit("ChatActionSignal", screenshot("c$index"))
                eventually("the notices of the burst are stored") { Query(ChatActivityNotice.modelName, ChatActivityNotice).read().size == burst.last + 1 }
            }
            gate.complete(Unit)
            var events = 0L
            val lost = mutableListOf<Pair<ULong, ULong>>()
            fun accounted() = events + lost.sumOf { (first, last) -> (last - first + 1uL).toLong() }
            while (accounted() < 400) {
                when (val item = awaitWithin("the next item of the burst") { items.receive() }) {
                    is ModelEventItem.Event -> events += 1
                    is ModelEventItem.Lost -> lost.add(item.first to item.last)
                    else -> throw AssertionError("the burst ended before 400 events were accounted: $item")
                }
            }
            assertEquals("kept events and lost positions add up to the events sent", 400L, accounted())
            org.junit.Assert.assertTrue("the journal kept $events events of a burst of 400", events <= 256)
            org.junit.Assert.assertTrue("a reader that read nothing during the burst lost no position", lost.isNotEmpty())
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
            assertEquals(ModelEventItem.Ready, awaitWithin("the source opens") { first.receive() })
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

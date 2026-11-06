package com.noma4i.dblayer

import com.noma4i.dblayer.models.Chat
import com.noma4i.dblayer.models.Presence
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Test

/** Spec B, first frame: the cached rows come with the first value, read off the calling thread. Related queries: a stream over a server relation follows a change of its rows and of the link. */
class FirstFrameAndRelatedTest {
    private fun titles(projected: Projected<Chat>) = projected.projection.value.rows.map { it.title }

    /** Requirement: a stream opened over cached rows delivers them in its very first value, which the reader takes off the calling thread. Observable: the kind, the rows and the revision of the first value, and the thread that took it. */
    @Test
    fun theFirstFrameOfAWarmStreamArrivesOffTheCallingThreadWithTheCachedRows() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, MockTransport())
        val ann = User.create(name = "Ann").optimisticValue!!
        val seats = listOf(Presence.create(userId = ann.id, room = "r").optimisticValue!!, Presence.create(userId = ann.id, room = "r").optimisticValue!!)
        val stream = AccountScope.current().stream(Presence.where(Condition.equal("room", jsonValue("r"))))
        val caller = Thread.currentThread()
        try {
            val (onCaller, first) = withContext(Dispatchers.Default) { (Thread.currentThread() === caller) to stream.values.first() }
            assertFalse("the first value was taken on the calling thread", onCaller)
            val snapshot = first as? ReadValue.Snapshot
            assertNotNull("the first value was not a snapshot with a revision", snapshot)
            assertEquals("the first value lacked the cached rows: ${snapshot?.rows?.map { it.id }}", seats.map { it.id }.toSet(), snapshot!!.rows.map { it.id }.toSet())
        } finally {
            stream.cancel()
            DBlayer.close()
        }
    }

    /** Requirement: a stream over a server relation shows the rows of the relation, follows a change of one of its rows and a change of the link that moves a row out of it. Observable: the titles the projection shows after each step. */
    @Test
    fun aRelatedQueryReturnsTheLinkedRowsAndFollowsATargetChangeAndALinkChange() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"ChatList","document":"query ChatList { chats { id title revision lists } }"},{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId chat { id title revision lists } message { id text chatId } } } }"}]""")
        val screen = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val scope = AccountScope.current()
            val events = scope.events(Chat)
            screen.launch { runCatching { events.values.collect { } } }
            eventually("the events subscription opens") { transport.calls.size == 1 }
            transport.enqueue("ChatList", """{"data":{"chats":[{"__typename":"Chat","id":"c1","title":"before","revision":1,"lists":["SECONDARY"]}]}}""")
            val related = Projected(scope.stream(Chat.list("SECONDARY")), Chat, screen)
            Chat.list("SECONDARY").load()
            eventually("the linked row shows") { titles(related) == listOf("before") }

            transport.emit("RecordEvents", """{"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"m1","chat":{"id":"c1","title":"after","revision":2,"lists":["SECONDARY"]},"message":{"id":"m1","text":"x","chatId":"c1"}}]}}}""")
            eventually("a change of the row shows") { titles(related) == listOf("after") }

            transport.emit("RecordEvents", """{"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"m1","chat":{"id":"c1","title":"after","revision":3,"lists":["PRIMARY"]},"message":{"id":"m1","text":"x","chatId":"c1"}}]}}}""")
            eventually("a change of the link moves the row out of the relation") { titles(related).isEmpty() }
        } finally {
            screen.cancel()
            DBlayer.close()
        }
    }
}

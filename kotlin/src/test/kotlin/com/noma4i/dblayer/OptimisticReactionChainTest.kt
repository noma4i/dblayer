package com.noma4i.dblayer

import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlinx.coroutines.withTimeout

@OptIn(DBlayerTestingApi::class)
class OptimisticReactionChainTest {
    @Test
    fun reactThenUndoKeepsTheOtherUsersReaction() = bounded {
        val created = """{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":[{"body":"B","user":{"id":"u2"}}]}}}"""
        val transport = openRecordSession(reacts = 0, recordReply = created)
        val record = settledRecord()
        assertTrue(record.reactions.any { it.userId == "u2" && it.body == "B" })
        val firstReply = """{"data":{"recordReact":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":[{"body":"B","user":{"id":"u2"}},{"body":"A","user":{"id":"u1"}}]}}}"""
        val secondReply = """{"data":{"recordReact":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":[{"body":"B","user":{"id":"u2"}}]}}}"""
        val first = transport.hold("RecordReact", firstReply)
        val second = transport.hold("RecordReact", secondReply)
        try {
            val firstReceipt = withTimeout(5000) { record.react(reactionBody = "A", userId = "u1") }
            withTimeout(5000) { first.entered() }
            val changed = firstReceipt.optimisticValue!!
            val secondReceipt = withTimeout(5000) { changed.react(reactionBody = "", userId = "u1") }
            assertEquals(listOf("u2" to "B"), secondReceipt.optimisticValue!!.reactions.map { it.userId to it.body })
            first.release()
            withTimeout(5000) { firstReceipt.confirmed() }
            withTimeout(5000) { second.entered() }
            second.release()
            withTimeout(5000) { secondReceipt.confirmed() }
            val result = Query("Record", Record).readRow(record.id)!!
            assertEquals(listOf("u2" to "B"), result.reactions.map { it.userId to it.body })
        } finally {
            first.release()
            second.release()
        }
    }

    /** The order of the app: a send the server confirmed, then each reaction on the row the previous call returned. The session of the plain transport holds from the open. */
    @Test
    fun eachReactionOnTheRowOfThePreviousCallKeepsOtherUsersReactions() = bounded {
        val transport = openRecordSession(reacts = 0)
        repeat(4) { transport.hold("RecordReact", RECORD_REACTED) }
        var reacted = settledRecord().react(reactionBody = "A", userId = "u1").optimisticValue!!
        reacted = reacted.react(reactionBody = "B", userId = "u2").optimisticValue!!
        reacted = reacted.react(reactionBody = "C", userId = "u1").optimisticValue!!
        assertEquals(listOf("u1" to "C", "u2" to "B"), reacted.reactions.map { it.userId to it.body })

        reacted = reacted.react(reactionBody = "", userId = "u1").optimisticValue!!
        assertEquals(listOf("u2" to "B"), reacted.reactions.map { it.userId to it.body })
    }

    /** A database opened without a transport holds no session, so a send is refused. */
    @Test
    fun aSendOnADatabaseWithoutATransportIsRefusedAsNoSession() = bounded {
        try {
            closeForCleanup()
            DBlayer.openTesting(null, modelsJSON)
            val refused = runCatching { Record.create(containerId = "c1", authorId = "u1", text = "hello", account = AccountScope.current()) }.exceptionOrNull()
            assertTrue("the send was not refused: $refused", refused is DbException.NoSession)
        } finally {
            closeForCleanup()
        }
    }
}

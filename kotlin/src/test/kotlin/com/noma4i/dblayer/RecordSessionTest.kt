package com.noma4i.dblayer

import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

private const val RECORD_CREATED = """{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z"}}}"""
private const val RECORD_REACTED = """{"data":{"recordReact":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":[{"body":"C","userId":"u1"}]}}}"""

/** A signed-in plain transport: the tests of a non-durable action need the network, so the record is sent online and answered before the action starts. */
suspend fun openRecordSession(reactDelayMillis: Long = 0, reacts: Int = 3, recordReply: String = RECORD_CREATED): MockTransport {
    val transport = MockTransport()
    transport.enqueue("CreateRecord", recordReply)
    repeat(reacts) { transport.enqueue("RecordReact", RECORD_REACTED, reactDelayMillis) }
    reopen(Config(null, null, 32u), modelsJSON, transport)
    DBlayer.database.registerOperations("""[{"name":"CreateRecord","document":"mutation CreateRecord(${'$'}containerId: ID!, ${'$'}text: String!, ${'$'}signedId: String, ${'$'}attachedUserId: ID) { createRecord(containerId: ${'$'}containerId, text: ${'$'}text, signedId: ${'$'}signedId, attachedUserId: ${'$'}attachedUserId) { id containerId authorId text createdAt reactions { body userId } } }"},{"name":"RecordReact","document":"mutation RecordReact(${'$'}id: ID!, ${'$'}reactionBody: String!, ${'$'}userId: ID!) { recordReact(id: ${'$'}id, reactionBody: ${'$'}reactionBody, userId: ${'$'}userId) { id containerId authorId text createdAt reactions { body userId } } }"}]""")
    return transport
}

suspend fun settledRecord(): Record {
    val receipt = withTimeout(5000) { Record.send(AccountScope.current(), containerId = "c1", authorId = "u1", text = "hello") }
    check(withTimeout(5000) { receipt.outcome() } is OperationOutcome.Confirmed)
    return Query("Record", Record).read().first()
}

class RecordSessionTest {
    /** K14 (B6, the order of the app): a reaction whose answer is delayed shows at once as unsent, and the answer ends it as synced. */
    @Test
    fun aReactionWithADelayedAnswerShowsAsUnsentBeforeTheAnswerAndSyncedAfter() = bounded {
        val transport = openRecordSession(reacts = 0)
        val record = settledRecord()
        val hold = transport.hold("RecordReact", RECORD_REACTED)
        try {
        val receipt = record.react(reactionBody = "C", userId = "u1")
        withTimeout(5000) { hold.entered() }
        val changed = requireNotNull(receipt.optimisticValue)
        assertEquals("the reaction was not shown before the answer", listOf("C"), changed.reactions.map { it.body })
        val during = Query("Record", Record).readRow(record.id)!!
        assertTrue("the row was synced before the server answered", during.sync.state != SyncState.Synced)
        assertEquals(listOf("C"), during.reactions.map { it.body })
        hold.release()
        assertTrue(withTimeout(5000) { receipt.outcome() } is OperationOutcome.Confirmed)
        assertEquals(SyncState.Synced, Query("Record", Record).readRow(record.id)!!.sync.state)
        assertEquals(1, transport.calls.count { it.operationName == "RecordReact" })
        } finally { hold.release() }
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

private const val RECORD_CREATED = """{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z"}}}"""
const val RECORD_REACTED = """{"data":{"recordReact":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":[{"body":"C","userId":"u1"}]}}}"""

/** A signed-in plain transport: a test that edits the record starts after the server answered its create. */
suspend fun openRecordSession(reacts: Int = 3, recordReply: String = RECORD_CREATED): MockTransport {
    val transport = MockTransport()
    transport.enqueue("CreateRecord", recordReply)
    repeat(reacts) { transport.enqueue("RecordReact", RECORD_REACTED) }
    openSigned(Config(null, null), modelsJSON, transport)
    registerStandOperations(DBlayer.database, """[{"name":"CreateRecord","document":"mutation CreateRecord(${'$'}containerId: ID!, ${'$'}text: String!, ${'$'}signedId: String, ${'$'}attachedUserId: ID) { createRecord(containerId: ${'$'}containerId, text: ${'$'}text, signedId: ${'$'}signedId, attachedUserId: ${'$'}attachedUserId) { id containerId authorId text createdAt reactions { body userId } } }"},{"name":"RecordReact","document":"mutation RecordReact(${'$'}id: ID!, ${'$'}reactionBody: String!, ${'$'}userId: ID!) { recordReact(id: ${'$'}id, reactionBody: ${'$'}reactionBody, userId: ${'$'}userId) { id containerId authorId text createdAt reactions { body userId } } }"}]""")
    return transport
}

suspend fun settledRecord(): Record {
    val receipt = withTimeout(5000) { Record.create(containerId = "c1", authorId = "u1", text = "hello", account = AccountScope.current()) }
    withTimeout(5000) { receipt.confirmed() }
    return Query("Record", Record).read().first()
}

class RecordSessionTest {
}

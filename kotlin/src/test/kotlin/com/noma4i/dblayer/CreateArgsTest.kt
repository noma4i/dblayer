package com.noma4i.dblayer

import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class CreateArgsTest {
    @Test
    fun aCreateSendsItsArgsAndTheRowNeverStoresThem() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateRecord", """{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c","authorId":"u1","text":"hi","createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateRecord","document":"mutation CreateRecord(${'$'}containerId: ID!, ${'$'}text: String!, ${'$'}signedId: String, ${'$'}attachedUserId: ID) { createRecord(containerId: ${'$'}containerId, text: ${'$'}text, signedId: ${'$'}signedId, attachedUserId: ${'$'}attachedUserId) { id containerId authorId text createdAt } }"}]""")

        val local = Record.create(containerId = "c", authorId = "u1", text = "hi", attachedUserId = "42", signedId = "blob-1")
        withTimeout(5000) { local.settled() }
        val sent = transport.calls.first { it.operationName == "CreateRecord" }.variablesJson
        assertTrue("variables sent: $sent", sent.contains("\"signedId\":\"blob-1\"") && sent.contains("\"attachedUserId\":42"))
        assertEquals("hi", Record.find(withTimeout(5000) { local.confirmedId() }).current()?.text)
    }
}

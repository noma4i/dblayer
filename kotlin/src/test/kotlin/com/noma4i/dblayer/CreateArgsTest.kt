package com.noma4i.dblayer

import com.noma4i.dblayer.models.Message
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class CreateArgsTest {
    @Test
    fun aCreateSendsItsArgsAndTheRowNeverStoresThem() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMessage", """{"data":{"createMessage":{"__typename":"Message","id":"x1","chatId":"c","authorId":"u1","text":"hi","createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMessage","document":"mutation CreateMessage(${'$'}chatId: ID!, ${'$'}text: String!, ${'$'}signedId: String, ${'$'}attachedUserId: ID) { createMessage(chatId: ${'$'}chatId, text: ${'$'}text, signedId: ${'$'}signedId, attachedUserId: ${'$'}attachedUserId) { id chatId authorId text createdAt } }"}]""")

        val local = Message.create(chatId = "c", authorId = "u1", text = "hi", attachedUserId = "42", signedId = "blob-1")
        withTimeout(5000) { local.settled() }
        val sent = transport.calls.first { it.operationName == "CreateMessage" }.variablesJson
        assertTrue("variables sent: $sent", sent.contains("\"signedId\":\"blob-1\"") && sent.contains("\"attachedUserId\":42"))
        assertEquals("hi", Message.find(withTimeout(5000) { local.confirmedId() }).current()?.text)
    }
}

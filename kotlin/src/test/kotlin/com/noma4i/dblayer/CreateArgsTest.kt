package com.noma4i.dblayer

import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.CurrentUser
import com.noma4i.dblayer.models.CurrentRegion
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class CreateArgsTest {
    @Test
    fun aCreateReadsTwoDeclaredLocalSources() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateRecord", """{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c","authorId":"u1","text":"hi","createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        openSigned(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateRecord","document":"mutation CreateRecord(${'$'}containerId: ID!, ${'$'}text: String!, ${'$'}signedId: String, ${'$'}attachedUserId: ID) { createRecord(containerId: ${'$'}containerId, text: ${'$'}text, signedId: ${'$'}signedId, attachedUserId: ${'$'}attachedUserId) { id containerId authorId text createdAt } }"},{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"},{"name":"CurrentRegionQuery","document":"query CurrentRegionQuery { region { id name } }"}]""")
        transport.enqueue("CurrentUserQuery", """{"data":{"user":{"id":"u1","name":"Ann"}}}""")
        transport.enqueue("CurrentRegionQuery", """{"data":{"region":{"id":"r1","name":"Home"}}}""")
        CurrentUser.current.load()
        CurrentRegion.current.load()
        val row = Record.create(containerId = "c", text = "hi", account = AccountScope.current()).optimisticValue!!
        assertEquals(listOf("u1", "r1"), listOf(row.authorId, row.regionId))
    }

    @Test
    fun aCreateSendsItsArgsAndTheRowNeverStoresThem() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateRecord", """{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c","authorId":"u1","text":"hi","createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        openSigned(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateRecord","document":"mutation CreateRecord(${'$'}containerId: ID!, ${'$'}text: String!, ${'$'}signedId: String, ${'$'}attachedUserId: ID) { createRecord(containerId: ${'$'}containerId, text: ${'$'}text, signedId: ${'$'}signedId, attachedUserId: ${'$'}attachedUserId) { id containerId authorId text createdAt } }"}]""")

        val confirmed = withTimeout(5000) { Record.create(containerId = "c", authorId = "u1", text = "hi", attachedUserId = "42", signedId = "blob-1", account = AccountScope.current()).confirmed() }
        val sent = transport.calls.first { it.operationName == "CreateRecord" }.variablesJson
        assertTrue("variables sent: $sent", sent.contains("\"signedId\":\"blob-1\"") && sent.contains("\"attachedUserId\":42"))
        assertEquals("hi", Record.find(confirmed.id).current()?.text)
    }
}

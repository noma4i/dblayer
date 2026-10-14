package com.noma4i.dblayer

import com.noma4i.dblayer.models.Operations
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

class OperationCallTest {
    @Test
    fun anOperationWithoutAModelSendsItsVariablesAndReadsTheDeclaredResult() = bounded {
        val transport = MockTransport()
        transport.enqueue("QrCodeCreate", """{"data":{"qrCodeCreate":{"url":"https://q"}}}""")
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"QrCodeCreate","document":"mutation QrCodeCreate(${'$'}input: CreateQrCodeInput!) { qrCodeCreate(input: ${'$'}input) { url } }"}]""")

        val result = withTimeout(5000) { Operations.qrCodeCreate(kind = "chat", size = 3) }

        assertEquals("https://q", result.url)
        assertEquals("""{"input":{"kind":"chat","size":3}}""", transport.calls.first { it.operationName == "QrCodeCreate" }.variablesJson)
    }
}

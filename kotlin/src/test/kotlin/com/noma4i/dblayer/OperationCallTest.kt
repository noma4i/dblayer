package com.noma4i.dblayer

import com.noma4i.dblayer.models.Operations
import com.noma4i.dblayer.models.OwnerInput
import com.noma4i.dblayer.models.StickerInput
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

class OperationCallTest {
    private fun rig(name: String, document: String, reply: String): MockTransport {
        val transport = MockTransport()
        transport.enqueue(name, reply)
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"$name","document":"$document"}]""")
        return transport
    }

    @Test
    fun anOperationWithoutAModelSendsItsVariablesAndReadsTheDeclaredResult() = bounded {
        val transport = rig("QrCodeCreate", "mutation QrCodeCreate(${'$'}input: CreateQrCodeInput!) { qrCodeCreate(input: ${'$'}input) { url } }", """{"data":{"qrCodeCreate":{"url":"https://q"}}}""")

        val result = withTimeout(5000) { Operations.qrCodeCreate(kind = "chat", size = 3) }

        assertEquals("https://q", result.url)
        assertEquals(parseJson("""{"input":{"kind":"chat","size":3}}"""), parseJson(transport.calls.first { it.operationName == "QrCodeCreate" }.variablesJson))
    }

    @Test
    fun anInputObjectOfTheSchemaIsSentNestedAndWithoutItsNullFields() = bounded {
        val transport = rig("StickerSend", "mutation StickerSend(${'$'}input: StickerInput!) { stickerSend(input: ${'$'}input) { id } }", """{"data":{"stickerSend":{"id":"s1"}}}""")
        val input = StickerInput(name = "wave", tags = listOf("a", "b"), notes = listOf("n", null), pinned = true, owner = OwnerInput(id = "7"), helpers = listOf(OwnerInput(id = "8", nickname = "ann")))

        val result = withTimeout(5000) { Operations.stickerSend(input) }

        assertEquals("s1", result.id)
        val expected = """{"input":{"name":"wave","tags":["a","b"],"notes":["n",null],"pinned":true,"owner":{"id":"7"},"helpers":[{"id":"8","nickname":"ann"}]}}"""
        assertEquals(parseJson(expected), parseJson(transport.calls.first { it.operationName == "StickerSend" }.variablesJson))
    }

    @Test
    fun aResultWithListsObjectsAndRawJsonIsDecodedIntoGeneratedTypes() = bounded {
        rig("Pricing", "query Pricing { pricing { gifts { id name price } videoIntro { enabled } tags spec } }", """{"data":{"pricing":{"gifts":[{"id":7,"name":"rose","price":5},{"id":"8","name":"ring"}],"videoIntro":{"enabled":true},"tags":["a","b"],"spec":{"k":[1,2]}}}}""")

        val result = withTimeout(5000) { Operations.pricingLoad() }

        assertEquals(listOf("7", "8"), result.gifts?.map { it.id })
        assertEquals(listOf(5L, null), result.gifts?.map { it.price })
        assertEquals(true, result.videoIntro?.enabled)
        assertEquals(listOf("a", "b"), result.tags)
        assertEquals(parseJson("""{"k":[1,2]}"""), result.spec)
    }
}

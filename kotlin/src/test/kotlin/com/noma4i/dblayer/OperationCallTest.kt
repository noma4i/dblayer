package com.noma4i.dblayer

import com.noma4i.dblayer.models.Operations
import com.noma4i.dblayer.models.MomentInput
import com.noma4i.dblayer.models.OwnerInput
import com.noma4i.dblayer.models.SettingsInput
import com.noma4i.dblayer.models.StickerInput
import com.noma4i.dblayer.models.WidgetInput
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
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

    private fun refusal(error: DbException): String = when (error) {
        is DbException.Http -> "Http ${error.status}"
        is DbException.Graphql -> "Graphql ${error.messages.joinToString(",")}"
        else -> error.javaClass.simpleName
    }

    @Test
    fun aRefusalOfTheServerArrivesTypedThroughAnOperationCall() = bounded {
        val refusals = listOf(DbException.Http(401u, "unauthorized") to "Http 401", DbException.Graphql(listOf("nope", "bad")) to "Graphql nope,bad")
        for ((failure, expected) in refusals) {
            val transport = MockTransport()
            transport.enqueue("QrCodeCreate", failure)
            DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
            DBlayer.database.registerOperations("""[{"name":"QrCodeCreate","document":"mutation QrCodeCreate(${'$'}input: CreateQrCodeInput!) { qrCodeCreate(input: ${'$'}input) { url } }"}]""")
            val error = runCatching { withTimeout(5000) { Operations.qrCodeCreate(kind = "chat", size = 3) } }.exceptionOrNull()
            assertEquals(expected, refusal(error as DbException))
        }
    }

    @Test
    fun aGraphqlErrorsAnswerArrivesWithEveryMessageThroughAnOperationCall() = bounded {
        rig("QrCodeCreate", "mutation QrCodeCreate(${'$'}input: CreateQrCodeInput!) { qrCodeCreate(input: ${'$'}input) { url } }", """{"errors":[{"message":"nope"},{"message":"bad"}]}""")
        val error = runCatching { withTimeout(5000) { Operations.qrCodeCreate(kind = "chat", size = 3) } }.exceptionOrNull()
        assertEquals("Graphql nope,bad", refusal(error as DbException))
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

    @Test
    fun aListOperationReturnsOneGeneratedResultPerElementOfTheRootList() = bounded {
        rig("Giphy", "query Giphy(${'$'}type: String!, ${'$'}limit: Int) { giphy(type: ${'$'}type, limit: ${'$'}limit) { id url isSticker } }", """{"data":{"giphy":[{"id":1,"url":"u1","isSticker":true},{"id":"2"}]}}""")

        val result = withTimeout(5000) { Operations.giphyLoad(type = "gif") }

        assertEquals(listOf("1", "2"), result.map { it.id })
        assertEquals(listOf("u1", null), result.map { it.url })
        assertEquals(listOf(true, null), result.map { it.isSticker })
    }

    @Test
    fun aJsonScalarOfAnInputObjectIsSentAsAnObjectAndAnOmittedListIsNotSent() = bounded {
        val transport = rig("MomentCreate", "mutation MomentCreate(${'$'}input: MomentInput!) { momentCreate(input: ${'$'}input) { moment { id } } }", """{"data":{"momentCreate":{"moment":{"id":9}}}}""")
        val props = JsonObject(mapOf("emoji" to JsonPrimitive("fire"), "size" to JsonPrimitive(3), "tags" to JsonArray(listOf(JsonPrimitive("a")))))
        val widget = WidgetInput(kind = "EMOJI", settings = SettingsInput(itemProps = props, rotate = 1.5))

        assertEquals("9", withTimeout(5000) { Operations.momentCreate(MomentInput(file = "s1", widgets = listOf(widget))) }.moment?.id)
        transport.enqueue("MomentCreate", """{"data":{"momentCreate":{"moment":{"id":9}}}}""")
        assertEquals("9", withTimeout(5000) { Operations.momentCreate(MomentInput(file = "s2")) }.moment?.id)

        val calls = transport.calls.filter { it.operationName == "MomentCreate" }.map { it.variablesJson }
        assertEquals(parseJson("""{"input":{"file":"s1","widgets":[{"kind":"EMOJI","settings":{"itemProps":{"emoji":"fire","size":3,"tags":["a"]},"rotate":1.5}}]}}"""), parseJson(calls[0]))
        assertEquals(parseJson("""{"input":{"file":"s2"}}"""), parseJson(calls[1]))
    }
}

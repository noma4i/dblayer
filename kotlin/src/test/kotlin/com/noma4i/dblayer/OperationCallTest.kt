package com.noma4i.dblayer

import com.noma4i.dblayer.models.Operations
import com.noma4i.dblayer.models.MomentInput
import com.noma4i.dblayer.models.OwnerInput
import com.noma4i.dblayer.models.RecursiveInput
import com.noma4i.dblayer.models.RecursiveChoice
import com.noma4i.dblayer.models.SettingsInput
import com.noma4i.dblayer.models.StickerInput
import com.noma4i.dblayer.models.WidgetInput
import com.noma4i.dblayer.models.WidgetKind
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.Test

class OperationCallTest {
    private suspend fun rig(name: String, document: String, reply: String): MockTransport {
        val transport = MockTransport()
        transport.enqueue(name, reply)
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"$name","document":"$document"}]""")
        return transport
    }

    @Test
    fun anOperationWithoutAModelSendsItsVariablesAndReadsTheDeclaredResult() = bounded {
        val transport = rig("QrCodeCreate", "mutation QrCodeCreate(${'$'}input: CreateQrCodeInput!) { qrCodeCreate(input: ${'$'}input) { url } }", """{"data":{"qrCodeCreate":{"url":"https://q"}}}""")

        val result = withTimeout(5000) { Operations.qrCodeCreate(kind = "container", size = 3) }

        assertEquals("https://q", result.url)
        assertEquals(parseJson("""{"input":{"kind":"container","size":3}}"""), parseJson(transport.calls.first { it.operationName == "QrCodeCreate" }.variablesJson))
    }

    private fun refusal(error: DbException): String = when (error) {
        is DbException.Http -> "Http ${error.status}"
        is DbException.Graphql -> "Graphql ${error.messages.joinToString(",")}"
        else -> error.javaClass.simpleName
    }

    @Test
    fun aRefusalOfTheServerArrivesTypedThroughAnOperationCall() = bounded {
        val refusals = listOf(DbException.Http(401u, "unauthorized") to "Http 401", DbException.Graphql(listOf("nope", "bad"), null) to "Graphql nope,bad")
        for ((failure, expected) in refusals) {
            val transport = MockTransport()
            transport.enqueue("QrCodeCreate", failure)
            reopen(Config(null, null, 32u), modelsJSON, transport)
            DBlayer.database.registerOperations("""[{"name":"QrCodeCreate","document":"mutation QrCodeCreate(${'$'}input: CreateQrCodeInput!) { qrCodeCreate(input: ${'$'}input) { url } }"}]""")
            val error = runCatching { withTimeout(5000) { Operations.qrCodeCreate(kind = "container", size = 3) } }.exceptionOrNull()
            assertEquals(expected, refusal(error as DbException))
        }
    }

    @Test
    fun inputObjectsDistinguishOmittedNullAndValues() = bounded {
        val transport = rig("StickerSend", "mutation StickerSend(${'$'}input: StickerInput!) { stickerSend(input: ${'$'}input) { id } }", """{"data":{"stickerSend":{"id":"s1"}}}""")
        val input = StickerInput(name = "wave", mood = GraphQLNullable.Null, tags = listOf("a", "b"), notes = GraphQLNullable.Value(listOf("n", null)), pinned = true, owner = OwnerInput(id = "7"), helpers = GraphQLNullable.Value(listOf(OwnerInput(id = "8", nickname = GraphQLNullable.Value("ann")))))

        val result = withTimeout(5000) { Operations.stickerSend(input) }

        assertEquals("s1", result.id)
        val expected = """{"input":{"name":"wave","mood":null,"tags":["a","b"],"notes":["n",null],"pinned":true,"owner":{"id":"7"},"helpers":[{"id":"8","nickname":"ann"}]}}"""
        assertEquals(parseJson(expected), parseJson(transport.calls.first { it.operationName == "StickerSend" }.variablesJson))
    }

    @Test
    fun recursiveInputsPreserveNestedValues() = bounded {
        val transport = rig("StickerSend", "mutation StickerSend(${'$'}input: StickerInput!) { stickerSend(input: ${'$'}input) { id } }", """{"data":{"stickerSend":{"id":"s1"}}}""")
        val recursive = RecursiveInput(next = GraphQLNullable.Value(RecursiveInput(next = GraphQLNullable.Null)), choice = GraphQLNullable.Value(RecursiveChoice.Next(RecursiveChoice.Value("leaf"))))
        val input = StickerInput(name = "tree", tags = emptyList(), pinned = false, owner = OwnerInput(id = "7"), recursive = GraphQLNullable.Value(recursive))
        withTimeout(5000) { Operations.stickerSend(input) }
        val expected = """{"input":{"name":"tree","tags":[],"pinned":false,"owner":{"id":"7"},"recursive":{"next":{"next":null},"choice":{"next":{"value":"leaf"}}}}}"""
        assertEquals(parseJson(expected), parseJson(transport.calls.first { it.operationName == "StickerSend" }.variablesJson))
    }

    @Test
    fun aResultWithListsObjectsAndRawJsonIsDecodedIntoGeneratedTypes() = bounded {
        rig("Pricing", "query Pricing { pricing { rewards { id name price } videoIntro { enabled } tags spec } }", """{"data":{"pricing":{"rewards":[{"id":7,"name":"rose","price":5},{"id":"8","name":"ring"}],"videoIntro":{"enabled":true},"tags":["a","b"],"spec":{"k":[1,2]}}}}""")

        val result = withTimeout(5000) { Operations.pricingLoad() }

        assertEquals(listOf("7", "8"), result.rewards?.map { it.id })
        assertEquals(listOf(5L, null), result.rewards?.map { it.price })
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
        val widget = WidgetInput(kind = WidgetKind.Emoji, settings = SettingsInput(itemProps = props, rotate = GraphQLNullable.Value(1.5)))

        assertEquals("9", withTimeout(5000) { Operations.momentCreate(MomentInput(file = "s1", widgets = GraphQLNullable.Value(listOf(widget)))) }.moment?.id)
        transport.enqueue("MomentCreate", """{"data":{"momentCreate":{"moment":{"id":9}}}}""")
        assertEquals("9", withTimeout(5000) { Operations.momentCreate(MomentInput(file = "s2")) }.moment?.id)

        val calls = transport.calls.filter { it.operationName == "MomentCreate" }.map { it.variablesJson }
        assertEquals(parseJson("""{"input":{"file":"s1","widgets":[{"kind":"EMOJI","settings":{"itemProps":{"emoji":"fire","size":3,"tags":["a"]},"rotate":1.5}}]}}"""), parseJson(calls[0]))
        assertEquals(parseJson("""{"input":{"file":"s2"}}"""), parseJson(calls[1]))
    }
}

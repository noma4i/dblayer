package com.noma4i.dblayer

import com.noma4i.dblayer.models.Clip
import com.noma4i.dblayer.models.Operations
import com.noma4i.dblayer.models.StickerEmojiInput
import com.noma4i.dblayer.models.StickerPropsInput
import com.noma4i.dblayer.models.StickerTextInput
import com.noma4i.dblayer.models.StickerWidgetInput
import com.noma4i.dblayer.models.WidgetKind
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertEquals
import org.junit.Test

class InputListTest {
    private val widgets = listOf(
        StickerWidgetInput(kind = WidgetKind.Emoji, props = StickerPropsInput.Emoji(StickerEmojiInput(code = "fire", kind = GraphQLNullable.Value(WidgetKind.Gif))), extras = GraphQLNullable.Value(listOf(StickerPropsInput.Tags(listOf("a")), StickerPropsInput.Text(StickerTextInput(body = "hi"))))),
        StickerWidgetInput(kind = WidgetKind.Gif, props = StickerPropsInput.Tags(listOf("b"))),
    )
    private val widgetsJson = """[{"kind":"EMOJI","props":{"emoji":{"code":"fire","kind":"GIF"}},"extras":[{"tags":["a"]},{"text":{"body":"hi"}}]},{"kind":"GIF","props":{"tags":["b"]}}]"""

    private fun rig(reply: String): MockTransport {
        val transport = MockTransport()
        transport.enqueue("WidgetsSend", reply)
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"WidgetsSend","document":"mutation WidgetsSend(${'$'}widgets: [StickerWidgetInput!]) { widgetsSend(widgets: ${'$'}widgets) { id } }"}]""")
        return transport
    }

    @Test
    fun aListOfInputObjectsWithOneOfValuesIsSentWithOneKeyPerValue() = bounded {
        val transport = rig("""{"data":{"widgetsSend":{"id":"w1"}}}""")

        val result = withTimeout(5000) { Operations.widgetsSend(widgets) }

        assertEquals("w1", result.id)
        assertEquals(parseJson("""{"widgets":$widgetsJson}"""), parseJson(transport.calls.first { it.operationName == "WidgetsSend" }.variablesJson))
    }

    @Test
    fun anOmittedOptionalListIsNotSent() = bounded {
        val transport = rig("""{"data":{"widgetsSend":{"id":"w0"}}}""")

        withTimeout(5000) { Operations.widgetsSend() }

        assertEquals(parseJson("{}"), parseJson(transport.calls.first { it.operationName == "WidgetsSend" }.variablesJson))
    }

    @Test
    fun theUploadArgsOfAModelBecomeTypedVariables() {
        assertEquals(parseJson("""{"widgets":$widgetsJson}"""), Clip.createFileVariables(widgets))
        assertEquals(JsonObject(emptyMap()), Clip.createFileVariables())
    }
}

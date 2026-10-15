package com.noma4i.dblayer

import com.noma4i.dblayer.models.MessageReplyNode
import com.noma4i.dblayer.models.NestedSample
import com.noma4i.dblayer.models.NestedValue
import com.noma4i.dblayer.models.modelsJSON
import com.noma4i.dblayer.models.NestedTree
import java.io.File
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class NestedTypesTest {
    private fun comparableJson(value: JsonElement): JsonElement = when (value) {
        is JsonObject -> JsonObject(value.filterValues { it != JsonNull }.mapValues { comparableJson(it.value) })
        is JsonArray -> JsonArray(value.map(::comparableJson))
        is JsonPrimitive -> if (value.isString) value else value.content.toBigDecimalOrNull()?.let { parseJson(it.stripTrailingZeros().toPlainString()) } ?: value
    }

    @Test
    fun generatedNestedTypesDecodeTheSharedCases() {
        val cases = parseJson(File("../fixtures/nested.json").readText()) as JsonArray
        for (row in cases) {
            val fields = row.jsonObject
            val label = fields.getValue("name").toString()
            val input = (fields["inputText"] as? JsonPrimitive)?.content ?: fields.getValue("input").toString()
            if (fields["error"] == JsonPrimitive(true)) {
                assertThrows(label, Exception::class.java) { dbJson.decodeFromString(NestedValue.serializer(), input) }
            } else {
                val decoded = dbJson.decodeFromString(NestedValue.serializer(), input)
                assertEquals(label, comparableJson(fields.getValue("output")), comparableJson(jsonValue(decoded)))
            }
        }
        val reply = dbJson.decodeFromString(MessageReplyNode.serializer(), """{"userId":23,"user":{"id":"u1","fullName":"Ada"},"media":{}}""")
        assertEquals("23", reply.userId)
        assertEquals("Ada", reply.user?.fullName)
        assertEquals("", reply.media?.id)
        val tree = dbJson.decodeFromString(NestedTree.serializer(), """{"name":"root","children":[{"name":"leaf"}]}""")
        assertEquals("leaf", tree.children.first().name)
        assertEquals(emptyList<NestedTree>(), tree.children.first().children)
    }
}

class CellDecoderTest {
    private fun open() = Database.open(Config(null, null, 32u), modelsJSON, null)

    @Test
    fun aStoredRowDecodesFromCellsLikeTheSharedJSONCases() = bounded {
        val cases = parseJson(File("../fixtures/nested.json").readText()) as JsonArray
        val database = open()
        for (row in cases) {
            val fields = row.jsonObject
            if (fields["error"] == JsonPrimitive(true) || fields["input"] == null) continue
            val label = fields.getValue("name").toString()
            val wire = database.create("NestedSample", buildJsonObject { put("value", fields.getValue("input")) }.toString(), "{}")
            val expected = dbJson.decodeFromString(NestedValue.serializer(), fields.getValue("output").toString())
            assertEquals(label, expected, NestedSample.read(wire).value)
        }
    }

    @Test
    fun aRowWithAnotherLayoutIsRefused(): Unit = bounded {
        val wire = open().create("NestedSample", "{}", "{}")
        assertThrows(DbException::class.java) { NestedSample.read(wire.copy(layout = "other")) }
    }
}

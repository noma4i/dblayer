package com.noma4i.dblayer

import com.noma4i.dblayer.models.RecordReplyNode
import com.noma4i.dblayer.models.NestedSample
import com.noma4i.dblayer.models.NestedValue
import com.noma4i.dblayer.models.modelsJSON
import com.noma4i.dblayer.models.NestedTree
import java.io.File
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
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
            if (fields["error"] == JsonPrimitive(true)) continue
            val label = fields.getValue("name").toString()
            val output = fields.getValue("output")
            val decoded = dbJson.decodeFromString(NestedValue.serializer(), output.toString())
            assertEquals(label, comparableJson(output), comparableJson(jsonValue(decoded)))
        }
        assertThrows(SerializationException::class.java) { dbJson.decodeFromString(NestedValue.serializer(), """{"title":"loose","owner_id":42}""") }
        val reply = dbJson.decodeFromString(RecordReplyNode.serializer(), """{"userId":"23","user":{"id":"u1","fullName":"Ada"},"media":{"id":""}}""")
        assertEquals("23", reply.userId)
        assertEquals("Ada", reply.user?.fullName)
        assertEquals("", reply.media?.id)
        val tree = dbJson.decodeFromString(NestedTree.serializer(), """{"name":"root","children":[{"name":"leaf","children":[]}]}""")
        assertEquals("leaf", tree.children.first().name)
        assertEquals(emptyList<NestedTree>(), tree.children.first().children)
    }
}

class CellDecoderTest {
    private suspend fun open() = openDatabase(Config(null, null), modelsJSON, null)

    private fun withoutUnknownKeys(value: JsonElement): JsonElement = when (value) {
        is JsonObject -> JsonObject(value.filterKeys { it != "unknown" }.mapValues { withoutUnknownKeys(it.value) })
        is JsonArray -> JsonArray(value.map(::withoutUnknownKeys))
        else -> value
    }

}

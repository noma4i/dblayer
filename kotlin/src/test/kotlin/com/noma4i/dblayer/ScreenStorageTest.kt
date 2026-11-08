package com.noma4i.dblayer

import com.noma4i.dblayer.models.CustomScreen
import com.noma4i.dblayer.models.ScreenRequires
import com.noma4i.dblayer.models.modelsJSON
import java.io.File
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Test

class ScreenStorageTest {
    private suspend fun storedVector(index: Int) {
        val vectors = dbJson.parseToJsonElement(File("../fixtures/screen.json").readText()).jsonArray
        val vector = vectors[index]
        val input = vector.jsonObject.toMutableMap()
        val elements = input.getValue("elements").jsonArray
        input["elements"] = JsonArray(elements.map { element ->
            val fields = element.jsonObject.toMutableMap()
            listOf("props", "visible", "press", "change").forEach { key -> fields[key]?.let { fields[key] = JsonPrimitive(it.toString()) } }
            JsonObject(fields)
        })
        input["state"] = JsonArray(input.getValue("state").jsonArray.map { entry ->
            val fields = entry.jsonObject.toMutableMap()
            fields["value"] = JsonPrimitive(fields.getValue("value").toString())
            JsonObject(fields)
        })
        val transport = MockTransport()
        openSigned(Config(null, null), modelsJSON, transport)
        val document = "query ScreenNode(\$id: ID!) { node(id: \$id) { ... on CustomScreen { id slug version requires { ios android } spec { root state { path value } elements { id type slot children padding margin background radius expand align accessibilityLabel props visible press change } } } } }"
        DBlayer.database.registerOperations(JsonArray(listOf(buildJsonObject { put("name", JsonPrimitive("ScreenNode")); put("document", JsonPrimitive(document)) })).toString())
        val row = buildJsonObject {
            put("__typename", JsonPrimitive("CustomScreen"))
            put("id", JsonPrimitive("sample"))
            put("slug", JsonPrimitive("sample"))
            put("version", JsonPrimitive(1))
            put("requires", dbJson.parseToJsonElement("{\"ios\":2,\"android\":null}"))
            put("spec", JsonObject(input))
        }
        transport.enqueue("ScreenNode", buildJsonObject { put("data", buildJsonObject { put("node", row) }) }.toString())
        CustomScreen.node("sample")
        val saved = CustomScreen.find("sample").current()!!
        assertEquals("sample", saved.slug)
        assertEquals(1L, saved.version)
        assertEquals(ScreenRequires(ios = 2, android = null), saved.requires)
        val stored = saved.spec!!
        assertEquals(vector.jsonObject["state"], jsonValue(stored.state))
        assertEquals(elements.size, stored.elements.size)
        stored.elements.zip(elements).forEach { (element, source) ->
            val fields = source.jsonObject
            assertEquals(fields.getValue("type").jsonPrimitive.content, element.type)
            assertEquals(fields["props"], element.props)
            assertEquals(fields["visible"], element.visible)
            assertEquals(fields["press"], jsonValue(element.press))
            assertEquals(fields["change"], jsonValue(element.change))
        }
        transport.enqueue("ScreenNode", """{"data":{"node":{"__typename":"CustomScreen","id":"sample","slug":"updated"}}}""")
        CustomScreen.node("sample", force = true)
        assertEquals(stored, CustomScreen.find("sample").current()!!.spec)
    }

    @Test
    fun anUnknownElementTypeKeepsArbitraryPropsInStorage() = bounded { storedVector(0) }

    @Test
    fun orderedActionListsKeepNestedConditionsInStorage() = bounded { storedVector(1) }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.KeywordRecord
import com.noma4i.dblayer.models.KeywordValue
import com.noma4i.dblayer.models.Operations
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Test

class KeywordNamesTest {
    @Test
    fun keywordNamesPreserveStoredKeysAndNestedValues() = bounded {
        reopen(Config(null, null), modelsJSON, null)
        val input = """{"default":"nested","when":"now","class":"kind","container":"box","r":"reader"}"""
        val nested = dbJson.decodeFromString(KeywordValue.serializer(), input)
        val created = KeywordRecord.create(default = "row", `class` = "selected", value = nested).optimisticValue!!
        val updated = created.update(default = "changed").optimisticValue!!
        val row = KeywordRecord.`when`("selected").live.current().single()
        assertEquals(Triple(updated.id, "changed", "selected"), Triple(row.id, row.default, row.`class`))
        assertEquals(nested, row.value)
        assertEquals(listOf("box", "reader", "d", "w", "c"), listOf(row.value?.container, row.value?.r, row.value?.child?.default, row.value?.child?.`when`, row.value?.child?.`class`))
        val expected = """{"default":"nested","when":"now","class":"kind","container":"box","r":"reader","child":{"default":"d","when":"w","class":"c"}}"""
        assertEquals(parseJson(expected), dbJson.encodeToJsonElement(KeywordValue.serializer(), nested))
        val transport = MockTransport()
        reopen(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"KeywordCall","document":"query KeywordCall(${'$'}class: String!) { keywordCall(class: ${'$'}class) { default when container r } }"}]""")
        transport.enqueue("KeywordCall", """{"data":{"keywordCall":{"default":"answer","when":"now","container":"box","r":"reader"}}}""")
        val answer = Operations.`when`(`class` = "request")
        assertEquals(listOf("answer", "now", "box", "reader"), listOf(answer.default, answer.`when`, answer.container, answer.r))
        assertEquals(parseJson("""{"class":"request"}"""), parseJson(transport.calls.single().variablesJson))
    }
}

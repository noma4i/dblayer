package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class CloudSpecTest {
    private val models = (parseJson(modelsJSON) as JsonArray).map { it.jsonObject }

    private fun cloud(key: String): List<String>? =
        (models.first { (it.getValue("key") as JsonPrimitive).content == key }["cloud"] as JsonArray?)?.map { (it as JsonPrimitive).content }

    @Test
    fun modelsJsonCarriesTheExpandedCloudList() {
        assertEquals(listOf("title", "pinned"), cloud("CloudDraft"))
        assertEquals(listOf("chatId", "readUntil"), cloud("CloudCursor"))
        assertEquals(listOf("tone"), cloud("CloudNote"))
        assertNull(cloud("StorageRow"))
    }
}

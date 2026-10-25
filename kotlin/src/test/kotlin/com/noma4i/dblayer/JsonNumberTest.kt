package com.noma4i.dblayer

import java.io.File
import kotlinx.serialization.json.jsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import com.noma4i.dblayer.models.Moment
import org.junit.Test

class JsonNumberTest {
    @Test
    fun invalidJSONCannotBeSentOrBecomeAnEmptyObject() {
        val variables = JsonObject(mapOf("value" to JsonPrimitive(Double.POSITIVE_INFINITY)))
        val invalid = listOf<() -> Unit>(
            { Query("Moment", Moment, relation = "feed", params = variables).json },
            { UploadRequest("unused", "CreateMoment", variables = variables).json },
            { parseJsonObject("null") },
            { parseJsonObject("[]") },
        )
        for (encode in invalid) assertThrows(SerializationException::class.java) { encode() }
    }

    @Test
    fun integersAboveTheDoubleRangeKeepEveryDigit() {
        val value = parseJson(File("../fixtures/numbers.json").readText()).jsonObject
        val texts = value.mapValues { (_, element) -> element.toString() }
        assertEquals(mapOf("big" to "9007199254740993", "negative" to "-9007199254740993", "max" to "9223372036854775807", "whole" to "3", "real" to "2.5", "list" to "[9007199254740993,1.5]"), texts)
        assertEquals("9007199254740993", Cell.Int(9007199254740993L).asJson().toString())
        assertEquals("9007199254740993", jsonValue(9007199254740993L).toString())
    }
}

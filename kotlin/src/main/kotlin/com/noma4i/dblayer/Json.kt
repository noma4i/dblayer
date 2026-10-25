package com.noma4i.dblayer

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.encodeToJsonElement

val dbJson = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
    coerceInputValues = true
}

inline fun <reified Value> jsonValue(value: Value): JsonElement = dbJson.encodeToJsonElement(value)

suspend fun <Result> offCaller(block: suspend () -> Result): Result = withContext(Dispatchers.Default) { block() }

fun JsonElement.jsonText(): String = dbJson.encodeToString(JsonElement.serializer(), this)

fun parseJson(text: String): JsonElement = dbJson.parseToJsonElement(text)

fun parseJsonObject(text: String): JsonObject = parseJson(text) as? JsonObject ?: throw SerializationException("JSON body is not an object")

object IDText : KSerializer<String> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("IDText", PrimitiveKind.STRING)

    override fun deserialize(decoder: Decoder): String {
        val element = (decoder as JsonDecoder).decodeJsonElement()
        if (element !is JsonPrimitive || !element.isString && element.longOrNull == null) {
            throw SerializationException("id must be a string or an integer")
        }
        return element.content
    }

    override fun serialize(encoder: Encoder, value: String) = encoder.encodeString(value)
}

object IDTexts : KSerializer<List<String>> by ListSerializer(IDText)

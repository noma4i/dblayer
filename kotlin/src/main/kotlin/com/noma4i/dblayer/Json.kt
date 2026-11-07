package com.noma4i.dblayer

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.encodeToJsonElement

val dbJson = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
}

inline fun <reified Value> jsonValue(value: Value): JsonElement = dbJson.encodeToJsonElement(value)

suspend fun <Result> offCaller(block: suspend () -> Result): Result = withContext(Dispatchers.Default) { block() }

fun JsonElement.jsonText(): String = dbJson.encodeToString(JsonElement.serializer(), this)

fun parseJson(text: String): JsonElement = dbJson.parseToJsonElement(text)

fun parseJsonObject(text: String): JsonObject = parseJson(text) as? JsonObject ?: throw SerializationException("JSON body is not an object")

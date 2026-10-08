package com.noma4i.dblayer

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.encodeToJsonElement

val dbJson = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
}

inline fun <reified Value> jsonValue(value: Value): JsonElement = dbJson.encodeToJsonElement(value)

fun parseJson(text: String): JsonElement = dbJson.parseToJsonElement(text)

fun parseJsonObject(text: String): JsonObject = parseJson(text) as? JsonObject ?: JsonObject(emptyMap())

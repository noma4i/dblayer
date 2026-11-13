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

/** Takes the place of the call in the order of its database on the thread of the caller, runs the call off the caller with the database of that place and gives the place back. A receipt holds the database of the caller, never the database of the place. */
@PublishedApi
internal suspend fun <Result> offCallerInOrder(database: Database, call: suspend (Database) -> Result): Result {
    val ordered = database.inOrder()
    try {
        return offCaller { call(ordered) }
    } finally {
        ordered.close()
    }
}

fun JsonElement.jsonText(): String = dbJson.encodeToString(JsonElement.serializer(), this)

fun parseJson(text: String): JsonElement = dbJson.parseToJsonElement(text)

fun parseJsonObject(text: String): JsonObject = parseJson(text) as? JsonObject ?: throw SerializationException("JSON body is not an object")

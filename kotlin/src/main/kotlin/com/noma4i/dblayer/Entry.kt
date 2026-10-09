package com.noma4i.dblayer

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

object DBlayer {
    /** Opens the subscriptions that models declare with `listen`. Call it after sign-in, stop it with `unlisten()`. */
    fun listen() = database.listen()
    fun unlisten() = database.unlisten()
    /** Reloads the stale relations that have readers. Call it when the app returns to the foreground. */
    suspend fun resume() = database.resume()

    lateinit var database: Database

    fun open(endpoint: String, headers: Map<String, String> = emptyMap(), modelsJson: String, path: String? = null, operations: List<GraphQLOperation> = emptyList()) =
        open(OkHttpTransport(endpoint, { headers }), modelsJson, path, operations)

    fun open(transport: Transport?, modelsJson: String, path: String? = null, operations: List<GraphQLOperation> = emptyList()) {
        val opened = Database.open(Config(path, null, 2000u), modelsJson, transport)
        val definitions = operations.map { JsonObject(mapOf("name" to JsonPrimitive(it.operationName), "document" to JsonPrimitive(it.document))) }
        if (transport != null) opened.registerOperations(JsonArray(definitions).toString())
        database = opened
    }
}

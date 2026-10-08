package com.noma4i.dblayer

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

object DBlayer {
    lateinit var database: Database

    fun open(endpoint: String, headers: Map<String, String> = emptyMap(), modelsJson: String, path: String? = null, operations: List<GraphQLOperation> = emptyList()) =
        open(OkHttpTransport(endpoint, { headers }), modelsJson, path, operations)

    fun open(transport: Transport?, modelsJson: String, path: String? = null, operations: List<GraphQLOperation> = emptyList()) {
        val opened = Database.open(Config(path, null, 2000u), modelsJson, transport)
        val definitions = operations.map { JsonObject(mapOf("name" to JsonPrimitive(it.operationName), "document" to JsonPrimitive(it.document))) }
        opened.registerOperations(JsonArray(definitions).toString())
        database = opened
    }
}

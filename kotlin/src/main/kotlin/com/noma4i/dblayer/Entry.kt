package com.noma4i.dblayer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

object DBlayer {
    /** Opens the subscriptions that models declare with `listen`. Call it after sign-in, stop it with `unlisten()`. */
    fun listen() = database.listen()
    fun unlisten() = database.unlisten()
    /** Reloads the stale relations that have readers. Call it when the app returns to the foreground. */
    suspend fun resume() {
        database.resume()
        EntityRegistries.resumeAll()
    }

    /** Ends the session: clears the stored data and drops every entity, lease and retained value. */
    suspend fun logout() {
        database.logout()
        EntityRegistries.resetAll()
    }

    /** The live subscriptions of the database: observed queries and subscriptions, for probes of how many a screen opens. */
    fun subscriptionCensus(): SubscriptionCensus = database.subscriptionCensus()

    /** What the open discarded: the reason the database file was recreated, and the models whose rows a schema change cleared. Empty when nothing was discarded. */
    fun openReport(): OpenReport = database.openReport()

    /** Sends one registered operation that no model owns and decodes the value at its root field. The generated `Operations` calls use it. */
    suspend fun <Answer> run(operation: String, variables: JsonObject, serializer: KSerializer<Answer>): Answer =
        dbJson.decodeFromString(serializer, database.run(operation, variables.toString()))

    suspend fun run(operation: String, variables: JsonObject) {
        database.run(operation, variables.toString())
    }

    lateinit var database: Database
    internal val databaseOrNull: Database? get() = if (::database.isInitialized) database else null

    suspend fun serverConfig(encrypted: EncryptedConfig, platform: AppPlatform, version: String, build: String? = null, http: EncryptedHttp? = null, retryListener: RetryListener? = null): ServerConfig =
        fetchServerConfig(encrypted, platform, version, build, http ?: OkHttpEncryptedHttp(encrypted.origin), retryListener)

    fun open(
        endpoint: String? = null,
        headers: Map<String, String> = emptyMap(),
        encrypted: EncryptedConfig? = null,
        credentials: CredentialStore? = null,
        transport: Transport? = null,
        modelsJson: String,
        path: String? = null,
        operations: List<GraphQLOperation> = emptyList(),
        http: EncryptedHttp? = null,
    ) {
        val selectedTransport = transport ?: (endpoint ?: encrypted?.origin)?.let { OkHttpTransport(it, { headers }, cablePath = encrypted?.endpoints?.cable) }
        install(Database.open(Config(path, null, 2000u), modelsJson, selectedTransport, encrypted, credentials, http ?: encrypted?.let { OkHttpEncryptedHttp(it.origin) }), selectedTransport, operations)
    }

    internal fun install(opened: Database, transport: Transport?, operations: List<GraphQLOperation>) {
        val definitions = operations.map { JsonObject(mapOf("name" to JsonPrimitive(it.operationName), "document" to JsonPrimitive(it.document))) }
        if (transport != null) opened.registerOperations(JsonArray(definitions).toString())
        database = opened
    }
}

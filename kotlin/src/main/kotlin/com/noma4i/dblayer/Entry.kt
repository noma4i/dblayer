package com.noma4i.dblayer

import android.content.Context
import java.util.IdentityHashMap
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

object DBlayer {
    private val opening = Mutex()
    private val opener = OpenDatabase()
    private val references = IdentityHashMap<Any, ULong>()
    private var nextReference = 1uL

    private fun reference(value: Any?): ULong = value?.let { references.getOrPut(it) { nextReference++ } } ?: 0uL

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

    suspend fun serverConfig(context: Context, encrypted: EncryptedConfig, platform: AppPlatform, version: String, build: String? = null, http: EncryptedHttp? = null, retryListener: RetryListener? = null): ServerConfig =
        fetchServerConfig(encrypted, platform, version, build, http ?: OkHttpEncryptedHttp(encrypted.origin), retryListener, Reachability.start(context))

    suspend fun open(
        context: Context,
        endpoint: String? = null,
        headers: Map<String, String> = emptyMap(),
        encrypted: EncryptedConfig? = null,
        credentials: CredentialStore? = null,
        transport: Transport? = null,
        modelsJson: String,
        path: String? = null,
        operations: List<GraphQLOperation> = emptyList(),
        http: EncryptedHttp? = null,
    ) = opening.withLock {
        val previousReferences = IdentityHashMap(references)
        try {
            val definitions = operations.map { JsonObject(mapOf("name" to JsonPrimitive(it.operationName), "document" to JsonPrimitive(it.document))) }
            val options = OpenOptions(endpoint, headers.toMap(), JsonArray(definitions).toString(), listOf(reference(credentials), reference(transport), reference(http)))
            val selectedTransport = transport ?: (endpoint ?: encrypted?.origin)?.let { OkHttpTransport(it, { options.headers }, cablePath = encrypted?.endpoints?.cable) }
            val opened = opener.open(Config(path, null, 2000u), modelsJson, options, selectedTransport, encrypted, credentials, http ?: encrypted?.let { OkHttpEncryptedHttp(it.origin) })
            Reachability.start(context)
            Reachability.connect(opened.onlineSink())
            database = opened
        } catch (error: Throwable) {
            references.clear()
            references.putAll(previousReferences)
            throw error
        }
    }

    internal fun install(opened: Database, transport: Transport?, operations: List<GraphQLOperation>) {
        val definitions = operations.map { JsonObject(mapOf("name" to JsonPrimitive(it.operationName), "document" to JsonPrimitive(it.document))) }
        if (transport != null) opened.registerOperations(JsonArray(definitions).toString())
        database = opened
    }
}

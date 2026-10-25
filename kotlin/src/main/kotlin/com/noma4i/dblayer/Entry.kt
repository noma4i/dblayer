package com.noma4i.dblayer

import android.content.Context
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

object DBlayer {
    private val opening = Mutex()
    private val opener = OpenDatabase()
    private var opened: Database? = null

    /** Opens the subscriptions that models declare with `listen`. Call it after sign-in, stop it with `unlisten()`. */
    fun listen() = database.listen()
    fun unlisten() = database.unlisten()
    /** Reloads the stale relations that have readers. The library already reloads stale windows when the app returns to the foreground and when the network returns. Call this for a manual refresh and after sign-in: it also restarts stopped subscriptions and singleton entities. */
    suspend fun resume() {
        offCaller { database.resume() }
        EntityRegistries.resumeAll()
    }

    /** Ends the session: clears the stored data. Entities that readers hold become empty and receive the next session's rows. */
    suspend fun logout() {
        offCaller { database.logout() }
        EntityRegistries.resetAll()
    }

    /** Closes the database. Every entity becomes closed, and `open` may open the database again. Meant for tests and the end of the process. */
    fun close() {
        EntityRegistries.closeAll()
        ForegroundEvents.stop()
        opened?.shutdown()
        opened = null
    }

    /** The live subscriptions of the database: observed queries and subscriptions, for probes of how many a screen opens. */
    fun subscriptionCensus(): SubscriptionCensus = database.subscriptionCensus()

    /** What the entity observers did: facts told, deliveries skipped as not newer than the row, reads that failed. A failed read leaves the id dirty and the core reads it again. */
    fun entityCensus(): EntityCensus = database.entityCensus()

    /** Lease calls the core refused and rows that failed to decode for an entity, for probes that tell a refused lease from a lost delivery. */
    fun entityLeaseFailures(): Int = EntityRegistries.leaseFailures()

    /** What the open discarded: the reason the database file was recreated, and the models whose rows a schema change cleared. Empty when nothing was discarded. */
    fun openReport(): OpenReport = database.openReport()

    /** Sends one registered operation that no model owns and decodes the value at its root field. The generated `Operations` calls use it. */
    suspend fun <Answer> run(operation: String, variables: JsonObject, serializer: KSerializer<Answer>): Answer =
        offCaller { dbJson.decodeFromString(serializer, database.run(operation, variables.jsonText())) }

    suspend fun run(operation: String, variables: JsonObject) {
        offCaller { database.run(operation, variables.jsonText()) }
    }

    /** The one database of the process. Set by `open`, empty again after `close`. */
    var database: Database
        get() = opened ?: throw IllegalStateException("database is not open; call DBlayer.open once at app start")
        internal set(value) { opened = value }

    suspend fun serverConfig(context: Context, encrypted: EncryptedConfig, platform: AppPlatform, version: String, build: String? = null, http: EncryptedHttp? = null, retryListener: RetryListener? = null): ServerConfig =
        fetchServerConfig(encrypted, platform, version, build, http ?: OkHttpEncryptedHttp(encrypted.origin), retryListener, Reachability.start(context))

    suspend fun open(
        context: Context,
        endpoint: String? = null,
        headers: () -> Map<String, String> = { emptyMap() },
        encrypted: EncryptedConfig? = null,
        credentials: CredentialStore? = null,
        transport: Transport? = null,
        modelsJson: String,
        path: String? = null,
        operations: List<GraphQLOperation> = emptyList(),
        uploads: UploadOperations? = null,
        http: EncryptedHttp? = null,
    ) = opening.withLock {
        val definitions = operations.map { JsonObject(mapOf("name" to JsonPrimitive(it.operationName), "document" to JsonPrimitive(it.document))) }
        val options = OpenOptions(JsonArray(definitions).jsonText(), uploads?.wire())
        val selectedTransport = transport ?: (endpoint ?: encrypted?.origin)?.let { OkHttpTransport(it, headers, cablePath = encrypted?.endpoints?.cable) }
        Reachability.start(context)
        val made = opener.open(Config(path, null, 2000u), modelsJson, options, selectedTransport, encrypted, credentials, http ?: encrypted?.let { OkHttpEncryptedHttp(it.origin) })
        val sink = made.onlineSink()
        Reachability.connect(sink)
        ForegroundEvents.start(sink)
        database = made
    }

    internal suspend fun openOnClock(config: Config, modelsJson: String, transport: Transport?, uploads: UploadOperations?, clock: TestClock): Database = opening.withLock { opener.openWithClock(config, modelsJson, transport, uploads?.wire(), clock) }

    internal fun install(opened: Database, transport: Transport?, operations: List<GraphQLOperation>) {
        val definitions = operations.map { JsonObject(mapOf("name" to JsonPrimitive(it.operationName), "document" to JsonPrimitive(it.document))) }
        if (transport != null) opened.registerOperations(JsonArray(definitions).jsonText())
        database = opened
    }
}

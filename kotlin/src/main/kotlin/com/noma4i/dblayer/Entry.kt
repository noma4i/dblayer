package com.noma4i.dblayer

import android.content.Context
import kotlinx.coroutines.CompletableDeferred
import kotlinx.serialization.KSerializer
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

object DBlayer {
    private val opening = Mutex()
    private val opener = OpenDatabase()
    private var opened: Database? = null
    private var ready = CompletableDeferred<Database>()

    internal suspend fun awaitDatabase(): Database = ready.await()

    internal fun openedDatabase(): Database? = opened

    fun listen() = database.listen()
    fun unlisten() = database.unlisten()
    /** Reloads the stale relations that have readers. The library already reloads stale windows when the app returns to the foreground, when the network returns and after a sign-in. Call this for a manual refresh: it also restarts stopped subscriptions and singleton entities. */
    suspend fun resume() {
        offCaller { database.resume() }
        EntityRegistries.resumeAll()
    }

    suspend fun logout() {
        offCaller { database.logout() }
        EntityRegistries.resetAll()
    }

    /** Reloads every `preload` scope now, fresh or not, and returns when all have settled. Offline it throws without a request. When some scopes fail the error names them. Call it for pull to refresh instead of refreshing each reference list. */
    suspend fun refreshPreloads() = offCaller { database.refreshPreloads() }

    suspend fun retryAll(): ULong = offCaller { database.retryAll() }

    suspend inline fun <reified Body, reified Answer> sessionCall(operation: String, body: Body): Answer =
        offCaller { dbJson.decodeFromString<Answer>(database.sessionCall(operation, dbJson.encodeToString(body))) }.also { resumeEntities() }

    suspend inline fun <reified Body> sessionSend(operation: String, body: Body) {
        offCaller { database.sessionCall(operation, dbJson.encodeToString(body)) }
        resumeEntities()
    }

    @PublishedApi internal fun resumeEntities() = EntityRegistries.resumeAll()

    suspend inline fun <reified Answer> sessionResume(): Answer? =
        offCaller { database.sessionResume()?.let { dbJson.decodeFromString<Answer>(it) } }

    fun sessionReset() = database.sessionReset()

    fun close() {
        EntityRegistries.closeAll()
        ForegroundEvents.stop()
        PowerSaving.stop()
        opened?.shutdown()
        opened = null
        ready = CompletableDeferred()
    }

    fun subscriptionCensus(): SubscriptionCensus = database.subscriptionCensus()

    fun entityCensus(): EntityCensus = database.entityCensus()

    fun entityLeaseFailures(): Int = EntityRegistries.leaseFailures()

    fun openReport(): OpenReport = database.openReport()

    suspend fun <Answer> run(operation: String, variables: JsonObject, serializer: KSerializer<Answer>): Answer =
        offCaller { dbJson.decodeFromString(serializer, database.run(operation, variables.jsonText())) }

    suspend fun run(operation: String, variables: JsonObject) {
        offCaller { database.run(operation, variables.jsonText()) }
    }

    var database: Database
        get() = opened ?: throw IllegalStateException("database is not open; call DBlayer.open once at app start")
        internal set(value) { opened = value }

    suspend fun serverConfig(context: Context, encrypted: EncryptedConfig, platform: AppPlatform, version: String, build: String? = null, http: EncryptedHttp? = null, retries: RetryProgress? = null): ServerConfig =
        fetchServerConfig(encrypted, platform, version, build, http ?: OkHttpEncryptedHttp(encrypted.origin), retries, Reachability.start(context))

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
        retries: RetryProgress? = null,
        uploadNotification: UploadNotification,
    ) = opening.withLock {
        try {
            openLocked(context, endpoint, headers, encrypted, credentials, transport, modelsJson, path, operations, uploads, http, retries, uploadNotification)
        } catch (error: Throwable) {
            ready.completeExceptionally(error)
            ready = CompletableDeferred()
            throw error
        }
    }

    private suspend fun openLocked(
        context: Context,
        endpoint: String?,
        headers: () -> Map<String, String>,
        encrypted: EncryptedConfig?,
        credentials: CredentialStore?,
        transport: Transport?,
        modelsJson: String,
        path: String?,
        operations: List<GraphQLOperation>,
        uploads: UploadOperations?,
        http: EncryptedHttp?,
        retries: RetryProgress?,
        uploadNotification: UploadNotification,
    ) {
        UploadNotifications.store(context, uploadNotification)
        val definitions = operations.map { JsonObject(mapOf("name" to JsonPrimitive(it.operationName), "document" to JsonPrimitive(it.document))) }
        val options = OpenOptions(JsonArray(definitions).jsonText(), uploads?.wire(), PowerSaving.saving(context), PowerSaving.lowRam(context), true)
        val selectedTransport = transport ?: (endpoint ?: encrypted?.origin)?.let { OkHttpTransport(it, headers, cablePath = encrypted?.endpoints?.cable) }
        Reachability.start(context)
        val made = opener.open(Config(path, null, 2000u), modelsJson, options, selectedTransport, encrypted, credentials, http ?: encrypted?.let { OkHttpEncryptedHttp(it.origin) }, retries)
        val sink = made.onlineSink()
        Reachability.connect(sink)
        ForegroundEvents.start(sink)
        PowerSaving.start(context, sink)
        made.setUploadAcceptedSink(UploadAcceptedStart(context.applicationContext, made))
        database = made
        ready.complete(made)
    }

    internal suspend fun openOnClock(config: Config, modelsJson: String, transport: Transport?, uploads: UploadOperations?, clock: TestClock, preload: Boolean): Database = opening.withLock { opener.openWithClock(config, modelsJson, transport, uploads?.wire(), clock, preload) }

    internal fun install(opened: Database, transport: Transport?, operations: List<GraphQLOperation>) {
        val definitions = operations.map { JsonObject(mapOf("name" to JsonPrimitive(it.operationName), "document" to JsonPrimitive(it.document))) }
        if (transport != null) opened.registerOperations(JsonArray(definitions).jsonText())
        database = opened
    }
}

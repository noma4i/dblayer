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
    @Volatile private var opened: Database? = null
    @Volatile private var ready = CompletableDeferred<Database>()

    internal suspend fun awaitDatabase(): Database = ready.await()

    internal fun openedDatabase(): Database? = opened

    suspend inline fun <reified Body, reified Answer> sessionCall(operation: String, body: Body): Answer =
        offCaller { dbJson.decodeFromString<Answer>(database.sessionCall(operation, dbJson.encodeToString(body))) }

    suspend inline fun <reified Body> sessionSend(operation: String, body: Body) {
        offCaller { database.sessionCall(operation, dbJson.encodeToString(body)) }
    }

    /** Ends the session: work of the old session ends, the credential and the data of the account go. Call it for every sign-out; the library clears the data itself. */
    suspend fun sessionReset() {
        offCaller { database.sessionReset() }
    }

    /** Closes the database. Every request, load and subscription ends, and the call returns after the library released its resources. */
    suspend fun close() {
        val closing = opening.withLock {
            ForegroundEvents.stop()
            PowerSaving.stop()
            Reachability.disconnect()
            opened
        }
        closing?.let { offCaller { it.closeDatabase() } }
    }

    fun subscriptionCensus(): SubscriptionCensus = database.subscriptionCensus()

    fun openReport(): OpenReport = database.openReport()

    suspend fun <Answer> run(operation: String, variables: JsonObject, serializer: KSerializer<Answer>, account: AccountScope? = null): Answer =
        onAccount(account) { database, epoch -> dbJson.decodeFromString(serializer, database.run(epoch, operation, variables.jsonText())) }

    suspend fun run(operation: String, variables: JsonObject, account: AccountScope? = null) {
        onAccount(account) { database, epoch -> database.run(epoch, operation, variables.jsonText()) }
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
        http: EncryptedHttp? = null,
        retries: RetryProgress? = null,
        uploadNotification: UploadNotification,
    ) = opening.withLock {
        try {
            openLocked(context, endpoint, headers, encrypted, credentials, transport, modelsJson, path, operations, http, retries, uploadNotification)
        } catch (error: Throwable) {
            if (ready.completeExceptionally(error)) ready = CompletableDeferred()
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
        http: EncryptedHttp?,
        retries: RetryProgress?,
        uploadNotification: UploadNotification,
    ) {
        checkUploadPermission(android.os.Build.VERSION.SDK_INT) { context.packageManager.getPackageInfo(context.packageName, android.content.pm.PackageManager.GET_PERMISSIONS).requestedPermissions.orEmpty().toList() }
        UploadNotifications.store(context, uploadNotification)
        val definitions = operations.map { JsonObject(mapOf("name" to JsonPrimitive(it.operationName), "document" to JsonPrimitive(it.document))) }
        val options = OpenOptions(operations = JsonArray(definitions).jsonText(), powerSaving = PowerSaving.saving(context), lowRam = PowerSaving.lowRam(context))
        val selectedTransport = transport ?: (endpoint ?: encrypted?.origin)?.let { OkHttpTransport(it, headers) }
        Reachability.start(context)
        val made = opener.open(Config(path, null, 2000u), modelsJson, options, selectedTransport, encrypted, credentials, http ?: encrypted?.let { OkHttpEncryptedHttp(it.origin) }, retries)
        val sink = made.onlineSink()
        Reachability.connect(sink)
        ForegroundEvents.start(sink)
        PowerSaving.start(context, sink)
        made.setRoundJobs(UploadRounds(context.applicationContext, made))
        database = made
        if (ready.isCompleted) ready = CompletableDeferred()
        ready.complete(made)
    }

    internal suspend fun openOnClock(config: Config, modelsJson: String, transport: Transport?, clock: TestClock, encrypted: EncryptedConfig? = null, credentials: CredentialStore? = null, http: EncryptedHttp? = null, retries: RetryProgress? = null): Database = opening.withLock { opener.openWithClock(config, modelsJson, transport, clock, encrypted, credentials, http, retries) }

    internal fun install(opened: Database, transport: Transport?, operations: List<GraphQLOperation>) {
        val definitions = operations.map { JsonObject(mapOf("name" to JsonPrimitive(it.operationName), "document" to JsonPrimitive(it.document))) }
        if (transport != null) opened.registerOperations(JsonArray(definitions).jsonText())
        database = opened
    }
}

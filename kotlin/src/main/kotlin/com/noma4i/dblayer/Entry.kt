package com.noma4i.dblayer

import android.content.Context
import kotlinx.serialization.KSerializer
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.JsonObject

object DBlayer {
    private val opener = OpenDatabase()
    @Volatile internal var opened: Database? = null

    internal fun openedDatabase(): Database? = opened

    suspend inline fun <reified Body, reified Answer> sessionCall(operation: String, body: Body): Answer =
        offCaller { dbJson.decodeFromString<Answer>(database.sessionCall(operation, dbJson.encodeToString(body))) }

    /** Ends the session: work of the old session ends, the credential and the data of the account go. Call it for every sign-out; the library clears the data itself. */
    suspend fun sessionReset() {
        offCaller { database.sessionReset() }
    }

    /** Closes the database. Every request, load and subscription ends, and the call returns after the library released its resources. A call on the closed database, a second close included, gets the closed error of the core. */
    suspend fun close() {
        val database = this.database
        offCaller { database.closeDatabase() }
    }

    suspend fun subscriptionCensus(): SubscriptionCensus = database.subscriptionCensus()

    fun openReport(): OpenReport = database.openReport()

    suspend fun <Answer> run(operation: String, variables: JsonObject, serializer: KSerializer<Answer>, account: AccountScope? = null): Answer =
        onAccount(account) { database, epoch -> dbJson.decodeFromString(serializer, database.run(epoch, operation, variables.jsonText())) }

    suspend fun run(operation: String, variables: JsonObject, account: AccountScope? = null) {
        onAccount(account) { database, epoch -> database.run(epoch, operation, variables.jsonText()) }
    }

    var database: Database
        get() = opened ?: throw DbException.NotOpen("database is not open; call DBlayer.open once at app start")
        internal set(value) { opened = value }

    suspend fun serverConfig(context: Context, encrypted: EncryptedConfig, platform: AppPlatform, version: String, build: String? = null, http: EncryptedHttp? = null, retries: RetryProgress? = null): ServerConfig =
        fetchServerConfig(encrypted, platform, version, build, http ?: OkHttpEncryptedHttp(), retries, SystemObservers(context))

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
        uploadNotification: UploadNotification,
    ) {
        checkUploadPermission(android.os.Build.VERSION.SDK_INT) { context.packageManager.getPackageInfo(context.packageName, android.content.pm.PackageManager.GET_PERMISSIONS).requestedPermissions.orEmpty().toList() }
        UploadNotifications.store(context, uploadNotification)
        val ports = OpenPorts(transport ?: (endpoint ?: encrypted?.origin)?.let { OkHttpTransport(it, headers) }, encrypted, credentials, http ?: encrypted?.let { OkHttpEncryptedHttp(headers = headers) })
        val made = opener.open(Config(path, null), modelsJson, OpenOptions(definitions(operations)), ports, SystemObservers(context))
        made.setRoundJobs(UploadRounds(context.applicationContext, made))
        database = made
        UploadService.opened(made)
    }

    internal suspend fun openOnClock(config: Config, modelsJson: String, transport: Transport?, clock: TestClock, encrypted: EncryptedConfig? = null, credentials: CredentialStore? = null, http: EncryptedHttp? = null): Database = opener.openWithClock(config, modelsJson, OpenPorts(transport, encrypted, credentials, http), clock)

    internal suspend fun install(opened: Database, transport: Transport?, operations: List<GraphQLOperation>) {
        if (transport != null) opened.declareOperations(definitions(operations))
        database = opened
    }

    private fun definitions(operations: List<GraphQLOperation>): List<OperationDefinition> = operations.map { OperationDefinition(it.operationName, it.document) }
}

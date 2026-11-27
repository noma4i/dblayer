package com.noma4i.dblayer

import android.content.Context
import kotlinx.serialization.KSerializer
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.JsonObject

object DBlayer {
    internal val opener = OpenDatabase()

    internal fun openedDatabase(): Database? = try { opener.opened() } catch (_: DbException.NotOpen) { null }

    suspend inline fun <reified Body, reified Answer> sessionCall(operation: String, body: Body): Answer =
        offCallerInOrder(database) { dbJson.decodeFromString<Answer>(it.sessionCall(operation, dbJson.encodeToString(body))) }

    /** The answer of the session call that the killed process left running, taken once: `null` when there is none. */
    suspend fun interruptedSessionAnswer(): JsonObject? =
        offCallerInOrder(database) { it.interruptedSessionAnswer()?.let { dbJson.parseToJsonElement(it) as JsonObject } }

    /** Ends the session: work of the old session ends, the credential and the data of the account go. Call it for every sign-out; the library clears the data itself. */
    suspend fun sessionReset() {
        offCallerInOrder(database) { it.sessionReset() }
    }

    /** The session now: epoch, identity of its credential and phase. Read it again after a stream ends with the session or after `sessionCall` and `sessionReset`. */
    fun sessionStatus(): SessionStatus = database.sessionStatus()

    fun sessionRefusals(): kotlinx.coroutines.flow.Flow<SessionRefusal> = kotlinx.coroutines.flow.flow {
        val subscription = database.sessionRefusals()
        try {
            while (true) emit(subscription.next() ?: break)
        } finally {
            subscription.cancel()
        }
    }

    /** Closes the database. Every request, load and subscription ends, and the call returns after the library released its resources. A call on the closed database, a second close included, gets the closed error of the core. */
    suspend fun close() {
        val database = this.database
        offCaller { database.closeDatabase() }
    }

    suspend fun subscriptionCensus(): SubscriptionCensus = offCaller { database.subscriptionCensus() }

    fun openReport(): OpenReport = database.openReport()

    fun cloudStatus(): CloudStatus = database.cloudStatus()

    fun cloudChanges(): kotlinx.coroutines.flow.Flow<CloudStatus> = cloudChanges(database)

    suspend fun setCloudEnabled(enabled: Boolean): Boolean = database.setCloudEnabled(enabled)

    suspend fun cloudModels(): List<CloudModelStatus> = database.cloudModels()

    suspend fun cloudRecordStatus(model: String, rowId: String): CloudRecordStatus = database.cloudRecordStatus(model, rowId)

    suspend fun <Answer> call(operation: String, variables: JsonObject, serializer: KSerializer<Answer>, account: AccountScope? = null): Answer =
        onAccountOf(account) { _, database, epoch -> dbJson.decodeFromString(serializer, database.call(epoch, operation, variables.jsonText())) }

    suspend fun call(operation: String, variables: JsonObject, account: AccountScope? = null) {
        onAccountOf(account) { _, database, epoch -> database.call(epoch, operation, variables.jsonText()) }
    }

    @PublishedApi internal val database: Database
        get() = opener.opened()

    suspend fun serverConfig(context: Context, encrypted: EncryptedConfig, platform: AppPlatform, version: String, build: String? = null, http: EncryptedHttp? = null, retries: RetryProgress? = null): ServerConfig = offCaller {
        NetworkStatus(context)
        fetchServerConfig(encrypted, platform, version, build, http ?: OkHttpTransport(), retries, null)
    }

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
        preparer: Preparer? = null,
        cloud: CloudConfig? = null,
    ) {
        offCaller {
            val dataSync = dataSyncDeclared(android.os.Build.VERSION.SDK_INT) { context.packageManager.getPackageInfo(context.packageName, android.content.pm.PackageManager.GET_PERMISSIONS).requestedPermissions.orEmpty().toList() }
            UploadNotifications.store(context, uploadNotification)
            val ports = OpenPorts(transport, encrypted, credentials, http, endpoint, OkHttpTransport(headers), dataSync, preparer, cloud = cloud)
            NetworkStatus(context)
            val made = opener.open(Config(path), modelsJson, OpenOptions(definitions(operations)), ports, SystemObservers(context))
            made.setRoundJobs(UploadRounds(context.applicationContext, made))
            UploadService.opened(made)
        }
    }

    private fun definitions(operations: List<GraphQLOperation>): List<OperationDefinition> = operations.map { OperationDefinition(it.operationName, it.document) }
}

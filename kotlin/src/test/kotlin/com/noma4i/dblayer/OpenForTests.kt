package com.noma4i.dblayer

/** A test transport that forwards to a `MockTransport`: the session cable of the stand goes to that mock. */
interface WrapsMock { val base: MockTransport }

suspend fun openDatabase(config: Config, modelsJson: String, transport: Transport? = null, encrypted: EncryptedConfig? = null, credentials: CredentialStore? = null, http: EncryptedHttp? = null, operations: String = "[]"): Database =
    OpenDatabase().open(config, modelsJson, OpenOptions(withUploadSteps(operations, modelsJson), false, false), OpenPorts(transport, encrypted, credentials, http, null))

/** A cleanup close: a database that is already closed stays closed. */
suspend fun closeForCleanup() {
    try {
        DBlayer.close()
    } catch (_: DbException.Closed) {
    }
}

suspend fun adopt(database: Database): Database {
    try {
        DBlayer.close()
    } catch (_: DbException.Closed) {
    }
    DBlayer.database = database
    return database
}

private class StandCredential : CredentialStore {
    @Volatile var current: String? = null
    override fun get(): String? = current
    override fun set(credential: String) { current = credential }
    override fun clear() { current = null }
}

private val credentialsByPath = java.util.concurrent.ConcurrentHashMap<String, StandCredential>()

/** The secret outlives a process like the Keychain does: a database file opened again finds the credential of its stored session. */
private fun credentialOf(config: Config): StandCredential = config.path?.let { credentialsByPath.getOrPut(it) { StandCredential() } } ?: StandCredential()

const val SEND_MOMENT = """[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid text likedByMe media createdAt } }"}]"""

private const val SESSION_PROFILE = """{"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"}"""

/** The HTTP of the encrypted session answers from the mock transport, so the test database signs in the real way and every request still reaches the mock. */
private fun bridgeToTransport(transport: Transport): suspend (String, kotlinx.serialization.json.JsonObject) -> kotlinx.serialization.json.JsonObject = { path, body ->
    val json = kotlinx.serialization.json.Json
    when {
        path == "/api/auth/login_link" -> json.parseToJsonElement("""{"sessionId":"stand"}""") as kotlinx.serialization.json.JsonObject
        path == "/api/logout" -> json.parseToJsonElement("""{"revoked":true}""") as kotlinx.serialization.json.JsonObject
        else -> {
            val operation = (body["operationName"] as? kotlinx.serialization.json.JsonPrimitive)?.content ?: ""
            if (operation == "SessionProfile") {
                json.parseToJsonElement("""{"data":{"sessionProfile":{"id":"stand"}}}""") as kotlinx.serialization.json.JsonObject
            } else {
                val variables = body["variables"]?.toString() ?: "{}"
                val document = (body["query"] as? kotlinx.serialization.json.JsonPrimitive)?.content ?: ""
                json.parseToJsonElement(transport.query(TransportRequest(document, operation, variables, NetworkPurpose.STANDARD))) as kotlinx.serialization.json.JsonObject
            }
        }
    }
}

private fun standEncrypted() = EncryptedConfig("https://api.example.com", SealedServer.SERVER_KEY, SealedServer.SERVER_PIN, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("login_link" to EncryptedSessionOperation("/api/auth/login_link", EncryptedOperationClass.AUTHENTICATED))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel")

private fun withSessionProfile(operations: String) = if (operations.trim() == "[]") "[$SESSION_PROFILE]" else operations.dropLast(1) + ",$SESSION_PROFILE]"

private suspend fun signIn(database: Database, restored: Boolean = false) {
    database.setOnline(true)
    if (!restored) database.sessionCall("login_link", """{"account":"stand"}""")
    DBlayer.run("SessionProfile", kotlinx.serialization.json.JsonObject(emptyMap()))
}

/** After a session reset the account signs in again through the same stand. */
suspend fun loginAgain() {
    signIn(DBlayer.database)
}

/** A transport signs in through the real session path, and its queries and mutations ride the session HTTP. */
suspend fun reopen(config: Config, modelsJson: String, transport: Transport? = null, operations: String = "[]"): Database {
    val mock = transport ?: return adopt(openDatabase(config, modelsJson, transport, operations = operations))
    val server = SealedServer(answer = bridgeToTransport(mock))
    val host = mock as? MockTransport ?: (mock as? WrapsMock)?.base
    host?.cable = EncryptedCablePeer(server)
    val credential = credentialOf(config)
    val restored = credential.get() != null
    val database = adopt(openDatabase(config, modelsJson, mock, standEncrypted(), credential, server, withSessionProfile(operations)))
    signIn(database, restored)
    return database
}

/** The same session stand on a clock that only `advance` moves. */
@OptIn(DBlayerTestingApi::class)
suspend fun reopenClocked(modelsJson: String, transport: MockTransport, operations: String = "[]", path: String? = null, startMillis: Long = System.currentTimeMillis()): TestClock {
    val server = SealedServer(answer = bridgeToTransport(transport))
    transport.cable = EncryptedCablePeer(server)
    try {
        DBlayer.close()
    } catch (_: DbException.Closed) {
    }
    val credential = credentialOf(Config(path, null, 2000u))
    val restored = credential.get() != null
    val clock = DBlayer.openTesting(transport, modelsJson, path = path, startMillis = startMillis, encrypted = standEncrypted(), credentials = credential, http = server)
    DBlayer.database.registerOperations(withSessionProfile(operations))
    signIn(DBlayer.database, restored)
    return clock
}

/** A database whose plain transport holds a session and sees no network: a sent row stays queued. A send needs a session. */
suspend fun reopenSigned(config: Config, modelsJson: String, operations: String = SEND_MOMENT): Database {
    val database = reopen(config, modelsJson, MockTransport(), operations)
    database.setOnline(false)
    return database
}

/** The schema with one server subscription: a schema holds one subscription, so a test of a model-level subscription runs on a schema that declares only that one. */
fun schema(onlySubscription: String, source: String = com.noma4i.dblayer.models.modelsJSON): String {
    fun keep(entries: kotlinx.serialization.json.JsonElement?) = entries?.let { all -> kotlinx.serialization.json.JsonArray((all as kotlinx.serialization.json.JsonArray).filter { ((it as kotlinx.serialization.json.JsonObject)["subscription"] as? kotlinx.serialization.json.JsonPrimitive)?.content == onlySubscription }) }
    return kotlinx.serialization.json.JsonArray((kotlinx.serialization.json.Json.parseToJsonElement(source) as kotlinx.serialization.json.JsonArray).map { model ->
        val fields = (model as kotlinx.serialization.json.JsonObject).filterKeys { it != "change_feed" }.toMutableMap()
        keep(fields["listens"])?.let { fields["listens"] = it }
        (fields["relations"] as? kotlinx.serialization.json.JsonArray)?.let { relations ->
            fields["relations"] = kotlinx.serialization.json.JsonArray(relations.map { relation ->
                val parts = (relation as kotlinx.serialization.json.JsonObject).toMutableMap()
                keep(parts["updates"])?.let { parts["updates"] = it }
                kotlinx.serialization.json.JsonObject(parts)
            })
        }
        kotlinx.serialization.json.JsonObject(fields)
    }).toString()
}

private val UPLOAD_STEP_OPERATIONS = listOf(
    "CreateBlob" to "mutation CreateBlob { createBlob { url headers signedId blobId } }",
    "InitMultipart" to "mutation InitMultipart { initMultipart { key uploadId } }",
    "PresignParts" to "mutation PresignParts { presignParts { parts { partNumber url } } }",
    "CompleteMultipart" to "mutation CompleteMultipart { completeMultipart { signedId blobId } }",
    "AbortMultipart" to "mutation AbortMultipart { abortMultipart { ok } }",
    "CreateAsset" to "mutation CreateAsset { createAsset { asset { id clientKey size position } } }",
)

/** The models declare the upload steps once, so every open registers the operations of the steps. */
fun withUploadSteps(operations: String, models: String): String {
    if (!models.contains("upload_steps") || !operations.endsWith("]")) return operations
    val missing = UPLOAD_STEP_OPERATIONS.filter { !operations.contains("\"name\":\"${it.first}\"") }
    if (missing.isEmpty()) return operations
    val added = missing.joinToString(",") { """{"name":"${it.first}","document":"${it.second}"}""" }
    return operations.dropLast(1) + (if (operations == "[]") "" else ",") + added + "]"
}

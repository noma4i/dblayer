package com.noma4i.dblayer

@Volatile private var standServer: SealedServer? = null

suspend fun registerStandOperations(database: Database, defsJson: String) {
    standServer?.registerDocuments(defsJson)
    database.registerOperations(defsJson)
}

/** A test transport that forwards to a `MockTransport`: the session cable of the stand goes to that mock. */
interface WrapsMock { val base: MockTransport }

suspend fun openDatabase(config: Config, modelsJson: String, transport: Transport? = null, encrypted: EncryptedConfig? = null, credentials: CredentialStore? = null, http: EncryptedHttp? = null, operations: String = "[]", preparer: Preparer? = ScriptedPreparer()): Database {
    standServer = null
    DBlayer.openedDatabase()?.closeDatabase()
    return DBlayer.opener.open(config, withRawCalls(modelsJson), OpenOptions(definitionsOf(withUploadSteps(operations, modelsJson))), OpenPorts(transport, encrypted, credentials, http, SCRIPTED_ENDPOINT, preparer = preparer, partialOperations = true), null)
}

fun withRawCalls(source: String, names: List<String> = listOf("SessionProfile", "PurchaseCreate", "RecordAlbumSend")): String {
    val fields = kotlinx.serialization.json.Json.parseToJsonElement(source) as kotlinx.serialization.json.JsonObject
    val declared = (fields["operations"] as? kotlinx.serialization.json.JsonArray).orEmpty()
    val known = declared.map { (it as kotlinx.serialization.json.JsonObject)["name"] }.toSet()
    val raw = names.filter { kotlinx.serialization.json.JsonPrimitive(it) !in known }.map {
        kotlinx.serialization.json.JsonObject(mapOf("name" to kotlinx.serialization.json.JsonPrimitive(it), "returns" to kotlinx.serialization.json.JsonObject(mapOf("name" to kotlinx.serialization.json.JsonPrimitive(it), "kind" to kotlinx.serialization.json.JsonObject(mapOf("type" to kotlinx.serialization.json.JsonPrimitive("raw")))))))
    }
    return kotlinx.serialization.json.JsonObject(fields + ("operations" to kotlinx.serialization.json.JsonArray(declared + raw))).toString()
}

private fun definitionsOf(operations: String): List<OperationDefinition> =
    (kotlinx.serialization.json.Json.parseToJsonElement(operations) as kotlinx.serialization.json.JsonArray).map { entry ->
        val operation = entry as kotlinx.serialization.json.JsonObject
        OperationDefinition((operation["name"] as kotlinx.serialization.json.JsonPrimitive).content, (operation["document"] as kotlinx.serialization.json.JsonPrimitive).content)
    }

/** A cleanup close: a database that is already closed stays closed. */
suspend fun closeForCleanup() {
    try {
        DBlayer.close()
    } catch (_: DbException.Closed) {
    } catch (_: DbException.NotOpen) {
    }
}

suspend fun adopt(database: Database): Database {
    ChangePump.start(database)
    return database
}

private class StandCredential : CredentialStore {
    val contract = ServerContract()
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
fun bridgeToTransport(transport: ScriptedTransport): suspend (String, kotlinx.serialization.json.JsonObject) -> kotlinx.serialization.json.JsonObject {
    val json = kotlinx.serialization.json.Json
    return { path, body ->
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
                    val request = TransportRequest(document, operation, variables, kotlin.coroutines.coroutineContext[RequestPurpose]?.value ?: NetworkPurpose.STANDARD, kotlin.coroutines.coroutineContext[ScriptedWire]?.author ?: Author.SESSION, kotlin.coroutines.coroutineContext[ScriptedWire]?.requestId ?: 0uL, kotlin.coroutines.coroutineContext[ScriptedWire]?.attempt ?: 0u)
                    json.parseToJsonElement(if (document.trimStart().startsWith("mutation")) transport.mutate(request) else transport.query(request)) as kotlinx.serialization.json.JsonObject
                }
            }
        }
    }
}

private fun standEncrypted() = EncryptedConfig("https://api.example.com", SealedServer.SERVER_KEY, SealedServer.SERVER_PIN, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("login_link" to EncryptedSessionOperation("/api/auth/login_link", EncryptedOperationClass.AUTHENTICATED))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel")

private fun withSessionProfile(operations: String) = if (operations.trim() == "[]") "[$SESSION_PROFILE]" else operations.dropLast(1) + ",$SESSION_PROFILE]"

/** The platform reports online the moment the database opens, before the operations are declared. */
class OnlineAtOpen : PlatformObserver {
    override fun `start`(sink: OnlineSink) { sink.setOnline(true) }
    override fun `stop`() {}
}

/** The platform reports nothing at open: the test sets online itself after the operations are declared. */
class SilentPlatform : PlatformObserver {
    override fun `start`(sink: OnlineSink) {}
    override fun `stop`() {}
}

private suspend fun signIn(database: Database, restored: Boolean = false, setsOnline: Boolean = true) {
    if (setsOnline) database.setOnline(true)
    if (!restored) database.sessionCall("login_link", """{"account":"stand"}""")
    DBlayer.call("SessionProfile", kotlinx.serialization.json.JsonObject(emptyMap()))
}

/** After a session reset the account signs in again through the same stand. */
suspend fun loginAgain() {
    signIn(DBlayer.database)
}

/** A transport signs in through the real session path, and its queries and mutations ride the session HTTP. By default the operations are declared at open and online follows in the sign-in. With `onlineAtOpen` the platform reports online at open and the operations are declared after it, except the upload steps the core requires at open. */
suspend fun openSigned(config: Config, modelsJson: String, transport: Transport? = null, operations: String = "[]", onlineAtOpen: Boolean = false, preparer: Preparer? = ScriptedPreparer()): Database {
    val mock = transport as? ScriptedTransport ?: return adopt(openDatabase(config, modelsJson, transport, operations = operations, preparer = preparer))
    val credential = credentialOf(config)
    val server = SealedServer(contract = credential.contract, answer = bridgeToTransport(mock))
    server.registerDocuments(withSessionProfile(withUploadSteps(operations, modelsJson)))
    val host = mock as? MockTransport ?: (mock as? WrapsMock)?.base
    host?.cable = EncryptedCablePeer(server)
    val restored = credential.get() != null
    val database = if (onlineAtOpen) {
        val declared = kotlinx.serialization.json.Json.parseToJsonElement(withUploadSteps(operations, modelsJson)) as kotlinx.serialization.json.JsonArray
        val steps = UPLOAD_STEP_OPERATIONS.map { it.first }.toSet()
        val (atOpen, later) = declared.partition { ((it as kotlinx.serialization.json.JsonObject)["name"] as kotlinx.serialization.json.JsonPrimitive).content in steps }
        DBlayer.openedDatabase()?.closeDatabase()
        val opened = adopt(DBlayer.opener.open(config, withRawCalls(modelsJson), OpenOptions(definitionsOf(kotlinx.serialization.json.JsonArray(atOpen).toString())), OpenPorts(mock, standEncrypted(), credential, server, SCRIPTED_ENDPOINT, preparer = preparer, partialOperations = true), OnlineAtOpen()))
        opened.registerOperations(withSessionProfile(kotlinx.serialization.json.JsonArray(later).toString()))
        opened
    } else {
        adopt(openDatabase(config, modelsJson, mock, standEncrypted(), credential, server, withSessionProfile(operations), preparer))
    }
    standServer = server
    signIn(database, restored, setsOnline = !onlineAtOpen)
    return database
}

/** The same session stand on a clock that only `advance` moves. The platform reports online at open unless `onlineAtOpen` is false: then the sign-in sets online after the operations. */
@OptIn(DBlayerTestingApi::class)
suspend fun openSignedClocked(modelsJson: String, transport: MockTransport, operations: String = "[]", path: String? = null, startMillis: Long = System.currentTimeMillis(), onlineAtOpen: Boolean = true, observers: PlatformObserver? = null, preparer: Preparer? = ScriptedPreparer()): TestClock {
    val credential = credentialOf(Config(path, null))
    val server = SealedServer(contract = credential.contract, answer = bridgeToTransport(transport))
    server.registerDocuments(withSessionProfile(withUploadSteps(operations, modelsJson)))
    transport.cable = EncryptedCablePeer(server)
    try {
        DBlayer.close()
    } catch (_: DbException.Closed) {
    } catch (_: DbException.NotOpen) {
    }
    val restored = credential.get() != null
    val clock = DBlayer.openTesting(transport, withRawCalls(modelsJson), path = path, startMillis = startMillis, encrypted = standEncrypted(), credentials = credential, http = server, observers = observers ?: if (onlineAtOpen) null else SilentPlatform(), preparer = preparer)
    if (onlineAtOpen) DBlayer.database.setOnline(true)
    standServer = server
    DBlayer.database.registerOperations(withSessionProfile(operations))
    signIn(DBlayer.database, restored)
    return clock
}

/** A database whose plain transport holds a session and sees no network: a sent row stays queued. A send needs a session. */
suspend fun openSignedOffline(config: Config, modelsJson: String, operations: String = SEND_MOMENT, preparer: Preparer? = ScriptedPreparer()): Database {
    val database = openSigned(config, modelsJson, MockTransport(), operations, preparer = preparer)
    database.setOnline(false)
    return database
}

val UPLOAD_STEP_OPERATIONS = listOf(
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

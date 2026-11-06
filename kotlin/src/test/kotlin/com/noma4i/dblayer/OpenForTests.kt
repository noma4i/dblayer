package com.noma4i.dblayer

/** A test transport that forwards to a `MockTransport`: the session cable of the stand goes to that mock. */
interface WrapsMock { val base: MockTransport }

suspend fun openDatabase(config: Config, modelsJson: String, transport: Transport? = null, encrypted: EncryptedConfig? = null, credentials: CredentialStore? = null, http: EncryptedHttp? = null, operations: String = "[]", uploads: UploadOperations? = null): Database =
    OpenDatabase().open(config, modelsJson, OpenOptions(operations, uploads?.wire(), false, false), transport, encrypted, credentials, http, null)

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

private suspend fun signIn(database: Database) {
    database.setOnline(true)
    database.sessionCall("login_link", """{"account":"stand"}""")
    DBlayer.run("SessionProfile", kotlinx.serialization.json.JsonObject(emptyMap()))
}

/** A transport signs in through the real session path, and its queries and mutations ride the session HTTP. */
suspend fun reopen(config: Config, modelsJson: String, transport: Transport? = null, operations: String = "[]", uploads: UploadOperations? = null): Database {
    val mock = transport ?: return adopt(openDatabase(config, modelsJson, transport, operations = operations, uploads = uploads))
    val server = SealedServer(answer = bridgeToTransport(mock))
    val host = mock as? MockTransport ?: (mock as? WrapsMock)?.base
    host?.cable = EncryptedCablePeer(server)
    val database = adopt(openDatabase(config, modelsJson, mock, standEncrypted(), StandCredential(), server, withSessionProfile(operations), uploads))
    signIn(database)
    return database
}

/** The same session stand on a clock that only `advance` moves. */
@OptIn(DBlayerTestingApi::class)
suspend fun reopenClocked(modelsJson: String, transport: MockTransport, operations: String = "[]", path: String? = null): TestClock {
    val server = SealedServer(answer = bridgeToTransport(transport))
    transport.cable = EncryptedCablePeer(server)
    try {
        DBlayer.close()
    } catch (_: DbException.Closed) {
    }
    val clock = DBlayer.openTesting(transport, modelsJson, path = path, encrypted = standEncrypted(), credentials = StandCredential(), http = server)
    DBlayer.database.registerOperations(withSessionProfile(operations))
    signIn(DBlayer.database)
    return clock
}

/** A database whose plain transport holds a session and sees no network: a sent row stays queued. A send needs a session. */
suspend fun reopenSigned(config: Config, modelsJson: String, operations: String = SEND_MOMENT, uploads: UploadOperations? = null): Database {
    val database = reopen(config, modelsJson, MockTransport(), operations, uploads)
    database.setOnline(false)
    return database
}

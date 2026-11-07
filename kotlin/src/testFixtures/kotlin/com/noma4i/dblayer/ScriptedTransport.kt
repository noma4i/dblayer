package com.noma4i.dblayer

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.add

/** A GraphQL call of the library as a test reads it: the operation text, its name and its variables. */
data class TransportRequest(val document: String, val operationName: String, val variablesJson: String, val purpose: NetworkPurpose)

/** The answer of an encrypted POST as a test scripts it. */
class EncryptedReply(val status: UShort, val body: String)

/** A scripted error as the raw outcome the platform executor would report. */
private fun failed(kind: RawFailureKind, message: String): RawOutcome = RawOutcome.Failed(RawFailure(kind, null, message))

fun scriptedOutcome(error: Throwable): RawOutcome = when (error) {
    is DbException.Http -> RawOutcome.Response(error.status, null, error.detail)
    is DbException.Network, is DbException.Offline -> failed(RawFailureKind.UNREACHABLE, error.message ?: "network")
    is DbException.Interrupted, is kotlinx.coroutines.CancellationException -> failed(RawFailureKind.CANCELLED, error.message ?: "cancelled")
    is DbException.Security -> failed(if (error.code == SecurityCode.TLS_TRUST) RawFailureKind.TLS else RawFailureKind.IO, error.message ?: "security")
    is DbException.Graphql -> RawOutcome.Response(200u, null, buildJsonObject {
        put("errors", buildJsonArray {
            error.messages.forEach { text -> add(buildJsonObject { put("message", text); put("extensions", buildJsonObject { put("code", text) }) }) }
        })
    }.toString())
    else -> failed(RawFailureKind.IO, error.message ?: error.toString())
}

private fun graphqlCall(request: HttpRequest): TransportRequest {
    val body: JsonObject = kotlinx.serialization.json.Json.parseToJsonElement(request.body).jsonObject
    fun text(key: String) = (body[key] as? JsonPrimitive)?.contentOrNull ?: ""
    val variables: JsonElement = body["variables"] ?: JsonObject(emptyMap())
    return TransportRequest(text("query"), text("operationName"), variables.toString(), request.purpose)
}

/** A transport a test scripts with values and errors; the defaults turn them into the raw outcomes of the platform port. */
interface ScriptedTransport : Transport {
    suspend fun query(request: TransportRequest): String = throw DbException.Invalid("unexpected plain query")

    suspend fun mutate(request: TransportRequest): String = throw DbException.Invalid("unexpected plain mutation")

    suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply = throw DbException.Invalid("unexpected upload")

    suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome = CableOutcome.Failed(RawFailure(RawFailureKind.UNREACHABLE, null, "no cable"))

    override fun endpoint(): String = "https://graphql.test/graphql"

    override suspend fun send(request: HttpRequest): RawOutcome = sendGraphql(request)

    suspend fun sendGraphql(request: HttpRequest): RawOutcome {
        val scripted = graphqlCall(request)
        return try {
            val text = if (scripted.document.trimStart().startsWith("mutation")) mutate(scripted) else query(scripted)
            RawOutcome.Response(200u, null, text)
        } catch (error: DbException) {
            scriptedOutcome(error)
        }
    }

    override suspend fun openCable(url: String, connectMs: ULong, events: CableEvents): CableOutcome = scriptedCable(url, events)

    override suspend fun put(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): RawOutcome = try {
        val reply = scriptedPut(url, headers, file, identity, progress)
        RawOutcome.Response(reply.status, reply.etag, "")
    } catch (error: DbException) {
        scriptedOutcome(error)
    }
}

/** An encrypted HTTP exchange a test scripts by path. */
interface ScriptedHttp : EncryptedHttp {
    suspend fun post(path: String, body: String, purpose: NetworkPurpose): EncryptedReply

    override suspend fun send(request: HttpRequest): RawOutcome = sendEncrypted(request)

    suspend fun sendEncrypted(request: HttpRequest): RawOutcome = try {
        val reply = post(java.net.URI(request.url).path, request.body, request.purpose)
        RawOutcome.Response(reply.status, null, reply.body)
    } catch (error: DbException) {
        scriptedOutcome(error)
    }
}

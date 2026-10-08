package com.noma4i.dblayer

import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.put
import okhttp3.Call
import okhttp3.Callback
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response as HttpResponse
import java.io.IOException
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

enum class GraphQLOperationKind { Query, Mutation }

object PublicOperations {
    val names = setOf("AuthSignIn", "AuthConsumeLoginLink")
}

data class GraphQLRequest<Variables>(
    val operationName: String,
    val document: String,
    val variables: Variables,
    val kind: GraphQLOperationKind = GraphQLOperationKind.Query,
) {
    val isPublic: Boolean get() = operationName in PublicOperations.names
}

data class RequestFailure(val operationName: String, val status: Int?, val errorClass: String)

sealed class GraphQLTransportError(val errorClass: String, override val message: String) : Exception(message) {
    data object Unauthenticated : GraphQLTransportError("unauthenticated", "UNAUTHENTICATED")
    data class PayloadTransport(val code: String, val status: Int) : GraphQLTransportError("payloadTransport", "$code (HTTP $status)")
    data class Server(val status: Int) : GraphQLTransportError("server", "SERVER_ERROR (HTTP $status)")
    data class GraphQL(val messages: List<String>) : GraphQLTransportError("graphQL", messages.joinToString(", "))

    companion object {
        fun classify(status: Int, errorMessages: List<String>): GraphQLTransportError? = when {
            status in 500..599 -> Server(status)
            status == 401 -> Unauthenticated
            errorMessages.isNotEmpty() -> GraphQL(errorMessages)
            status == 403 -> Unauthenticated
            status in 200..299 -> null
            else -> Server(status)
        }
    }
}

enum class TransportRetryReason { PersistedQueryNotFound, SignatureExpired, SignatureReplay }

class TransportRetryPolicy {
    private val used = mutableSetOf<TransportRetryReason>()

    fun shouldRetry(reason: TransportRetryReason, kind: GraphQLOperationKind): Boolean {
        if (reason == TransportRetryReason.SignatureReplay && kind != GraphQLOperationKind.Query) return false
        return used.add(reason)
    }

    companion object {
        fun reason(errorMessage: String, code: String?): TransportRetryReason? = when {
            errorMessage == "PersistedQueryNotFound" || code == "PERSISTED_QUERY_NOT_FOUND" -> TransportRetryReason.PersistedQueryNotFound
            code == "SIGNATURE_EXPIRED" -> TransportRetryReason.SignatureExpired
            code == "SIGNATURE_REPLAY" -> TransportRetryReason.SignatureReplay
            else -> null
        }
    }
}

object UnauthorizedDecision {
    const val LOGOUT_OPERATION_NAME = "myProfile"
    private val securityCodes = setOf(
        "SIGNATURE_VERSION_UNSUPPORTED", "SIGNATURE_MISSING", "SIGNATURE_INVALID", "SIGNATURE_EXPIRED",
        "PAYLOAD_ENCRYPTION_REQUIRED", "PAYLOAD_DECRYPTION_FAILED",
    )

    fun logsOut(operationName: String, status: Int?, codes: List<String>, messages: List<String>): Boolean {
        if (operationName != LOGOUT_OPERATION_NAME) return false
        if (status == 401 || status == 403) return true
        if ("UNAUTHENTICATED" in codes) return true
        return messages.any { val text = it.lowercase(); "unauthorized" in text || "unauthenticated" in text }
    }

    fun reportsFatal(status: Int?): Boolean = status in setOf(500, 502, 503, 504)

    fun isSecurityCode(code: String): Boolean = code in securityCodes
}

object TransportIdentity {
    fun resolve(operationName: String, token: String?, deviceUuid: String): String {
        if (operationName in PublicOperations.names) return deviceUuid
        if (token.isNullOrEmpty()) throw GraphQLTransportError.Unauthenticated
        return token
    }

    fun authorizationToken(operationName: String, token: String?): String? = if (operationName in PublicOperations.names) null else token
}

object TransportHeaders {
    const val API_VERSION = "2"
    const val PLATFORM = "android"

    fun make(appVersion: String, appBuild: String, deviceUuid: String, token: String?, signed: SignedHeaders): Map<String, String> = buildMap {
        put("X-API-Version", API_VERSION)
        put("x-app-version", appVersion)
        put("x-app-build", appBuild)
        put("x-app-platform", PLATFORM)
        put("x-device-uuid", deviceUuid)
        put("X-Payload-Encryption", "1")
        put("X-Request-Signature-Version", signed.version)
        put("X-Request-Signature", signed.signature)
        put("X-Request-Timestamp", signed.timestamp)
        put("X-Request-Nonce", signed.nonce)
        if (!token.isNullOrEmpty()) put("authorization", "Bearer $token")
    }
}

data class TransportInputs(val appVersion: String, val appBuild: String, val deviceUuid: String, val token: String? = null)

data class GraphQLResponseError(val message: String, val code: String?, val status: Int?)

data class GraphQLResponseBody(val data: JsonElement?, val errors: List<GraphQLResponseError>) {
    companion object {
        fun parse(body: ByteArray): GraphQLResponseBody? {
            val root = runCatching { parseJson(String(body)) }.getOrNull() as? JsonObject ?: return null
            val errors = (root["errors"] as? JsonArray).orEmpty().mapNotNull { element ->
                val error = element as? JsonObject ?: return@mapNotNull null
                val message = (error["message"] as? JsonPrimitive)?.takeIf { it.isString }?.content ?: return@mapNotNull null
                val extensions = error["extensions"] as? JsonObject
                val code = (extensions?.get("code") as? JsonPrimitive)?.takeIf { it.isString }?.content
                val httpStatus = ((extensions?.get("http") as? JsonObject)?.get("status") as? JsonPrimitive)?.intOrNull
                val status = httpStatus ?: (extensions?.get("status") as? JsonPrimitive)?.intOrNull
                GraphQLResponseError(message, code, status)
            }
            return GraphQLResponseBody(root["data"], errors)
        }
    }
}

object GraphQLRequestBody {
    const val PERSISTED_QUERY_VERSION = 1

    fun make(operationName: String, documentHash: String, variablesJson: ByteArray, document: String?): ByteArray? {
        val variables = runCatching { parseJson(String(variablesJson)) }.getOrNull() ?: return null
        val body = buildJsonObject {
            put("extensions", buildJsonObject { put("persistedQuery", buildJsonObject { put("sha256Hash", documentHash); put("version", PERSISTED_QUERY_VERSION) }) })
            put("operationName", operationName)
            put("variables", variables)
            if (!document.isNullOrEmpty()) put("query", document)
        }
        return CanonicalJson.encode(body)
    }
}

object CanonicalJson {
    fun encode(value: JsonElement): ByteArray = buildString { write(value, this) }.toByteArray()

    private fun write(value: JsonElement, out: StringBuilder) {
        when (value) {
            is JsonObject -> {
                out.append('{')
                value.entries.sortedWith { a, b -> compareUtf8(a.key, b.key) }.forEachIndexed { index, entry ->
                    if (index > 0) out.append(',')
                    writeString(entry.key, out)
                    out.append(':')
                    write(entry.value, out)
                }
                out.append('}')
            }
            is JsonArray -> {
                out.append('[')
                value.forEachIndexed { index, element -> if (index > 0) out.append(','); write(element, out) }
                out.append(']')
            }
            is JsonPrimitive -> when {
                value is kotlinx.serialization.json.JsonNull -> out.append("null")
                value.isString -> writeString(value.content, out)
                value.content == "true" || value.content == "false" -> out.append(value.content)
                else -> out.append(formatNumber(value.content.toDouble()))
            }
        }
    }

    private fun compareUtf8(a: String, b: String): Int {
        val left = a.toByteArray()
        val right = b.toByteArray()
        for (index in 0 until minOf(left.size, right.size)) {
            val difference = (left[index].toInt() and 0xff) - (right[index].toInt() and 0xff)
            if (difference != 0) return difference
        }
        return left.size - right.size
    }

    private fun formatNumber(number: Double): String {
        if (!number.isFinite()) return "null"
        if (kotlin.math.floor(number) == number && kotlin.math.abs(number) < 9_007_199_254_740_992.0) return number.toLong().toString()
        return number.toString()
    }

    private fun writeString(text: String, out: StringBuilder) {
        out.append('"')
        text.forEach { character ->
            when {
                character == '\b' -> out.append("\\b")
                character == '\t' -> out.append("\\t")
                character == '\n' -> out.append("\\n")
                character == '\u000c' -> out.append("\\f")
                character == '\r' -> out.append("\\r")
                character == '"' -> out.append("\\\"")
                character == '\\' -> out.append("\\\\")
                character == '<' -> out.append("\\u003c")
                character == '>' -> out.append("\\u003e")
                character == '&' -> out.append("\\u0026")
                character == ' ' -> out.append("\\u2028")
                character == ' ' -> out.append("\\u2029")
                character.code < 32 -> out.append("\\u00").append("%02x".format(character.code))
                else -> out.append(character)
            }
        }
        out.append('"')
    }
}

class ServerClock {
    private val offsetMillis = AtomicLong(0)

    val offsetSeconds: Double get() = offsetMillis.get() / 1000.0

    fun update(dateHeader: String?, receivedAtMillis: Long) {
        val header = dateHeader ?: return
        val server = runCatching { OffsetDateTime.parse(header, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toEpochMilli() }.getOrNull() ?: return
        offsetMillis.set(server - receivedAtMillis)
    }

    fun nowMillis(referenceMillis: Long = System.currentTimeMillis()): Long = referenceMillis + offsetMillis.get()
}

class GraphQLHttpTransport(
    private val url: String,
    private val client: OkHttpClient,
    private val signer: RequestSigner,
    private val clock: ServerClock,
    private val appVersion: String,
    private val appBuild: String,
    private val deviceUuid: String,
    private val token: String?,
) {
    data class Reply(val status: Int, val body: ByteArray)

    suspend fun send(operationName: String, documentHash: String, document: String?, variablesJson: ByteArray): Reply {
        val identity = TransportIdentity.resolve(operationName, token, deviceUuid)
        val nowSeconds = clock.nowMillis() / 1000.0
        val signed = signer.sign(SignatureInput(operationName, variablesJson, identity, deviceUuid, nowSeconds, RequestSigner.makeNonce()))
        val requestBody = GraphQLRequestBody.make(operationName, documentHash, variablesJson, document) ?: throw GraphQLTransportError.Server(0)
        val sealed = signer.payloadCipher().seal(requestBody)
        val encrypted = dbJson.encodeToString(EnvelopeWire.serializer(), EnvelopeWire(sealed.envelopeVersion, sealed.iv, sealed.ciphertext, sealed.tag))
        val headers = TransportHeaders.make(appVersion, appBuild, deviceUuid, TransportIdentity.authorizationToken(operationName, token), signed)
        val request = Request.Builder().url(url).post(encrypted.toRequestBody("application/json".toMediaType())).apply {
            headers.forEach { (field, value) -> header(field, value) }
        }.build()
        val response = client.newCall(request).await()
        response.use {
            val bytes = it.body?.bytes() ?: ByteArray(0)
            clock.update(it.header("Date"), System.currentTimeMillis())
            return Reply(it.code, open(bytes, it))
        }
    }

    private fun open(body: ByteArray, response: HttpResponse): ByteArray {
        if (response.header("x-payload-encryption") != "1") return body
        return try {
            val wire = dbJson.decodeFromString(EnvelopeWire.serializer(), String(body))
            signer.payloadCipher().open(PayloadEnvelope(wire.v, wire.iv, wire.ct, wire.tag))
        } catch (error: Exception) {
            throw PayloadTransportError.ResponseDecryptionFailed(response.code)
        }
    }
}

@kotlinx.serialization.Serializable
internal data class EnvelopeWire(val v: Int, val iv: String, val ct: String, val tag: String)

internal suspend fun Call.await(): HttpResponse = suspendCancellableCoroutine { continuation ->
    enqueue(object : Callback {
        override fun onFailure(call: Call, e: IOException) = continuation.resumeWithException(e)
        override fun onResponse(call: Call, response: HttpResponse) = continuation.resume(response)
    })
    continuation.invokeOnCancellation { cancel() }
}

class OperationRun(
    private val transport: GraphQLHttpTransport,
    private val hashes: PersistedQueryHashes,
    private val onFailure: (RequestFailure) -> Unit = {},
) {
    suspend fun send(operationName: String, document: String, variables: JsonElement, kind: GraphQLOperationKind, dataSystemMutation: Boolean = false): ByteArray {
        val variablesJson = CanonicalJson.encode(variables)
        val documentHash = hashes.hash(document)
        suspend fun post(withDocument: String?) = try {
            transport.send(operationName, documentHash, withDocument, variablesJson)
        } catch (error: Exception) {
            onFailure(RequestFailure(operationName, null, error.javaClass.simpleName))
            throw error
        }
        var result = post(null)
        var retryPolicy: TransportRetryPolicy? = TransportRetryPolicy()
        while (true) {
            val response = GraphQLResponseBody.parse(result.body)
            val retryReason = response?.errors?.firstOrNull()?.let {
                if (result.status == 200 || result.status == 403) TransportRetryPolicy.reason(it.message, it.code) else null
            }
            if (retryReason != null && retryPolicy?.shouldRetry(retryReason, kind) == true) {
                retryPolicy = null
                result = post(if (retryReason == TransportRetryReason.PersistedQueryNotFound) document else null)
                continue
            }
            val returnsMutationBody = kind == GraphQLOperationKind.Mutation && (dataSystemMutation || response?.data != null)
            if (result.status in 200..299 && returnsMutationBody) {
                if (response?.errors?.isNotEmpty() == true) onFailure(RequestFailure(operationName, result.status, "graphQL"))
                return result.body
            }
            val error = GraphQLTransportError.classify(result.status, response?.errors?.map { it.message }.orEmpty())
            if (error != null) {
                onFailure(RequestFailure(operationName, result.status, error.errorClass))
                throw error
            }
            return result.body
        }
    }
}

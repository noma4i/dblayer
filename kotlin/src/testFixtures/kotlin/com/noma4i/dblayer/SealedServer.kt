package com.noma4i.dblayer

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.security.KeyFactory
import java.security.PrivateKey
import java.security.spec.MGF1ParameterSpec
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.OAEPParameterSpec
import javax.crypto.spec.PSource
import javax.crypto.spec.SecretKeySpec

class RequestPurpose(val value: NetworkPurpose) : kotlin.coroutines.AbstractCoroutineContextElement(Key) {
    companion object Key : kotlin.coroutines.CoroutineContext.Key<RequestPurpose>
}

/** An encrypted server that opens every sealed request with the fixture key and seals the answer, so tests run the real encrypted session path and fake only the HTTP exchange. */
class SealedServer(private val deduplicate: Boolean = false, private val answer: suspend (path: String, body: JsonObject) -> JsonObject) : ScriptedHttp {
    companion object {
        const val SERVER_KEY = "MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAtzb67l/cXul2uArFqeZXsMf4tImrLcaHb3uiVNp8mjjJrFK/zwUEEazm3pmriAbj1nBzSxQLuq3jNUeFHXoQxjwWmTdv+PoAypz4ZII6Dz6rknk290/ddmimc1Z4Mx0AkT4ffAA9WHiDtLPF6tKZWNsWF/SpJps9PWrIKNOKY78ZfCgfegrWfKwoNYExyZ0IuUUgG0lsRjEEE/U59GpbNBqdKtw7KaIGK47QRk6HWOkCQtyqqjsseOJLwdI0k6jDNmcH6DulPNmar9AbwsmpngD55P8y4JdH49Eq3Nr040gmg0Y1p1FZm5PbKeC4WgZZdmg1bxjKAa+MYP0fROi4dFz05/81fqFb6DWaXencb0p/Iz+IUfQiI/0EYNv1SfdTUHzGyHFAmQHeCUIKLSkNNg3uF/jqKthFecCnWEfTUWRWFsN/8/Nb65a1UI/L1hP/qd+T6xKfchqVBEf767yeHdgsy3GeuqTsgXUPkuA1DO9HPP1xWqa6orUNWkimSwxDAgMBAAE="
        private const val PRIVATE_KEY_BODY = "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"
        const val SERVER_PIN = "fab4f816eb33917161942684230405ae47ad4df5b456bd973085599b1364342a"
    }

    private val privateKey: PrivateKey
    val requests = CopyOnWriteArrayList<Pair<String, JsonObject>>()
    val routes = CopyOnWriteArrayList<Pair<String?, NetworkPurpose>>()
    private val refusing = AtomicBoolean(false)
    private val holding = java.util.concurrent.atomic.AtomicReference<String?>(null)
    private val parked = CopyOnWriteArrayList<kotlinx.coroutines.CompletableDeferred<Unit>>()
    private val arrivals = kotlinx.coroutines.channels.Channel<Unit>(kotlinx.coroutines.channels.Channel.UNLIMITED)
    private val lostAnswers = kotlinx.coroutines.channels.Channel<Unit>(kotlinx.coroutines.channels.Channel.UNLIMITED)
    private val cached = java.util.concurrent.ConcurrentHashMap<String, Pair<JsonObject, String>>()
    private val lostFile = java.util.concurrent.atomic.AtomicReference<String?>(null)
    private val lost = AtomicBoolean(false)
    val effects = CopyOnWriteArrayList<JsonObject>()

    init {
        val pkcs1 = Base64.getDecoder().decode(PRIVATE_KEY_BODY)
        val inner = pkcs1.size + 22
        val pkcs8 = byteArrayOf(0x30, 0x82.toByte(), (inner shr 8).toByte(), inner.toByte(), 0x02, 0x01, 0x00, 0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86.toByte(), 0x48, 0x86.toByte(), 0xf7.toByte(), 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00, 0x04, 0x82.toByte(), (pkcs1.size shr 8).toByte(), pkcs1.size.toByte()) + pkcs1
        privateKey = KeyFactory.getInstance("RSA").generatePrivate(PKCS8EncodedKeySpec(pkcs8))
    }

    fun pathRequests(path: String): Int = requests.count { it.first == path }

    fun operationRequests(operation: String): Int = requests.count { it.second["operationName"]?.jsonPrimitive?.content == operation }

    /** While true every request is recorded and refused with HTTP 500, so a test that forbids background reads finds them in `requests`. */
    fun refuseEverything(refusing: Boolean) = this.refusing.set(refusing)

    /** While set, a request for this operation reaches the server and waits there, so a test ends a call while its request is in flight. `null` lets every waiting request answer. */
    fun hold(operation: String?) {
        holding.set(operation)
        if (operation == null) {
            parked.forEach { it.complete(Unit) }
            parked.clear()
            TestPulse.pulse()
        }
    }

    /** How many requests wait in the hold. */
    val parkedCount: Int get() = parked.size

    suspend fun heldRequest() { arrivals.receive() }
    fun loseAnswer(filename: String) { lostFile.set(filename) }
    suspend fun lostAnswer() { lostAnswers.receive() }
    fun hasLostAnswer(): Boolean = lost.get()

    override suspend fun post(path: String, body: String, purpose: NetworkPurpose): EncryptedReply {
        val request = open(body)
        val sent = request.getValue("body").jsonObject
        requests.add(path to sent)
        TestPulse.pulse()
        routes.add(sent["operationName"]?.jsonPrimitive?.content to purpose)
        if (refusing.get()) return EncryptedReply(500u, "")
        val requestId = request.getValue("requestId").jsonPrimitive.content
        val stable = JsonObject(request.filterKeys { it != "issuedAt" })
        if (deduplicate) cached[requestId]?.let {
            if (it.first != stable) throw DbException.Invalid("request ID reused with different content")
            return EncryptedReply(200u, it.second)
        }
        val reply = try { kotlinx.coroutines.withContext(RequestPurpose(purpose)) { answer(path, sent) } } catch (failure: DbException) {
            when (failure) {
                is DbException.Http -> return EncryptedReply(failure.status, "")
                is DbException.Session -> buildJsonObject { put("error", failure.code.name) }
                is DbException.Graphql -> buildJsonObject {
                    put("errors", buildJsonArray {
                        failure.messages.forEach { message ->
                            add(buildJsonObject {
                                put("message", message)
                                put("extensions", buildJsonObject { failure.extensions?.code?.let { put("code", it) } })
                            })
                        }
                    })
                }
                else -> throw failure
            }
        }
        effects.add(sent)
        TestPulse.pulse()
        val operation = sent["operationName"]?.jsonPrimitive?.content
        if (operation != null && holding.get() == operation) {
            val gate = kotlinx.coroutines.CompletableDeferred<Unit>()
            parked.add(gate)
            arrivals.trySend(Unit)
            TestPulse.pulse()
            if (holding.get() != operation) gate.complete(Unit)
            kotlinx.coroutines.withContext(kotlinx.coroutines.NonCancellable) { gate.await() }
        }
        val sealed = seal(request, reply)
        if (deduplicate) cached[requestId] = stable to sealed
        val filename = sent["variables"]?.jsonObject?.get("filename")?.jsonPrimitive?.content
        val wanted = lostFile.get()
        if (filename != null && filename == wanted && lostFile.compareAndSet(wanted, null)) {
            lost.set(true)
            lostAnswers.trySend(Unit)
            throw DbException.Network("lost answer")
        }
        return EncryptedReply(200u, sealed)
    }

    fun open(token: String): JsonObject {
        val parts = token.split(".")
        val oaep = Cipher.getInstance("RSA/ECB/OAEPPadding")
        oaep.init(Cipher.DECRYPT_MODE, privateKey, OAEPParameterSpec("SHA-1", "MGF1", MGF1ParameterSpec.SHA1, PSource.PSpecified.DEFAULT))
        val cek = oaep.doFinal(decode(parts[1]))
        val gcm = Cipher.getInstance("AES/GCM/NoPadding")
        gcm.init(Cipher.DECRYPT_MODE, SecretKeySpec(cek, "AES"), GCMParameterSpec(128, decode(parts[2])))
        gcm.updateAAD(parts[0].toByteArray())
        val plain = gcm.doFinal(decode(parts[3]) + decode(parts[4]))
        return Json.parseToJsonElement(String(plain)).jsonObject
    }

    fun seal(request: JsonObject, body: JsonObject): String {
        val header = encode("""{"alg":"dir","enc":"A256GCM"}""".toByteArray())
        val plain = buildJsonObject {
            put("requestId", request.getValue("requestId"))
            put("sequence", request["sequence"] ?: JsonNull)
            put("body", body)
        }.toString().toByteArray()
        val iv = ByteArray(12) { 7 }
        val gcm = Cipher.getInstance("AES/GCM/NoPadding")
        gcm.init(Cipher.ENCRYPT_MODE, SecretKeySpec(decode(request.getValue("responseKey").jsonPrimitive.content), "AES"), GCMParameterSpec(128, iv))
        gcm.updateAAD(header.toByteArray())
        val sealed = gcm.doFinal(plain)
        val text = sealed.copyOfRange(0, sealed.size - 16)
        val tag = sealed.copyOfRange(sealed.size - 16, sealed.size)
        return "$header..${encode(iv)}.${encode(text)}.${encode(tag)}"
    }

    private fun decode(text: String): ByteArray = Base64.getUrlDecoder().decode(text)

    private fun encode(bytes: ByteArray): String = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)
}

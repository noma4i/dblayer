package com.noma4i.dblayer

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.io.File
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

/** An encrypted server that opens every sealed request with the fixture key and seals the answer, so tests run the real encrypted session path and fake only the HTTP exchange. */
class SealedServer(private val answer: (path: String, body: JsonObject) -> JsonObject) : EncryptedHttp {
    companion object {
        const val SERVER_KEY = "MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAtzb67l/cXul2uArFqeZXsMf4tImrLcaHb3uiVNp8mjjJrFK/zwUEEazm3pmriAbj1nBzSxQLuq3jNUeFHXoQxjwWmTdv+PoAypz4ZII6Dz6rknk290/ddmimc1Z4Mx0AkT4ffAA9WHiDtLPF6tKZWNsWF/SpJps9PWrIKNOKY78ZfCgfegrWfKwoNYExyZ0IuUUgG0lsRjEEE/U59GpbNBqdKtw7KaIGK47QRk6HWOkCQtyqqjsseOJLwdI0k6jDNmcH6DulPNmar9AbwsmpngD55P8y4JdH49Eq3Nr040gmg0Y1p1FZm5PbKeC4WgZZdmg1bxjKAa+MYP0fROi4dFz05/81fqFb6DWaXencb0p/Iz+IUfQiI/0EYNv1SfdTUHzGyHFAmQHeCUIKLSkNNg3uF/jqKthFecCnWEfTUWRWFsN/8/Nb65a1UI/L1hP/qd+T6xKfchqVBEf767yeHdgsy3GeuqTsgXUPkuA1DO9HPP1xWqa6orUNWkimSwxDAgMBAAE="
        const val SERVER_PIN = "fab4f816eb33917161942684230405ae47ad4df5b456bd973085599b1364342a"
    }

    private val privateKey: PrivateKey
    val requests = CopyOnWriteArrayList<Pair<String, JsonObject>>()
    private val refusing = AtomicBoolean(false)
    private val holding = java.util.concurrent.atomic.AtomicReference<String?>(null)
    private val parked = CopyOnWriteArrayList<kotlinx.coroutines.CompletableDeferred<Unit>>()

    init {
        val fixture = Json.parseToJsonElement(File("../fixtures/encrypted-http/kat.json").readText()).jsonObject
        val pem = fixture.getValue("SERVER_PRIVATE_KEY_PEM").jsonPrimitive.content.lines().filter { !it.startsWith("-----") }.joinToString("")
        val pkcs1 = Base64.getDecoder().decode(pem)
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
        }
    }

    /** How many requests wait in the hold. */
    val parkedCount: Int get() = parked.size

    override suspend fun post(path: String, body: String, upload: Boolean): EncryptedReply {
        val request = open(body)
        val sent = request.getValue("body").jsonObject
        requests.add(path to sent)
        if (refusing.get()) return EncryptedReply(500u, "")
        val reply = answer(path, sent)
        val operation = sent["operationName"]?.jsonPrimitive?.content
        if (operation != null && holding.get() == operation) {
            val gate = kotlinx.coroutines.CompletableDeferred<Unit>()
            parked.add(gate)
            if (holding.get() != operation) gate.complete(Unit)
            gate.await()
        }
        return EncryptedReply(200u, seal(request, reply))
    }

    private fun open(token: String): JsonObject {
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

    private fun seal(request: JsonObject, body: JsonObject): String {
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

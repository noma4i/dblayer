package com.noma4i.dblayer

import java.security.MessageDigest
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.Mac
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

internal fun ByteArray.hex(): String = joinToString("") { "%02x".format(it) }

internal fun sha256(data: ByteArray): ByteArray = MessageDigest.getInstance("SHA-256").digest(data)

internal fun hmacSha256(key: ByteArray, data: ByteArray): ByteArray =
    Mac.getInstance("HmacSHA256").apply { init(SecretKeySpec(key, "HmacSHA256")) }.doFinal(data)

class PersistedQueryHashes {
    private val cache = java.util.concurrent.ConcurrentHashMap<String, String>()

    fun hash(document: String): String = cache.getOrPut(document) { makeHash(document) }

    companion object {
        fun makeHash(document: String): String = sha256(document.toByteArray()).hex()
    }
}

data class SignedHeaders(val version: String, val signature: String, val timestamp: String, val nonce: String)

data class SignatureInput(
    val operationName: String,
    val variablesJson: ByteArray,
    val identity: String,
    val deviceUuid: String,
    val nowSeconds: Double,
    val nonce: String,
)

class RequestSigner(private val secret: String) {
    fun payloadCipher() = PayloadCipher(secret)

    fun sign(input: SignatureInput): SignedHeaders {
        val timestamp = kotlin.math.floor(input.nowSeconds).toLong()
        val variablesHash = sha256(input.variablesJson).hex()
        val canonical = listOf(VERSION, input.operationName, variablesHash, timestamp.toString(), input.nonce, input.deviceUuid).joinToString("|")
        val signature = hmacSha256(secret.toByteArray(), "${input.identity}|$canonical".toByteArray()).hex()
        return SignedHeaders(VERSION, signature, timestamp.toString(), input.nonce)
    }

    companion object {
        const val VERSION = "2"

        fun makeNonce(): String = ByteArray(16).also { SecureRandom().nextBytes(it) }.hex()
    }
}

data class PayloadEnvelope(val envelopeVersion: Int = VERSION, val iv: String, val ciphertext: String, val tag: String) {
    val ivBytes get() = decodeHex(iv)
    val ciphertextBytes get() = decodeHex(ciphertext)
    val tagBytes get() = decodeHex(tag)

    companion object {
        const val VERSION = 1

        fun of(iv: ByteArray, ciphertext: ByteArray, tag: ByteArray) = PayloadEnvelope(VERSION, iv.hex(), ciphertext.hex(), tag.hex())

        internal fun decodeHex(value: String): ByteArray? {
            if (value.length % 2 != 0) return null
            return ByteArray(value.length / 2) { index ->
                val high = Character.digit(value[index * 2], 16)
                val low = Character.digit(value[index * 2 + 1], 16)
                if (high < 0 || low < 0 || value[index * 2].isUpperCase() || value[index * 2 + 1].isUpperCase()) return null
                ((high shl 4) or low).toByte()
            }
        }
    }
}

sealed class PayloadTransportError(val code: String, val status: Int) : Exception("$code (HTTP $status)") {
    class ResponseDecryptionFailed(status: Int) : PayloadTransportError("PAYLOAD_RESPONSE_DECRYPTION_FAILED", status)
    class ResponseEncryptionInvalid(status: Int) : PayloadTransportError("PAYLOAD_RESPONSE_ENCRYPTION_INVALID", status)
}

class PayloadCipher(secret: String) {
    enum class Failure { UnsupportedVersion, MalformedEnvelope, AuthenticationFailed }

    class CipherException(val failure: Failure) : Exception(failure.name)

    private val key: ByteArray = run {
        val pseudoRandomKey = hmacSha256(ByteArray(32), secret.toByteArray())
        hmacSha256(pseudoRandomKey, "payload-enc-v1".toByteArray() + byteArrayOf(1))
    }

    fun seal(plaintext: ByteArray, iv: ByteArray = ByteArray(12).also { SecureRandom().nextBytes(it) }): PayloadEnvelope {
        if (iv.size != 12) throw CipherException(Failure.MalformedEnvelope)
        val cipher = Cipher.getInstance("AES/GCM/NoPadding").apply { init(Cipher.ENCRYPT_MODE, SecretKeySpec(key, "AES"), GCMParameterSpec(128, iv)) }
        val sealed = cipher.doFinal(plaintext)
        return PayloadEnvelope.of(iv, sealed.copyOfRange(0, sealed.size - 16), sealed.copyOfRange(sealed.size - 16, sealed.size))
    }

    fun open(envelope: PayloadEnvelope): ByteArray {
        if (envelope.envelopeVersion != PayloadEnvelope.VERSION) throw CipherException(Failure.UnsupportedVersion)
        val iv = envelope.ivBytes
        val ciphertext = envelope.ciphertextBytes
        val tag = envelope.tagBytes
        if (iv == null || iv.size != 12 || ciphertext == null || tag == null || tag.size != 16) throw CipherException(Failure.MalformedEnvelope)
        return try {
            Cipher.getInstance("AES/GCM/NoPadding").apply { init(Cipher.DECRYPT_MODE, SecretKeySpec(key, "AES"), GCMParameterSpec(128, iv)) }.doFinal(ciphertext + tag)
        } catch (error: java.security.GeneralSecurityException) {
            throw CipherException(Failure.AuthenticationFailed)
        }
    }
}

package com.noma4i.dblayer

import java.security.KeyFactory
import java.security.KeyPairGenerator
import java.security.MessageDigest
import java.security.spec.X509EncodedKeySpec
import java.util.Base64
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import javax.crypto.Cipher
import javax.crypto.KeyAgreement
import javax.crypto.Mac
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

class EncryptedCablePeer(private val server: SealedServer) : Transport {
    private class Connection {
        val events = Channel<EncryptedSocketEvent>(Channel.UNLIMITED)
        val keys = KeyPairGenerator.getInstance("X25519").generateKeyPair()
        val salt = ByteArray(32) { 5 }
        var sendKey: ByteArray? = null
        var receiveKey: ByteArray? = null
        val sequence = AtomicInteger()
        var subscription: String? = null
    }

    val subscribed = Channel<String>(Channel.UNLIMITED)
    val closed = Channel<String>(Channel.UNLIMITED)
    private val connections = ConcurrentHashMap<String, Connection>()
    @Volatile var lastOpened: String? = null
    private val json = Json
    private fun encode(bytes: ByteArray) = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)
    private fun decode(value: String) = Base64.getUrlDecoder().decode(value)
    private fun objectOf(text: String) = json.parseToJsonElement(text).jsonObject
    private fun hex(text: String) = text.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
    private fun rawPublic(bytes: ByteArray) = KeyFactory.getInstance("X25519").generatePublic(X509EncodedKeySpec(hex("302a300506032b656e032100") + bytes))
    private fun text(value: JsonObject) = value.toString()

    override suspend fun openCable(lease: CableLease, url: String) {
        val token = UUID.randomUUID().toString()
        val connection = Connection()
        connections[token] = connection
        lastOpened = token
        lease.opened(token)
        val challenge = buildJsonObject {
            put("type", "challenge")
            put("v", 1)
            put("kid", SealedServer.SERVER_PIN)
            put("challenge", encode(connection.salt))
            put("x25519", encode(connection.keys.public.encoded.takeLast(32).toByteArray()))
        }
        connection.events.send(EncryptedSocketEvent.Text(text(challenge)))
    }

    override suspend fun cableSend(token: String, text: String) {
        val connection = connections.getValue(token)
        val command = objectOf(text)
        if (command["command"]?.jsonPrimitive?.content == "subscribe") {
            connection.events.send(EncryptedSocketEvent.Text("""{"type":"confirm_subscription"}"""))
            return
        }
        val data = objectOf(command.getValue("data").jsonPrimitive.content)
        val hello = data["hello"]?.jsonPrimitive?.content
        if (hello != null) {
            val plain = server.open(hello)
            val agreement = KeyAgreement.getInstance("X25519")
            agreement.init(connection.keys.private)
            agreement.doPhase(rawPublic(decode(plain.getValue("clientKey").jsonPrimitive.content)), true)
            val material = agreement.generateSecret() + decode(plain.getValue("responseKey").jsonPrimitive.content)
            fun key(direction: String): ByteArray {
                val mac = Mac.getInstance("HmacSHA256")
                mac.init(SecretKeySpec(connection.salt, "HmacSHA256"))
                val prk = mac.doFinal(material)
                mac.init(SecretKeySpec(prk, "HmacSHA256"))
                val info = listOf("example-ws-v1", direction, SealedServer.SERVER_PIN, "https://api.example.com", "/api/cable").joinToString("\u0000").toByteArray()
                return mac.doFinal(info + byteArrayOf(1))
            }
            connection.sendKey = key("s2c")
            connection.receiveKey = key("c2s")
            val digest = MessageDigest.getInstance("SHA-256").digest(connection.salt).joinToString("") { "%02x".format(it.toInt() and 255) }
            emit(token, buildJsonObject { put("t", "ready"); put("connectionId", "00000000-0000-4000-8000-000000000001"); put("challengeDigest", digest) })
            return
        }
        val frame = data.getValue("frame").jsonPrimitive.content
        val body = openFrame(frame, connection.receiveKey!!)
        if (body["t"]?.jsonPrimitive?.content == "subscribe") {
            val id = body.getValue("id").jsonPrimitive.content
            connection.subscription = id
            emit(token, buildJsonObject { put("t", "subscribed"); put("id", id) })
            subscribed.send(token)
        }
    }

    private fun openFrame(token: String, key: ByteArray): JsonObject {
        val parts = token.split(".")
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(key, "AES"), GCMParameterSpec(128, decode(parts[2])))
        cipher.updateAAD(parts[0].toByteArray())
        return objectOf(String(cipher.doFinal(decode(parts[3]) + decode(parts[4]))))
    }

    private suspend fun emit(token: String, fields: JsonObject) {
        val connection = connections.getValue(token)
        val sequence = connection.sequence.getAndIncrement()
        val body = buildJsonObject { fields.forEach { (key, value) -> put(key, value) }; put("n", sequence) }
        val header = encode("""{"alg":"dir","enc":"A256GCM"}""".toByteArray())
        val iv = ByteArray(12) { (sequence + 1).toByte() }
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.ENCRYPT_MODE, SecretKeySpec(connection.sendKey!!, "AES"), GCMParameterSpec(128, iv))
        cipher.updateAAD(header.toByteArray())
        val sealed = cipher.doFinal(body.toString().toByteArray())
        val frame = "$header..${encode(iv)}.${encode(sealed.dropLast(16).toByteArray())}.${encode(sealed.takeLast(16).toByteArray())}"
        connection.events.send(EncryptedSocketEvent.Text(text(buildJsonObject { put("message", buildJsonObject { put("frame", frame) }) })))
    }

    /** A data event of the open subscription. False while no subscription is open. */
    suspend fun event(token: String, result: JsonObject): Boolean {
        val id = connections.getValue(token).subscription ?: return false
        emit(token, buildJsonObject { put("t", "next"); put("id", id); put("result", result) })
        return true
    }

    /** The socket breaks. */
    suspend fun drop(token: String) {
        connections.getValue(token).events.send(EncryptedSocketEvent.Closed(1006u, "dropped"))
    }

    suspend fun emitOld(token: String) {
        val id = connections.getValue(token).subscription!!
        emit(token, buildJsonObject {
            put("t", "next")
            put("id", id)
            put("result", buildJsonObject { put("data", buildJsonObject { put("bulletinAdded", buildJsonObject { put("body", "old"); put("occurredAt", "2026-10-18T00:00:00Z") }) }) })
        })
    }

    override suspend fun cableNext(token: String): EncryptedSocketEvent? = connections[token]?.events?.receiveCatching()?.getOrNull()
    override suspend fun cableClose(token: String) { closed.send(token) }
    override suspend fun query(request: TransportRequest): String = throw DbException.Invalid("unexpected plain query")
    override suspend fun mutate(request: TransportRequest): String = throw DbException.Invalid("unexpected plain mutation")
    override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink) { throw DbException.Invalid("unexpected plain subscription") }
    override fun unsubscribe(token: String) {}
    override suspend fun put(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply = throw DbException.Invalid("unexpected upload")
}

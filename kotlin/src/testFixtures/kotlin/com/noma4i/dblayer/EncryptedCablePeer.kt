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

class EncryptedCablePeer(private val server: SealedServer) : CablePeer, ScriptedTransport {
    override var onSubscribe: ((TransportRequest) -> Unit)? = null
    private class Connection(val events: CableEvents) {
        val keys = KeyPairGenerator.getInstance("X25519").generateKeyPair()
        val salt = ByteArray(32) { 5 }
        var sendKey: ByteArray? = null
        var receiveKey: ByteArray? = null
        val sequence = AtomicInteger()
        val subscriptions = ConcurrentHashMap<String, String>()
    }

    val subscribed = Channel<String>(Channel.UNLIMITED)
    val ping = Channel<String>(Channel.UNLIMITED)
    val openedCount: Int get() = connections.size
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

    private class Link(private val peer: EncryptedCablePeer, private val token: String) : PlatformSocket {
        override fun send(text: String): Boolean {
            peer.receive(token, text)
            return true
        }

        override fun close(code: UShort, reason: String) {
            peer.closed.trySend(token)
        }

        override fun cancel() = Unit
    }

    override suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome = openCable(url, events)

    override suspend fun openCable(url: String, events: CableEvents): CableOutcome {
        val token = UUID.randomUUID().toString()
        val connection = Connection(events)
        connections[token] = connection
        lastOpened = token
        val challenge = buildJsonObject {
            put("type", "challenge")
            put("v", 1)
            put("kid", SealedServer.SERVER_PIN)
            put("challenge", encode(connection.salt))
            put("x25519", encode(connection.keys.public.encoded.takeLast(32).toByteArray()))
        }
        events.frame(text(challenge))
        return CableOutcome.Open(Link(this, token))
    }

    fun receive(token: String, text: String) {
        val connection = connections.getValue(token)
        val command = objectOf(text)
        if (command["command"]?.jsonPrimitive?.content == "subscribe") {
            connection.events.frame("""{"type":"confirm_subscription"}""")
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
        if (body["t"]?.jsonPrimitive?.content == "ping") {
            emit(token, buildJsonObject { put("t", "pong"); put("ts", body.getValue("ts")) })
            ping.trySend(token)
        }
        if (body["t"]?.jsonPrimitive?.content == "subscribe") {
            val id = body.getValue("id").jsonPrimitive.content
            val operation = body["operationName"]?.jsonPrimitive?.content ?: ""
            connection.subscriptions[operation] = id
            emit(token, buildJsonObject { put("t", "subscribed"); put("id", id) })
            subscribed.trySend(token)
            onSubscribe?.invoke(TransportRequest(body["query"]?.jsonPrimitive?.content ?: "", operation, body["variables"]?.toString() ?: "{}", NetworkPurpose.STANDARD))
        }
    }

    private fun openFrame(token: String, key: ByteArray): JsonObject {
        val parts = token.split(".")
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(key, "AES"), GCMParameterSpec(128, decode(parts[2])))
        cipher.updateAAD(parts[0].toByteArray())
        return objectOf(String(cipher.doFinal(decode(parts[3]) + decode(parts[4]))))
    }

    private fun emit(token: String, fields: JsonObject) {
        val connection = connections.getValue(token)
        synchronized(connection) { emitOrdered(connection, fields) }
    }

    private fun emitOrdered(connection: Connection, fields: JsonObject) {
        val sequence = connection.sequence.getAndIncrement()
        val body = buildJsonObject { fields.forEach { (key, value) -> put(key, value) }; put("n", sequence) }
        val header = encode("""{"alg":"dir","enc":"A256GCM"}""".toByteArray())
        val iv = ByteArray(12) { (sequence + 1).toByte() }
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.ENCRYPT_MODE, SecretKeySpec(connection.sendKey!!, "AES"), GCMParameterSpec(128, iv))
        cipher.updateAAD(header.toByteArray())
        val sealed = cipher.doFinal(body.toString().toByteArray())
        val frame = "$header..${encode(iv)}.${encode(sealed.dropLast(16).toByteArray())}.${encode(sealed.takeLast(16).toByteArray())}"
        connection.events.frame(text(buildJsonObject { put("message", buildJsonObject { put("frame", frame) }) }))
    }

    /** A data event of the open subscription. False while no subscription is open. */
    fun event(token: String, operation: String, result: JsonObject): Boolean {
        val id = connections.getValue(token).subscriptions[operation] ?: return false
        emit(token, buildJsonObject { put("t", "next"); put("id", id); put("result", result) })
        return true
    }

    /** A data event of the first open subscription. */
    fun event(token: String, result: JsonObject): Boolean {
        val operation = connections.getValue(token).subscriptions.keys.minOrNull() ?: return false
        return event(token, operation, result)
    }

    override fun event(operation: String, result: String): Boolean {
        val token = lastOpened ?: return false
        return event(token, operation, objectOf(result))
    }

    /** The socket breaks. */
    fun drop(token: String) {
        connections.getValue(token).events.ended(1006u, "dropped", null)
    }

    fun emitOld(token: String) {
        val id = connections.getValue(token).subscriptions.values.first()
        emit(token, buildJsonObject {
            put("t", "next")
            put("id", id)
            put("result", buildJsonObject { put("data", buildJsonObject { put("bulletinAdded", buildJsonObject { put("body", "old"); put("occurredAt", "2026-10-18T00:00:00Z") }) }) })
        })
    }
}

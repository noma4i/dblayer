package com.noma4i.dblayer

import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.CompletableDeferred
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/** The transport mock of the shared vocabulary. It answers no request by itself: a request waits until an input of the trace answers, fails or drops it. */
class CorpusMock : ScriptedTransport, ScriptedHttp {
    private class Seen(val id: String, val kind: String, val purpose: String, var announced: Boolean = false)

    override fun appHeaders(): Map<String, String> = emptyMap()

    private val seen = ArrayList<Seen>()
    private var next = 0
    private val waiting = ConcurrentHashMap<String, CompletableDeferred<String>>()
    private val waitingPuts = ConcurrentHashMap<String, CompletableDeferred<PutReply>>()
    private val waitingHttp = ConcurrentHashMap<String, Pair<JsonObject, CompletableDeferred<EncryptedReply>>>()
    private val sealed = SealedServer { _, _ -> JsonObject(emptyMap()) }
    private val cables = ConcurrentHashMap<String, String>()
    private val peer = EncryptedCablePeer(sealed)

    val count: Int get() = synchronized(seen) { seen.size }

    fun knows(id: String): Boolean = waiting.containsKey(id) || waitingPuts.containsKey(id) || waitingHttp.containsKey(id) || cables.containsKey(id)

    val unannounced: Int get() = synchronized(seen) { seen.count { !it.announced } }

    /** The requests that arrived since the last call, in arrival order, as `request` observations. */
    fun announce(): List<JsonObject> = synchronized(seen) {
        seen.filter { !it.announced }.map {
            it.announced = true
            observation("request", JsonObject(mapOf("id" to JsonPrimitive(it.id), "kind" to JsonPrimitive(it.kind), "purpose" to JsonPrimitive(it.purpose))))
        }
    }

    private fun record(kind: String, purpose: NetworkPurpose): String = synchronized(seen) {
        next += 1
        val id = "r$next"
        seen.add(Seen(id, kind, if (purpose == NetworkPurpose.STANDARD) "standard" else "transfer"))
        id
    }.also { TestPulse.pulse() }

    fun answer(id: String, body: String): Boolean {
        waiting.remove(id)?.let { return it.complete(body) }
        waitingHttp.remove(id)?.let { (request, reply) ->
            val parsed = runCatching { kotlinx.serialization.json.Json.parseToJsonElement(body) as? JsonObject }.getOrNull() ?: return false
            return reply.complete(EncryptedReply(200u, sealed.seal(request, parsed)))
        }
        waitingPuts.remove(id)?.let {
            val status = Regex("\"status\"\\s*:\\s*(\\d+)").find(body)?.groupValues?.get(1)?.toInt() ?: 200
            return it.complete(PutReply(status.toUShort(), null))
        }
        return false
    }

    private fun failure(kind: String): DbException? = when (kind) {
        "temporary" -> DbException.Network("temporary")
        "permanent" -> DbException.Graphql(listOf("permanent"), null)
        "auth" -> DbException.Http(401u, "auth")
        else -> null
    }

    fun fail(id: String, kind: String): Boolean {
        val error = failure(kind) ?: return false
        if (kind == "permanent" && waitingHttp.containsKey(id)) return answer(id, """{"errors":[{"message":"permanent","extensions":{"code":"permanent"}}]}""")
        if (kind == "auth") waitingHttp.remove(id)?.let { return it.second.complete(EncryptedReply(401u, "")) }
        return finish(id, error)
    }

    fun dropConnection(id: String): Boolean = finish(id, DbException.Network("dropped"))

    private fun finish(id: String, error: DbException): Boolean {
        waiting.remove(id)?.let { return it.completeExceptionally(error) }
        waitingPuts.remove(id)?.let { return it.completeExceptionally(error) }
        waitingHttp.remove(id)?.let { return it.second.completeExceptionally(error) }
        cables.remove(id)?.let { token -> bounded { peer.drop(token) }; return true }
        return false
    }

    /** Fails every waiting request with `Interrupted`. The runner calls it before a close: the app's transport cancels its calls when the database closes. */
    fun releaseAll() {
        for (id in waiting.keys.toList()) waiting.remove(id)?.completeExceptionally(DbException.Interrupted("closed"))
        for (id in waitingPuts.keys.toList()) waitingPuts.remove(id)?.completeExceptionally(DbException.Interrupted("closed"))
        for (id in waitingHttp.keys.toList()) waitingHttp.remove(id)?.second?.completeExceptionally(DbException.Interrupted("closed"))
    }

    fun ready(id: String): Boolean = cables.containsKey(id)

    fun frame(id: String, event: String): Boolean {
        val token = cables[id] ?: return false
        val result = kotlinx.serialization.json.Json.parseToJsonElement(event) as? JsonObject ?: return false
        return peer.event(token, result)
    }

    private suspend fun wait(kind: String, request: TransportRequest): String {
        val id = record(kind, request.purpose)
        val reply = CompletableDeferred<String>()
        waiting[id] = reply
        TestPulse.pulse()
        return reply.await()
    }

    override suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome {
        val id = record("subscription", NetworkPurpose.STANDARD)
        val outcome = peer.openCable(url, events)
        cables[id] = peer.lastOpened!!
        TestPulse.pulse()
        return outcome
    }

    override suspend fun send(request: HttpRequest): RawOutcome =
        if (request.headers["Content-Type"] == "application/jose") sendEncrypted(request) else sendGraphql(request)

    override suspend fun post(path: String, body: String, purpose: NetworkPurpose): EncryptedReply {
        val request = sealed.open(body)
        val id = record("http", purpose)
        val reply = CompletableDeferred<EncryptedReply>()
        waitingHttp[id] = request to reply
        TestPulse.pulse()
        return reply.await()
    }
    override suspend fun query(request: TransportRequest): String = wait("query", request)
    override suspend fun mutate(request: TransportRequest): String = wait("mutation", request)

    override suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply {
        val id = record("put", NetworkPurpose.TRANSFER)
        val reply = CompletableDeferred<PutReply>()
        waitingPuts[id] = reply
        TestPulse.pulse()
        return reply.await()
    }
}

/** One observation: `{"o":kind,"v":value}`, `v` left out when there is none. */
fun observation(kind: String, value: kotlinx.serialization.json.JsonElement? = null): JsonObject =
    JsonObject(if (value == null) mapOf("o" to JsonPrimitive(kind)) else mapOf("o" to JsonPrimitive(kind), "v" to value))

/** A JSON value from plain Kotlin values: maps, lists, strings, numbers, booleans. */
fun js(value: Any?): kotlinx.serialization.json.JsonElement = when (value) {
    null -> kotlinx.serialization.json.JsonNull
    is kotlinx.serialization.json.JsonElement -> value
    is String -> JsonPrimitive(value)
    is Boolean -> JsonPrimitive(value)
    is ULong -> JsonPrimitive(value.toLong())
    is UInt -> JsonPrimitive(value.toLong())
    is Number -> JsonPrimitive(value)
    is Map<*, *> -> JsonObject(value.entries.associate { (key, item) -> key as String to js(item) })
    is List<*> -> kotlinx.serialization.json.JsonArray(value.map { js(it) })
    else -> JsonPrimitive(value.toString())
}

fun obs(kind: String, vararg fields: Pair<String, Any?>): JsonObject = observation(kind, js(fields.toMap()))

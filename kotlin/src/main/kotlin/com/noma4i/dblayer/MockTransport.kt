package com.noma4i.dblayer

import kotlinx.coroutines.delay
import kotlinx.coroutines.channels.Channel
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/** Answers by operation name, can delay or fail answers, and lets a test push or break a subscription. */
class MockTransport : Transport {
    private val cableEvents = ConcurrentHashMap<String, Channel<EncryptedSocketEvent>>()
    private val sentFrames = CopyOnWriteArrayList<Pair<String, String>>()
    val cableFrames: List<Pair<String, String>> get() = sentFrames.toList()

    override suspend fun openCable(): String {
        val token = UUID.randomUUID().toString()
        cableEvents[token] = Channel(Channel.UNLIMITED)
        return token
    }

    override suspend fun cableSend(token: String, text: String) {
        if (!cableEvents.containsKey(token)) throw DbException.Network("cable socket is closed")
        sentFrames.add(token to text)
    }

    override suspend fun cableNext(token: String): EncryptedSocketEvent? = cableEvents[token]?.receiveCatching()?.getOrNull()

    override suspend fun cableClose(token: String) { cableEvents.remove(token)?.close() }

    fun emitCable(token: String, event: EncryptedSocketEvent) {
        val events = cableEvents[token] ?: throw DbException.Network("cable socket is closed")
        if (!events.trySend(event).isSuccess) throw DbException.Network("cable socket is closed")
        if (event is EncryptedSocketEvent.Closed) events.close()
    }
    private class Reply(val body: String?, val failure: DbException?, val delayMillis: Long)

    private val replies = ConcurrentHashMap<String, java.util.ArrayDeque<Reply>>()
    private val sinks = ConcurrentHashMap<String, SubscriptionSink>()
    private val issuedTokens = ConcurrentHashMap.newKeySet<String>()
    val unknownTokens = CopyOnWriteArrayList<String>()
    private val recorded = CopyOnWriteArrayList<TransportRequest>()

    val calls: List<TransportRequest> get() = recorded.toList()

    fun enqueue(operation: String, reply: String, delayMillis: Long = 0) = push(operation, Reply(reply, null, delayMillis))

    fun enqueue(operation: String, failure: DbException, delayMillis: Long = 0) = push(operation, Reply(null, failure, delayMillis))

    private fun push(operation: String, reply: Reply) {
        synchronized(replies) { replies.getOrPut(operation) { java.util.ArrayDeque() }.add(reply) }
    }

    fun emit(operation: String, json: String) { sinks[operation]?.event(json) }

    fun breakSubscription(operation: String) { sinks.remove(operation)?.failed(DbException.Network("connection lost")) }

    override suspend fun query(request: TransportRequest): String = answer(request)

    override suspend fun mutate(request: TransportRequest): String = answer(request)

    override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink): String {
        recorded.add(request)
        sinks[request.operationName] = sink
        issuedTokens.add(request.operationName)
        sink.ready()
        return request.operationName
    }

    override fun unsubscribe(token: String) {
        if (token !in issuedTokens) {
            unknownTokens.add(token)
            throw IllegalStateException("unsubscribe got a token the transport never issued: $token")
        }
        sinks.remove(token)
    }

    override suspend fun put(url: String, headers: Map<String, String>, bytes: ByteArray, progress: ByteProgress): PutReply {
        progress.sent(bytes.size.toULong())
        return PutReply(200u, null)
    }

    private suspend fun answer(request: TransportRequest): String {
        recorded.add(request)
        val reply = synchronized(replies) { replies[request.operationName]?.pollFirst() }
            ?: throw DbException.Network("no reply queued for ${request.operationName}")
        if (reply.delayMillis > 0) delay(reply.delayMillis)
        reply.failure?.let { throw it }
        return reply.body ?: "{}"
    }
}

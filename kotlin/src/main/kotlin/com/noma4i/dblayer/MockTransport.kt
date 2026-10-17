package com.noma4i.dblayer

import kotlinx.coroutines.delay
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/** Answers by operation name, can delay or fail answers, and lets a test push or break a subscription. */
class MockTransport : Transport {
    override suspend fun openCable(): String = throw DbException.Invalid("encrypted cable socket is not configured")
    override suspend fun cableSend(token: String, text: String): Unit = throw DbException.Invalid("encrypted cable socket is not configured")
    override suspend fun cableNext(token: String): EncryptedSocketEvent? = null
    override suspend fun cableClose(token: String) {}
    private class Reply(val body: String?, val failure: DbException?, val delayMillis: Long)

    private val replies = ConcurrentHashMap<String, java.util.ArrayDeque<Reply>>()
    private val sinks = ConcurrentHashMap<String, SubscriptionSink>()
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
        sink.ready()
        return request.operationName
    }

    override fun unsubscribe(token: String) { sinks.remove(token) }

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

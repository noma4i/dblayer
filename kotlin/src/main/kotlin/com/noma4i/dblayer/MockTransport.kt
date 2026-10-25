package com.noma4i.dblayer

import kotlinx.coroutines.delay
import kotlinx.coroutines.channels.Channel
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList

/** What a call without a queued reply gets: `Strict` fails it with `Invalid`, records it in `unscripted` and fails the test thread with `AssertionError` at its next touch of any mock, `NetworkError` throws the retried network error. */
enum class Unanswered { Strict, NetworkError }

/** Answers by operation name, can delay or fail answers, and lets a test push or break a subscription. */
class MockTransport(private val unanswered: Unanswered = Unanswered.Strict) : Transport {
    private class Missed(val owner: MockTransport, val message: String)

    private companion object {
        val missed = ConcurrentLinkedQueue<Missed>()
        val testThreads: MutableSet<Thread> = ConcurrentHashMap.newKeySet()

        init {
            Runtime.getRuntime().addShutdownHook(Thread {
                if (missed.isNotEmpty()) {
                    System.err.println(missed.joinToString("\n") { it.message })
                    Runtime.getRuntime().halt(1)
                }
            })
        }

        fun failTestThread() {
            if (Thread.currentThread() !in testThreads) return
            val batch = generateSequence { missed.poll() }.toList()
            if (batch.isNotEmpty()) throw AssertionError(batch.joinToString("\n") { it.message })
        }
    }

    private val createdAt = Throwable().stackTrace.firstOrNull { it.className != MockTransport::class.java.name }
    val unscripted = CopyOnWriteArrayList<TransportRequest>()

    init {
        failTestThread()
        testThreads.add(Thread.currentThread())
    }

    /** The requests that had no queued reply. Reading them clears the failure the test would get, so call it in a test that expects such a request. */
    fun claimUnscripted(): List<TransportRequest> {
        missed.removeAll { it.owner === this }
        return unscripted.toList()
    }
    private val cableEvents = ConcurrentHashMap<String, Channel<EncryptedSocketEvent>>()
    private val sentFrames = CopyOnWriteArrayList<Pair<String, String>>()
    val cableFrames: List<Pair<String, String>> get() { failTestThread(); return sentFrames.toList() }

    override suspend fun openCable(lease: CableLease) {
        val token = UUID.randomUUID().toString()
        cableEvents[token] = Channel(Channel.UNLIMITED)
        lease.opened(token)
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

    val calls: List<TransportRequest> get() { failTestThread(); return recorded.toList() }

    fun enqueue(operation: String, reply: String, delayMillis: Long = 0) = push(operation, Reply(reply, null, delayMillis))

    fun enqueue(operation: String, failure: DbException, delayMillis: Long = 0) = push(operation, Reply(null, failure, delayMillis))

    private fun push(operation: String, reply: Reply) {
        failTestThread()
        synchronized(replies) { replies.getOrPut(operation) { java.util.ArrayDeque() }.add(reply) }
    }

    fun emit(operation: String, json: String) { failTestThread(); sinks[operation]?.event(json) }

    fun breakSubscription(operation: String) { failTestThread(); sinks.remove(operation)?.failed(DbException.Network("connection lost")) }

    override suspend fun query(request: TransportRequest): String = answer(request)

    override suspend fun mutate(request: TransportRequest): String = answer(request)

    override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink) {
        recorded.add(request)
        sinks[request.operationName] = sink
        issuedTokens.add(request.operationName)
        sink.opened(request.operationName)
        sink.ready()
    }

    override fun unsubscribe(token: String) {
        failTestThread()
        if (token !in issuedTokens) unknownTokens.add(token)
        sinks.remove(token)
    }

    override suspend fun put(url: String, headers: Map<String, String>, file: PutFile, tag: String, progress: ByteProgress): PutReply {
        progress.sent(file.length)
        return PutReply(200u, null)
    }

    private suspend fun answer(request: TransportRequest): String {
        recorded.add(request)
        val reply = synchronized(replies) { replies[request.operationName]?.pollFirst() } ?: refuse(request)
        if (reply.delayMillis > 0) delay(reply.delayMillis)
        reply.failure?.let { throw it }
        return reply.body ?: "{}"
    }

    private fun refuse(request: TransportRequest): Nothing = when (unanswered) {
        Unanswered.NetworkError -> throw DbException.Network("no reply queued for ${request.operationName}")
        Unanswered.Strict -> {
            val detail = "MockTransport has no reply queued for ${request.operationName} variables=${request.variablesJson}"
            missed.add(Missed(this, "$detail (mock created at $createdAt)"))
            unscripted.add(request)
            throw DbException.Invalid(detail)
        }
    }
}

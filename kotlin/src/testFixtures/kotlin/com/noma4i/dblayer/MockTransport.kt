package com.noma4i.dblayer

import kotlinx.coroutines.delay
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList

/** What a call without a queued reply gets: `Strict` fails it with `Invalid`, records it in `unscripted` and fails the test thread with `AssertionError` at its next touch of any mock, `NetworkError` throws the retried network error. */
enum class Unanswered { Strict, NetworkError }

/** The server end of the encrypted session cable. `MockTransport` hands the cable calls of the session to it and pushes model events through it. */
interface CablePeer {
    /** Called with the request of every subscription the session opens on the cable. */
    var onSubscribe: ((TransportRequest) -> Unit)?

    /** A data event of the open subscription of that operation. False while it is not open. */
    fun event(operation: String, result: String): Boolean

    suspend fun openCable(url: String, events: CableEvents): CableOutcome
}

class MockTransport(private val unanswered: Unanswered = Unanswered.Strict) : ScriptedTransport {
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

    fun claimUnscripted(): List<TransportRequest> {
        missed.removeAll { it.owner === this }
        return unscripted.toList()
    }
    private val cableSinks = ConcurrentHashMap<String, CableEvents>()
    private val sentFrames = CopyOnWriteArrayList<Pair<String, String>>()
    val cableFrames: List<Pair<String, String>> get() { failTestThread(); return sentFrames.toList() }

    /** The cable of the session: events a test pushes with `emit` for an operation without a plain subscription go through it, and the cable calls of the session go to it. */
    @Volatile var cable: CablePeer? = null
        set(value) {
            value?.onSubscribe = { request -> recorded.add(request) }
            field = value
        }

    private class MockCable(private val sent: MutableList<Pair<String, String>>, private val token: String) : PlatformSocket {
        override fun send(text: String): Boolean = sent.add(token to text)

        override fun close(code: UShort, reason: String) = Unit
    }

    override suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome {
        cable?.let { return it.openCable(url, events) }
        val token = UUID.randomUUID().toString()
        cableSinks[token] = events
        return CableOutcome.Open(MockCable(sentFrames, token))
    }

    fun emitCable(token: String, frame: String) {
        (cableSinks[token] ?: throw DbException.Network("cable socket is closed")).frame(frame)
    }

    fun closeCable(token: String, code: UShort, reason: String) {
        (cableSinks.remove(token) ?: throw DbException.Network("cable socket is closed")).closed(code, reason)
    }

    private class Reply(val body: String?, val failure: DbException?, val delayMillis: Long, val hold: Hold? = null)

    private val replies = ConcurrentHashMap<String, java.util.ArrayDeque<Reply>>()
    private val recorded = CopyOnWriteArrayList<TransportRequest>()

    val calls: List<TransportRequest> get() { failTestThread(); return recorded.toList() }

    fun enqueue(operation: String, reply: String, delayMillis: Long = 0) = push(operation, Reply(reply, null, delayMillis))

    fun enqueue(operation: String, failure: DbException, delayMillis: Long = 0) = push(operation, Reply(null, failure, delayMillis))

    /** Queues an answer that waits at the server: `entered()` returns when the request arrived, and the answer goes out when the test calls `release()`. */
    fun hold(operation: String, reply: String = "{}", failure: DbException? = null): Hold {
        val hold = Hold()
        push(operation, Reply(if (failure == null) reply else null, failure, 0, hold))
        return hold
    }

    private fun push(operation: String, reply: Reply) {
        failTestThread()
        synchronized(replies) { replies.getOrPut(operation) { java.util.ArrayDeque() }.add(reply) }
    }

    fun emit(operation: String, json: String) {
        failTestThread()
        cable?.event(operation, json)
    }

    override suspend fun query(request: TransportRequest): String = answer(request)

    override suspend fun mutate(request: TransportRequest): String = answer(request)

    override suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply {
        progress.sent(file.length)
        return PutReply(200u, null)
    }

    private suspend fun answer(request: TransportRequest): String {
        recorded.add(request)
        val reply = synchronized(replies) { replies[request.operationName]?.pollFirst() } ?: refuse(request)
        if (reply.delayMillis > 0) delay(reply.delayMillis)
        reply.hold?.pass()
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

/** A request held at the server by `MockTransport.hold`. */
class Hold {
    private val arrived = kotlinx.coroutines.CompletableDeferred<Unit>()
    private val freed = kotlinx.coroutines.CompletableDeferred<Unit>()
    private val gone = kotlinx.coroutines.CompletableDeferred<Unit>()

    /** Returns when the request reached the transport. */
    suspend fun entered() = arrived.await()

    /** Returns when the library dropped the held request before its release. */
    suspend fun dropped() = gone.await()

    fun release() { freed.complete(Unit) }

    internal suspend fun pass() {
        arrived.complete(Unit)
        try {
            freed.await()
        } catch (cancelled: kotlinx.coroutines.CancellationException) {
            gone.complete(Unit)
            throw cancelled
        }
    }
}

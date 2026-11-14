package com.noma4i.dblayer

import kotlinx.serialization.json.put
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
    private class Missed(val message: String)

    private companion object {
        val missedQueues = ConcurrentLinkedQueue<ConcurrentLinkedQueue<Missed>>()
        val testThreads: MutableSet<Thread> = ConcurrentHashMap.newKeySet()

        init {
            Runtime.getRuntime().addShutdownHook(Thread {
                val unreported = missedQueues.flatten()
                if (unreported.isNotEmpty()) {
                    System.err.println(unreported.joinToString("\n") { it.message })
                    Runtime.getRuntime().halt(1)
                }
            })
        }
    }

    private val missed = ConcurrentLinkedQueue<Missed>()

    private fun failTestThread() {
        if (Thread.currentThread() !in testThreads) return
        val batch = generateSequence { missed.poll() }.toList()
        if (batch.isNotEmpty()) throw AssertionError(batch.joinToString("\n") { it.message })
    }

    private val createdAt = Throwable().stackTrace.firstOrNull { it.className != MockTransport::class.java.name }
    val unscripted = CopyOnWriteArrayList<TransportRequest>()

    init {
        testThreads.add(Thread.currentThread())
        missedQueues.add(missed)
    }

    private val cableSinks = ConcurrentHashMap<String, CableEvents>()
    private val sentFrames = CopyOnWriteArrayList<Pair<String, String>>()
    val cableFrames: List<Pair<String, String>> get() { failTestThread(); return sentFrames.toList() }

    /** The cable of the session: events a test pushes with `emit` for an operation without a plain subscription go through it, and the cable calls of the session go to it. */
    @Volatile var cable: CablePeer? = null
        set(value) {
            value?.onSubscribe = { request -> record(request); TestPulse.pulse() }
            field = value
        }

    private class MockCable(private val sent: MutableList<Pair<String, String>>, private val token: String) : PlatformSocket {
        override fun send(text: String): Boolean = sent.add(token to text)

        override fun close(code: UShort, reason: String) = Unit

        override fun cancel() = Unit
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
        (cableSinks.remove(token) ?: throw DbException.Network("cable socket is closed")).ended(code, reason, null)
    }

    private class Reply(val body: String?, val failure: DbException?, val hold: Hold? = null)

    private val contract = ServerContract()
    private val replies = ConcurrentHashMap<String, Reply>()
    private val tokens = java.util.concurrent.atomic.AtomicLong()
    private val recorded = CopyOnWriteArrayList<TransportRequest>()
    val recordedAt = CopyOnWriteArrayList<Long>()

    private fun record(request: TransportRequest) {
        recordedAt.add(System.nanoTime())
        recorded.add(request)
    }
    private val responders = ConcurrentHashMap<String, (TransportRequest) -> String?>()

    private class PutScript(val etag: String?, val failure: DbException?)

    private val puts = ConcurrentHashMap<String, java.util.ArrayDeque<PutScript>>()
    private val recordedPuts = CopyOnWriteArrayList<String>()

    /** The URLs of the PUTs the library sent, in order. */
    val putCalls: List<String> get() { failTestThread(); return recordedPuts.toList() }

    /** Queues the answer to the next PUT of `url`: each PUT takes one, a PUT without one is refused like a call without a reply. */
    fun enqueuePut(url: String, etag: String? = null) = pushPut(url, PutScript(etag, null))

    fun enqueuePut(url: String, failure: DbException) = pushPut(url, PutScript(null, failure))

    private fun pushPut(url: String, script: PutScript) {
        failTestThread()
        synchronized(puts) { puts.getOrPut(url) { java.util.ArrayDeque() }.add(script) }
    }

    val calls: List<TransportRequest> get() { failTestThread(); return recorded.toList() }

    fun enqueue(operation: String, reply: String) = push(operation, Reply(reply, null))

    fun enqueue(operation: String, failure: DbException) = push(operation, Reply(null, failure))

    /** Queues an answer that waits at the server: `entered()` returns when the request arrived, and the answer goes out when the test calls `release()`. */
    fun hold(operation: String, reply: String = "{}", failure: DbException? = null): Hold {
        val hold = Hold()
        push(operation, Reply(if (failure == null) reply else null, failure, hold))
        return hold
    }

    private fun store(reply: Reply): String {
        val token = "r${tokens.incrementAndGet()}"
        replies[token] = reply
        return token
    }

    /** The server contract holds the order of the scripted replies: the transport keeps the reply behind each token. */
    private fun push(operation: String, reply: Reply) {
        failTestThread()
        contract.script(operation, store(reply))
    }

    fun respondWith(operation: String, responder: (TransportRequest) -> String?) {
        failTestThread()
        responders[operation] = responder
    }

    fun emit(operation: String, json: String) {
        failTestThread()
        cable?.event(operation, json)
        TestPulse.pulse()
    }

    override suspend fun query(request: TransportRequest): String = answer(request)

    override suspend fun mutate(request: TransportRequest): String = answer(request)

    override suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply {
        recordedPuts.add(url)
        TestPulse.pulse()
        val script = synchronized(puts) { puts[url]?.pollFirst() } ?: refusePut(url)
        progress.sent(file.length)
        script.failure?.let { throw it }
        return PutReply(200u, script.etag)
    }

    private fun refusePut(url: String): Nothing = when (unanswered) {
        Unanswered.NetworkError -> throw DbException.Network("no PUT queued for $url")
        Unanswered.Strict -> {
            val detail = "MockTransport has no PUT queued for $url"
            missed.add(Missed("$detail (mock created at $createdAt)"))
            TestPulse.pulse()
            throw DbException.Invalid(detail)
        }
    }

    private fun wireOf(request: TransportRequest): String = kotlinx.serialization.json.buildJsonObject {
        put("requestId", request.requestId.toString())
        put("sequence", kotlinx.serialization.json.JsonNull)
        put("body", kotlinx.serialization.json.buildJsonObject {
            put("operationName", request.operationName)
            put("variables", kotlinx.serialization.json.Json.parseToJsonElement(request.variablesJson))
            put("query", request.document)
        })
    }.toString()

    /** The reply the server gave a request: a body or a refusal of the server, not a failure of the transport. The server contract decides: a repeat of a pair gets the reply of the first send, a second body under one pair and a request nobody scripted are refused. */
    private suspend fun answer(request: TransportRequest): String {
        record(request)
        TestPulse.pulse()
        val scope = request.author.toString()
        val wire = wireOf(request)
        val settle = { reply: Reply -> contract.settle(scope, wire, store(Reply(reply.body, reply.failure))) }
        val reply = when (val admitted = contract.admit(scope, wire)) {
            is ContractAdmission.Stored -> replies.getValue(admitted.token)
            is ContractAdmission.TwoBodies -> refuse(request, "MockTransport got the pair $scope#${request.requestId} with a second body: ${request.operationName} variables=${request.variablesJson}")
            is ContractAdmission.Refused -> refuse(request)
            is ContractAdmission.Fresh -> responders[request.operationName]?.invoke(request)?.let { body -> Reply(body, null).also(settle) }
                ?: (contract.take(request.operationName)?.let { replies[it] } ?: refuse(request)).also { if (!lostAnswer(it.failure)) settle(it) }
        }
        reply.hold?.pass()
        return deliver(reply)
    }

    private fun lostAnswer(failure: DbException?): Boolean = failure is DbException.Network || failure is DbException.Offline || failure is DbException.Interrupted || failure is DbException.Dropped

    private fun deliver(reply: Reply): String {
        TestPulse.pulse()
        reply.failure?.let { throw it }
        return reply.body ?: "{}"
    }

    private fun refuse(request: TransportRequest, given: String? = null): Nothing = when {
        unanswered == Unanswered.NetworkError && given == null -> throw DbException.Network("no reply queued for ${request.operationName}")
        else -> {
            val detail = given ?: "MockTransport has no reply queued for ${request.operationName} variables=${request.variablesJson}"
            missed.add(Missed("$detail (mock created at $createdAt)"))
            unscripted.add(request)
            TestPulse.pulse()
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

    fun release() {
        freed.complete(Unit)
        TestPulse.pulse()
    }

    internal suspend fun pass() {
        arrived.complete(Unit)
        TestPulse.pulse()
        try {
            freed.await()
        } catch (cancelled: kotlinx.coroutines.CancellationException) {
            gone.complete(Unit)
            TestPulse.pulse()
            throw cancelled
        }
    }
}

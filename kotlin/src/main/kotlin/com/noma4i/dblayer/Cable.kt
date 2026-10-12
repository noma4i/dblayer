package com.noma4i.dblayer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response as HttpResponse
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import java.util.UUID
import kotlin.random.Random

data class CableRecycle(val cause: String, val uptime: Double, val channels: Int, val drops: Int)

class CableTimers(
    val now: () -> Double = { System.currentTimeMillis() / 1000.0 },
    val sleep: suspend (Double) -> Unit = { delay((it * 1000).toLong()) },
    val jitter: (Double) -> Double = { Random.nextDouble(0.0, it.coerceAtLeast(Double.MIN_VALUE)) },
    val recycled: (CableRecycle) -> Unit = {},
)

interface CableSocket {
    suspend fun open()
    fun close()
    suspend fun send(text: String)
    suspend fun receive(): String
}

class CableSocketException(val reason: Reason) : Exception(reason.name) {
    enum class Reason { UnsupportedFrame, Closed }
}

class OkHttpCableSocket(private val url: String, private val token: String, private val client: OkHttpClient = OkHttpClient()) : CableSocket {
    private val messages = Channel<String>(Channel.UNLIMITED)
    private var socket: WebSocket? = null

    override suspend fun open() {
        val opened = CompletableDeferred<HttpResponse>()
        val request = Request.Builder().url(url)
            .header("Authorization", "Bearer $token")
            .header("Sec-WebSocket-Protocol", "actioncable-v1-json, actioncable-unsupported")
            .build()
        socket = client.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: HttpResponse) { opened.complete(response) }
            override fun onMessage(webSocket: WebSocket, text: String) { messages.trySend(text) }
            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) { messages.close() }
            override fun onFailure(webSocket: WebSocket, t: Throwable, response: HttpResponse?) {
                opened.completeExceptionally(t)
                messages.close(t)
            }
        })
        val negotiated = opened.await().header("Sec-WebSocket-Protocol")
        if (!isSupported(negotiated)) {
            close()
            throw CableSocketException(CableSocketException.Reason.UnsupportedFrame)
        }
    }

    override fun close() {
        socket?.close(1000, null)
        socket = null
    }

    override suspend fun send(text: String) {
        val current = socket ?: throw CableSocketException(CableSocketException.Reason.Closed)
        if (!current.send(text)) throw CableSocketException(CableSocketException.Reason.Closed)
    }

    override suspend fun receive(): String = messages.receiveCatching().let { result ->
        result.getOrNull() ?: throw (result.exceptionOrNull() ?: CableSocketException(CableSocketException.Reason.Closed))
    }

    companion object {
        fun isSupported(protocol: String?) = protocol == "actioncable-v1-json"
    }
}

enum class CableConnectionState { Idle, Connecting, Open, Closing, Closed }

@OptIn(ExperimentalCoroutinesApi::class)
internal val serial: CoroutineDispatcher get() = Dispatchers.Default.limitedParallelism(1)

class CableConnection(
    private val url: String,
    token: String,
    private val makeSocket: (String, String) -> CableSocket,
    private val timers: CableTimers = CableTimers(),
) {
    private val confined = serial
    private val scope = CoroutineScope(SupervisorJob() + confined)
    private val stream = Channel<CableFrame>(Channel.UNLIMITED)
    private var token = token
    private var socket: CableSocket? = null
    private var generation = 0
    private var readJob: Job? = null
    private var watchdogJob: Job? = null
    private var resetJob: Job? = null
    private var attempts = 0
    private var drops = 0
    private var pendingCause = ""
    private var pendingChannels = 0
    private var requestedAt = Double.NEGATIVE_INFINITY
    private var attemptStartedAt = Double.NEGATIVE_INFINITY
    private var openedAt: Double? = null
    private var closedAt: Double? = null
    private var isStopped = false

    var state = CableConnectionState.Idle
        private set
    var lastFrameAt: Double? = null
        private set

    val frames: Flow<CableFrame> get() = stream.receiveAsFlow()

    suspend fun open() = withContext(confined) {
        isStopped = false
        openSocket()
        if (watchdogJob == null) armWatchdog()
        Unit
    }

    suspend fun closeSocket() = withContext(confined) { closeSocketNow() }

    private fun closeSocketNow() {
        if (socket == null && state != CableConnectionState.Connecting) return
        generation += 1
        state = CableConnectionState.Closing
        readJob?.cancel()
        readJob = null
        socket?.close()
        socket = null
        markClosed()
    }

    suspend fun close() = withContext(confined) {
        isStopped = true
        watchdogJob?.cancel()
        watchdogJob = null
        resetJob?.cancel()
        resetJob = null
        generation += 1
        readJob?.cancel()
        readJob = null
        socket?.close()
        socket = null
        state = CableConnectionState.Closed
        stream.close()
    }

    suspend fun send(command: CableCommand) {
        val current = withContext(confined) { socket } ?: throw CableSocketException(CableSocketException.Reason.Closed)
        current.send(command.json)
    }

    suspend fun foregroundCheck() = withContext(confined) {
        timers.sleep(CableHeartbeat.FOREGROUND_DELAY)
        if (isStopped || state == CableConnectionState.Connecting || !(socket == null || isStale())) return@withContext
        closeSocketNow()
        openSocket()
        Unit
    }

    suspend fun resetAttempts() = withContext(confined) { attempts = 0 }

    suspend fun replaceToken(newToken: String, channels: Int = 0) {
        withContext(confined) { token = newToken }
        reset("token_replaced", channels)
    }

    suspend fun reset(cause: String, channels: Int) {
        val job = withContext(confined) {
            pendingCause = cause
            pendingChannels = channels
            requestedAt = timers.now()
            resetJob?.cancel()
            scope.launch { runReset() }.also { resetJob = it }
        }
        job.join()
    }

    private suspend fun runReset() {
        timers.sleep(CableReconnect.MERGE_DELAY)
        var attempt = 0
        while (kotlin.coroutines.coroutineContext.isActive && !isStopped && !hasAttemptSinceRequest) {
            attempt += 1
            if (attempt == 1) timers.recycled(CableRecycle(pendingCause, openedAt?.let { timers.now() - it } ?: 0.0, pendingChannels, drops))
            val wait = CableReconnect.delay(attempt, timers.jitter(1.0))
            if (wait > 0) timers.sleep(wait)
            if (!kotlin.coroutines.coroutineContext.isActive || hasAttemptSinceRequest) return
            closeSocketNow()
            if (openSocket()) return
        }
    }

    private val hasAttemptSinceRequest: Boolean get() = (socket != null || state == CableConnectionState.Connecting) && attemptStartedAt > requestedAt

    private suspend fun openSocket(): Boolean {
        if (socket != null || state == CableConnectionState.Connecting || isStopped) return socket != null
        attemptStartedAt = timers.now()
        state = CableConnectionState.Connecting
        generation += 1
        val current = generation
        val candidate = makeSocket(url, token)
        try {
            candidate.open()
        } catch (error: CancellationException) {
            throw error
        } catch (error: Exception) {
            if (current != generation) return false
            if (error is CableSocketException && error.reason == CableSocketException.Reason.UnsupportedFrame) closeAll() else markClosed()
            return false
        }
        if (current != generation || isStopped) {
            candidate.close()
            return false
        }
        socket = candidate
        state = CableConnectionState.Open
        openedAt = timers.now()
        armWatchdog()
        readJob = scope.launch { readFrames(candidate, current) }
        return true
    }

    private fun closeAll() {
        isStopped = true
        watchdogJob?.cancel()
        watchdogJob = null
        resetJob?.cancel()
        resetJob = null
        generation += 1
        readJob?.cancel()
        readJob = null
        socket?.close()
        socket = null
        state = CableConnectionState.Closed
        stream.close()
    }

    private suspend fun readFrames(from: CableSocket, current: Int) {
        try {
            while (kotlin.coroutines.coroutineContext.isActive) {
                val text = from.receive()
                val frame = if (current == generation) CableFrame.parse(text) else null
                if (frame == null) continue
                lastFrameAt = timers.now()
                armWatchdog()
                if (frame == CableFrame.Welcome) attempts = 0
                stream.trySend(frame)
            }
        } catch (error: CancellationException) {
            throw error
        } catch (error: Exception) {
            if (current != generation) return
            from.close()
            socket = null
            readJob = null
            drops += 1
            markClosed()
        }
    }

    private fun markClosed() {
        state = CableConnectionState.Closed
        closedAt = timers.now()
        armWatchdog()
        stream.trySend(CableFrame.Closed)
    }

    private fun armWatchdog() {
        if (isStopped) return
        val wait = if (socket != null && state == CableConnectionState.Open) CableHeartbeat.STALE_AFTER
        else CableHeartbeat.pollInterval(attempts, timers.jitter(CableHeartbeat.jitterBound(attempts)))
        watchdogJob?.cancel()
        watchdogJob = scope.launch {
            timers.sleep(wait)
            watchdogFired()
        }
    }

    private suspend fun watchdogFired() {
        watchdogJob = null
        if (isStopped) return
        reopenIfStale()
        if (watchdogJob == null) armWatchdog()
    }

    private suspend fun reopenIfStale() {
        if (state == CableConnectionState.Connecting || !isStale()) return
        val closed = closedAt
        if (closed != null && timers.now() - closed < CableHeartbeat.STALE_AFTER) {
            attempts += 1
            return
        }
        drops += 1
        closeSocketNow()
        timers.sleep(CableHeartbeat.REOPEN_DELAY)
        if (isStopped) return
        openSocket()
    }

    private fun isStale(): Boolean {
        val latest = listOfNotNull(lastFrameAt, openedAt).maxOrNull()
        return CableHeartbeat.isStale(latest, timers.now())
    }
}

class CableClientException(val reason: Reason) : Exception(reason.name) {
    enum class Reason { Rejected }
}

private class CableSubscription(
    var operationId: String,
    val document: String,
    val operationName: String,
    val events: Channel<GraphQLSubscriptionEvent>,
) {
    var attempt = 0
    var serverId: String? = null
    val readyRequestIds = mutableSetOf<String>()
    val requestId: String get() = GraphQLChannel.requestId(operationId, attempt)

    fun rekey(newOperationId: String) {
        operationId = newOperationId
        attempt = 0
        serverId = null
        readyRequestIds.clear()
    }
}

class CableClient(
    url: String,
    token: String,
    makeSocket: (String, String) -> CableSocket,
    private val makeChannelId: () -> String = { UUID.randomUUID().toString() },
    private val pingInterval: Double = OnlineChannel.PING_INTERVAL,
    private val timers: CableTimers = CableTimers(),
) {
    private val connection = CableConnection(url, token, makeSocket, timers)
    private val confined = serial
    private val scope = CoroutineScope(SupervisorJob() + confined)
    private val subscriptions = linkedMapOf<String, CableSubscription>()
    private var sharedChannelId: String? = null
    private var isSharedRequested = false
    private var isSharedConfirmed = false
    private var hasWelcomed = false
    private var presenceUserId: String? = null
    private var presenceJob: Job? = null
    private var isPingPaused = false
    private var isPresenceConfirmed = false

    suspend fun openPresence(userId: String) = withContext(confined) {
        presenceUserId = userId
        if (hasWelcomed) scope.launch { runCatching { sendPresenceSubscribe() } }
        Unit
    }

    suspend fun start() {
        val frames = connection.frames
        connection.open()
        frames.collect { frame -> withContext(confined) { handle(frame) } }
        connection.close()
        withContext(confined) { finishSubscriptions() }
    }

    suspend fun subscribeGraphQL(document: String, operationName: String): Flow<GraphQLSubscriptionEvent> = withContext(confined) {
        if (sharedChannelId == null) sharedChannelId = makeChannelId()
        val operationId = makeChannelId()
        val events = Channel<GraphQLSubscriptionEvent>(Channel.UNLIMITED)
        subscriptions[operationId] = CableSubscription(operationId, document, operationName, events)
        if (hasWelcomed) scope.launch { activate(operationId) }
        events.receiveAsFlow()
    }

    suspend fun replaceToken(token: String) {
        withContext(confined) {
            hasWelcomed = false
            isPresenceConfirmed = false
            presenceJob?.cancel()
            presenceJob = null
            resetSharedState()
            sharedChannelId = if (subscriptions.isEmpty()) null else makeChannelId()
            val rekeyed = subscriptions.values.toList()
            subscriptions.clear()
            rekeyed.forEach { it.rekey(makeChannelId()); subscriptions[it.operationId] = it }
        }
        connection.replaceToken(token, withContext(confined) { channelCount })
    }

    suspend fun isConnected(): Boolean = connection.state == CableConnectionState.Open

    suspend fun foregroundCheck() = connection.foregroundCheck()

    suspend fun reconnect(cause: String) = connection.reset(cause, withContext(confined) { channelCount })

    suspend fun pausePing() = withContext(confined) {
        isPingPaused = true
        presenceJob?.cancel()
        presenceJob = null
    }

    suspend fun resumePing() {
        withContext(confined) { isPingPaused = false }
        connection.resetAttempts()
        withContext(confined) {
            val user = presenceUserId
            if (isPresenceConfirmed && user != null) startPresencePing(user)
        }
    }

    suspend fun stop() {
        withContext(confined) { finishSubscriptions() }
        connection.close()
    }

    private suspend fun handle(frame: CableFrame) {
        when (frame) {
            CableFrame.Welcome -> resubscribeAll()
            is CableFrame.Confirmed -> {
                val user = presenceUserId
                if (user != null && frame.identifier == OnlineChannel.identifier(user)) {
                    isPresenceConfirmed = true
                    if (!isPingPaused) startPresencePing(user)
                    return
                }
                handleConfirmation(frame.identifier)
            }
            is CableFrame.Rejected -> if (frame.identifier == sharedIdentifier) {
                resetSharedState()
                finishSubscriptions(CableClientException(CableClientException.Reason.Rejected))
            }
            is CableFrame.Message -> handleMessage(frame.identifier, frame.payload)
            is CableFrame.Disconnect -> if (frame.reconnect) connection.closeSocket() else connection.close()
            CableFrame.Closed -> {
                hasWelcomed = false
                isPresenceConfirmed = false
                resetSharedState()
                presenceJob?.cancel()
                presenceJob = null
            }
            is CableFrame.Ping -> Unit
        }
    }

    private suspend fun resubscribeAll() {
        hasWelcomed = true
        resetSharedState()
        val identifier = sharedIdentifier
        if (identifier != null && subscriptions.isNotEmpty()) {
            isSharedRequested = true
            runCatching { connection.send(CableCommand.Subscribe(identifier)) }
        }
        runCatching { sendPresenceSubscribe() }
    }

    private suspend fun handleConfirmation(identifier: String) {
        if (identifier != sharedIdentifier) return
        isSharedConfirmed = true
        subscriptions.keys.toList().forEach { execute(it) }
    }

    private suspend fun activate(operationId: String) {
        if (isSharedConfirmed) {
            execute(operationId)
            return
        }
        val identifier = sharedIdentifier
        if (isSharedRequested || identifier == null) return
        isSharedRequested = true
        runCatching { connection.send(CableCommand.Subscribe(identifier)) }
    }

    private suspend fun execute(operationId: String) {
        val channelId = sharedChannelId ?: return
        val subscription = subscriptions[operationId] ?: return
        subscription.attempt += 1
        subscription.serverId = null
        runCatching { connection.send(GraphQLChannel.execute(channelId, operationId, subscription.attempt, subscription.document, subscription.operationName)) }
    }

    private fun operationId(route: GraphQLChannel.Route): String? {
        route.requestId?.let { requestId -> return subscriptions.entries.firstOrNull { it.value.requestId == requestId }?.key }
        val subscriptionId = route.subscriptionId ?: return null
        return subscriptions.entries.firstOrNull { it.value.serverId == subscriptionId }?.key
    }

    private fun handleMessage(identifier: String, payload: kotlinx.serialization.json.JsonElement) {
        if (identifier != sharedIdentifier) return
        val route = GraphQLChannel.route(payload)
        val id = operationId(route) ?: return
        val subscription = subscriptions[id] ?: return
        route.subscriptionId?.let { subscription.serverId = it }
        val requestId = subscription.requestId
        when (val event = GraphQLChannel.event(payload, requestId) ?: return) {
            GraphQLSubscriptionEvent.Ready -> if (subscription.readyRequestIds.add(requestId)) subscription.events.trySend(GraphQLSubscriptionEvent.Ready)
            is GraphQLSubscriptionEvent.Payload -> subscription.events.trySend(event)
            is GraphQLSubscriptionEvent.Errors -> subscription.events.trySend(event)
            GraphQLSubscriptionEvent.Completed -> {
                finishSubscription(id, null)
                val shared = sharedIdentifier
                if (subscriptions.isEmpty() && shared != null) {
                    resetSharedState()
                    scope.launch { runCatching { connection.send(CableCommand.Unsubscribe(shared)) } }
                }
            }
        }
    }

    private val sharedIdentifier: String? get() = sharedChannelId?.let { GraphQLChannel.identifier(it) }

    private val channelCount: Int get() = (if (subscriptions.isEmpty()) 0 else 1) + if (presenceUserId == null) 0 else 1

    private fun resetSharedState() {
        isSharedRequested = false
        isSharedConfirmed = false
    }

    private suspend fun sendPresenceSubscribe() {
        val user = presenceUserId ?: return
        connection.send(OnlineChannel.subscribe(user))
    }

    private fun startPresencePing(userId: String) {
        presenceJob?.cancel()
        presenceJob = scope.launch {
            while (isActive) {
                runCatching { connection.send(OnlineChannel.ping(userId, System.currentTimeMillis())) }
                timers.sleep(pingInterval)
            }
        }
    }

    private fun finishSubscription(channelId: String, error: Throwable?) {
        subscriptions.remove(channelId)?.events?.close(error)
    }

    private fun finishSubscriptions(error: Throwable? = null) {
        presenceJob?.cancel()
        presenceJob = null
        val active = subscriptions.values.toList()
        subscriptions.clear()
        active.forEach { it.events.close(error) }
    }
}

class NetworkRestore(
    private val client: CableClient,
    private val sync: suspend (Boolean) -> Unit,
    private val timers: CableTimers = CableTimers(),
) {
    private val confined = serial
    private val scope = CoroutineScope(SupervisorJob() + confined)
    private var wasOffline = false
    private var deferred: Job? = null

    var isOnline = true
        private set

    suspend fun update(isOnline: Boolean, subscriptionsActive: Boolean) = withContext(confined) {
        this@NetworkRestore.isOnline = isOnline
        deferred?.cancel()
        deferred = null
        if (!subscriptionsActive) {
            wasOffline = false
            return@withContext
        }
        if (!isOnline) {
            wasOffline = true
            return@withContext
        }
        if (!wasOffline) return@withContext
        wasOffline = false
        deferred = scope.launch {
            timers.sleep(ALIVE_DELAY)
            if (!isActive) return@launch
            val alive = async { client.foregroundCheck() }
            timers.sleep(SYNC_DELAY)
            if (!isActive) return@launch
            sync(false)
            alive.await()
        }
    }

    companion object {
        const val ALIVE_DELAY = 0.5
        const val SYNC_DELAY = 1.0
    }
}

class AppLifecycle(
    private val client: CableClient,
    private val presence: suspend (OnlineChannel.PresenceState) -> Unit,
    private val sync: suspend (Boolean) -> Unit,
) {
    enum class Phase { Active, Inactive, Background }

    private val confined = serial
    private val scope = CoroutineScope(SupervisorJob() + confined)
    private var isBackground = false

    suspend fun phaseChanged(phase: Phase) = withContext(confined) {
        when (phase) {
            Phase.Inactive -> Unit
            Phase.Background -> {
                if (isBackground) return@withContext
                isBackground = true
                client.pausePing()
                scope.launch { presence(OnlineChannel.PresenceState.Offline) }
                Unit
            }
            Phase.Active -> {
                if (!isBackground) return@withContext
                isBackground = false
                resume()
            }
        }
    }

    private suspend fun resume() = kotlinx.coroutines.coroutineScope {
        client.resumePing()
        val online = async { presence(OnlineChannel.PresenceState.Online) }
        val revived = async { reviveSocketThenSync() }
        online.await()
        revived.await()
    }

    private suspend fun reviveSocketThenSync() = kotlinx.coroutines.coroutineScope {
        if (!client.isConnected()) {
            client.reconnect("app_foregrounded")
            sync(true)
            return@coroutineScope
        }
        val alive = async { client.foregroundCheck() }
        sync(true)
        alive.await()
    }
}

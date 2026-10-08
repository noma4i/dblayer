package com.noma4i.dblayer

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.put
import kotlin.math.min
import kotlin.math.pow

sealed interface CableFrame {
    data object Welcome : CableFrame
    data class Ping(val epochSeconds: Double) : CableFrame
    data class Confirmed(val identifier: String) : CableFrame
    data class Rejected(val identifier: String) : CableFrame
    data class Disconnect(val reason: String, val reconnect: Boolean) : CableFrame
    data class Message(val identifier: String, val payload: JsonElement) : CableFrame
    data object Closed : CableFrame

    companion object {
        fun parse(json: String): CableFrame? {
            val fields = runCatching { parseJson(json) }.getOrNull() as? JsonObject ?: return null
            val type = fields["type"] ?: return message(fields)
            val name = (type as? JsonPrimitive)?.takeIf { it.isString }?.content ?: return null
            return when (name) {
                "welcome" -> Welcome
                "ping" -> (fields["message"] as? JsonPrimitive)?.takeIf { !it.isString }?.doubleOrNull?.let { Ping(it) }
                "confirm_subscription" -> text(fields["identifier"])?.let { Confirmed(it) }
                "reject_subscription" -> text(fields["identifier"])?.let { Rejected(it) }
                "disconnect" -> disconnect(fields)
                else -> null
            }
        }

        private fun text(value: JsonElement?) = (value as? JsonPrimitive)?.takeIf { it.isString }?.contentOrNull

        private fun disconnect(fields: JsonObject): CableFrame? {
            val reason = text(fields["reason"]) ?: return null
            val reconnect = when (val value = fields["reconnect"]) {
                null -> true
                is JsonPrimitive -> value.content.takeIf { !value.isString && (it == "true" || it == "false") }?.toBooleanStrict() ?: return null
                else -> return null
            }
            return Disconnect(reason, reconnect)
        }

        private fun message(fields: JsonObject): CableFrame? {
            val identifier = text(fields["identifier"]) ?: return null
            val payload = fields["message"] ?: return null
            return Message(identifier, payload)
        }
    }
}

sealed interface CableCommand {
    val json: String

    data class Subscribe(val identifier: String) : CableCommand {
        override val json get() = """{"command":"subscribe","identifier":${quoted(identifier)}}"""
    }

    data class Unsubscribe(val identifier: String) : CableCommand {
        override val json get() = """{"command":"unsubscribe","identifier":${quoted(identifier)}}"""
    }

    data class Message(val identifier: String, val data: String) : CableCommand {
        override val json get() = """{"command":"message","identifier":${quoted(identifier)},"data":${quoted(data)}}"""
    }

    companion object {
        fun identifier(channel: String, parameters: List<Pair<String, String>>): String =
            "{" + (listOf("\"channel\":${quoted(channel)}") + parameters.map { (key, value) -> "${quoted(key)}:${quoted(value)}" }).joinToString(",") + "}"

        internal fun quoted(value: String) = String(CanonicalJson.encode(JsonPrimitive(value)))
    }
}

sealed interface GraphQLSubscriptionEvent {
    data object Ready : GraphQLSubscriptionEvent
    data class Payload(val value: JsonElement) : GraphQLSubscriptionEvent
    data class Errors(val messages: List<String>) : GraphQLSubscriptionEvent
    data object Completed : GraphQLSubscriptionEvent
}

object GraphQLChannel {
    const val CHANNEL_NAME = "GraphqlChannel"
    const val API_VERSION = "2"

    fun identifier(channelId: String) = CableCommand.identifier(CHANNEL_NAME, listOf("channelId" to channelId, "api_version" to API_VERSION))

    fun requestId(channelId: String, attempt: Int) = "$channelId:$attempt"

    fun execute(channelId: String, attempt: Int, document: String, operationName: String): CableCommand {
        val q = CableCommand.Companion::quoted
        val data = "{\"action\":${q("execute")},\"requestId\":${q(requestId(channelId, attempt))},\"query\":${q(document)},\"variables\":{},\"operationName\":${q(operationName)}}"
        return CableCommand.Message(identifier(channelId), data)
    }

    fun event(payload: JsonElement, requestId: String): GraphQLSubscriptionEvent? {
        val fields = payload as? JsonObject ?: return null
        if ((fields["more"] as? JsonPrimitive)?.content == "false" && !(fields["more"] as JsonPrimitive).isString) return GraphQLSubscriptionEvent.Completed
        val ready = fields["subscriptionReady"] as? JsonPrimitive
        if (ready != null && !ready.isString && ready.content == "true") {
            if ((fields["requestId"] as? JsonPrimitive)?.takeIf { it.isString }?.content == requestId) return GraphQLSubscriptionEvent.Ready
        }
        val result = fields["result"] as? JsonObject ?: return null
        val data = result["data"] as? JsonObject
        if (data != null && data.isNotEmpty()) return GraphQLSubscriptionEvent.Payload(data)
        val errors = result["errors"] as? kotlinx.serialization.json.JsonArray ?: return null
        if (errors.isEmpty()) return null
        return GraphQLSubscriptionEvent.Errors(errors.mapNotNull { ((it as? JsonObject)?.get("message") as? JsonPrimitive)?.takeIf { message -> message.isString }?.content })
    }
}

object OnlineChannel {
    const val CHANNEL_NAME = "OnlineChannel"
    const val PING_INTERVAL = 30.0
    const val LEASE_SECONDS = 90.0

    enum class PresenceState { Online, Offline }

    fun identifier(userId: String) = CableCommand.identifier(CHANNEL_NAME, listOf("channelId" to userId))

    fun subscribe(userId: String): CableCommand = CableCommand.Subscribe(identifier(userId))

    fun ping(userId: String, epochMillis: Long): CableCommand =
        CableCommand.Message(identifier(userId), "{\"action\":\"ping\",\"timestamp\":$epochMillis}")
}

object CableHeartbeat {
    const val STALE_AFTER = 6.0
    const val REOPEN_DELAY = 0.5
    const val FOREGROUND_DELAY = 0.2
    const val MAXIMUM_ATTEMPTS = 10

    fun isStale(lastFrameAt: Double?, now: Double) = lastFrameAt == null || now - lastFrameAt >= STALE_AFTER

    fun pollInterval(attempts: Int, jitter: Double) = STALE_AFTER * 1.15.pow(min(attempts, MAXIMUM_ATTEMPTS)) * (1 + jitter)

    fun jitterBound(attempts: Int) = if (attempts == 0) 1.0 else 0.15
}

object CableReconnect {
    const val MERGE_DELAY = 0.1
    const val MAXIMUM_DELAY = 10.0
    const val JITTER_SHARE = 0.3

    fun delay(attempt: Int, jitter: Double): Double {
        if (attempt < 2) return 0.0
        return min(MAXIMUM_DELAY, 2.0.pow(attempt - 2)) * (1 + JITTER_SHARE * jitter)
    }
}

enum class PresenceStep { SendPing, SkipPing, StopPing }

object PresenceSchedule {
    fun step(isForeground: Boolean, hasConsumer: Boolean, isReconnecting: Boolean): PresenceStep {
        if (!isForeground) return PresenceStep.StopPing
        return if (!hasConsumer || isReconnecting) PresenceStep.SkipPing else PresenceStep.SendPing
    }

    fun nextPingDelay(lastPingAt: Double?, now: Double): Double =
        if (lastPingAt == null) 0.0 else maxOf(0.0, OnlineChannel.PING_INTERVAL - (now - lastPingAt))

    fun leaseExpired(lastPingAt: Double, now: Double) = now - lastPingAt > OnlineChannel.LEASE_SECONDS

    fun keepsLeaseOnUnsubscribe() = true
}

enum class SubscriptionLifecycle { Requested, Ready, Receiving, Failed, Cancelled }

enum class SubscriptionLifecycleEvent { ServerAck, Payload, Failure, Cancel }

object SubscriptionRules {
    fun next(stage: SubscriptionLifecycle, event: SubscriptionLifecycleEvent): SubscriptionLifecycle = when (event) {
        SubscriptionLifecycleEvent.ServerAck -> if (stage == SubscriptionLifecycle.Requested) SubscriptionLifecycle.Ready else stage
        SubscriptionLifecycleEvent.Payload -> if (stage == SubscriptionLifecycle.Ready || stage == SubscriptionLifecycle.Receiving) SubscriptionLifecycle.Receiving else stage
        SubscriptionLifecycleEvent.Failure -> SubscriptionLifecycle.Failed
        SubscriptionLifecycleEvent.Cancel -> SubscriptionLifecycle.Cancelled
    }

    fun deliversReady(stage: SubscriptionLifecycle) = stage == SubscriptionLifecycle.Ready

    fun acceptsPayload(stage: SubscriptionLifecycle) = stage == SubscriptionLifecycle.Ready || stage == SubscriptionLifecycle.Receiving
}

enum class SubscriptionReport(val wire: String) { Silent("silent"), Degraded("subscription-degraded"), Permanent("subscription-permanent") }

object SubscriptionRetry {
    const val BASE_DELAY = 1.0
    const val MAX_DELAY = 30.0

    fun delay(attempt: Int): Double = if (attempt <= 1) BASE_DELAY else min(MAX_DELAY, BASE_DELAY * 2.0.pow(attempt - 1))

    fun retries(isOnline: Boolean, isForeground: Boolean) = isOnline && isForeground

    fun isActive(isEnabled: Boolean, isAuthenticated: Boolean, hasSuccessContext: Boolean, registrationCompleted: Boolean) =
        isEnabled && isAuthenticated && !hasSuccessContext && registrationCompleted

    fun resetsAttempts(onServerAck: Boolean) = onServerAck
}

data class SessionState(val preloadFinished: Boolean, val isAuthenticated: Boolean, val hasPendingAuthRoute: Boolean, val registrationCompleted: Boolean)

object SubscriptionActivation {
    fun isActive(state: SessionState) = state.preloadFinished && state.isAuthenticated && !state.hasPendingAuthRoute && state.registrationCompleted

    fun blockingReason(state: SessionState): String? = when {
        !state.preloadFinished -> "preload"
        !state.isAuthenticated -> "auth"
        state.hasPendingAuthRoute -> "auth-route"
        !state.registrationCompleted -> "registration"
        else -> null
    }

    fun transition(previous: Boolean, next: Boolean) = when {
        !previous && next -> "subscribe"
        previous && !next -> "unsubscribe"
        else -> "keep"
    }

    fun documentsToOpen(state: SessionState, catalog: List<String>) = if (isActive(state)) catalog else emptyList()
}

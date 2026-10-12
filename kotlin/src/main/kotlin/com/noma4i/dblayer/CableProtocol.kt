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

    fun requestId(operationId: String, attempt: Int) = "$operationId:$attempt"

    fun execute(channelId: String, operationId: String, attempt: Int, document: String, operationName: String): CableCommand {
        val q = CableCommand.Companion::quoted
        val data = "{\"action\":${q("execute")},\"requestId\":${q(requestId(operationId, attempt))},\"query\":${q(document)},\"variables\":{},\"operationName\":${q(operationName)},\"multiplex\":true}"
        return CableCommand.Message(identifier(channelId), data)
    }

    data class Route(val requestId: String?, val subscriptionId: String?)

    fun route(payload: JsonElement): Route {
        val fields = payload as? JsonObject ?: return Route(null, null)
        fun text(key: String) = (fields[key] as? JsonPrimitive)?.takeIf { it.isString }?.content
        return Route(text("requestId"), text("subscriptionId"))
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

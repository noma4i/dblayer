package com.noma4i.dblayer

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

private val throttledReasons = setOf("rateLimitExceeded", "userRateLimitExceeded", "sharingRateLimitExceeded", "dailyLimitExceeded")
private val authorizationReasons = setOf("insufficientPermissions", "appNotAuthorizedToFile", "authError", "forbidden")

internal fun driveError(status: Int, retryAfter: String?, body: String): CloudException {
    val reason = driveReason(body)
    val detail = "Drive answered $status" + (reason?.let { " ($it)" } ?: "")
    val retryMs = (retryAfter?.trim()?.toLongOrNull() ?: 0L) * 1000
    return when {
        status == 401 -> CloudException.NeedsAuthorization(detail)
        status == 403 && reason in throttledReasons -> CloudException.Throttled(retryMs.toULong())
        status == 403 && reason == "storageQuotaExceeded" -> CloudException.QuotaExceeded(detail)
        status == 403 && reason in authorizationReasons -> CloudException.NeedsAuthorization(detail)
        status == 429 -> CloudException.Throttled(retryMs.toULong())
        status == 408 || status in 500..599 -> CloudException.Unavailable(detail)
        else -> CloudException.Rejected(detail)
    }
}

private fun driveReason(body: String): String? = try {
    val error = Json.parseToJsonElement(body).jsonObject["error"]?.jsonObject
    (error?.get("errors")?.jsonArray?.firstOrNull()?.jsonObject as JsonObject?)?.get("reason")?.jsonPrimitive?.contentOrNull
        ?: error?.get("status")?.jsonPrimitive?.contentOrNull
} catch (error: Exception) {
    null
}

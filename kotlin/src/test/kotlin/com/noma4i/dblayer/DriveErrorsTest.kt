package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Test

private fun describe(error: CloudException): String = when (error) {
    is CloudException.Offline -> "offline"
    is CloudException.Throttled -> "throttled ${error.retryAfterMs}"
    is CloudException.Unavailable -> "unavailable"
    is CloudException.NeedsAuthorization -> "needsAuthorization"
    is CloudException.QuotaExceeded -> "quotaExceeded"
    is CloudException.Rejected -> "rejected"
}

private fun reason(value: String) = """{"error":{"code":403,"errors":[{"reason":"$value"}]}}"""

class DriveErrorsTest {
    @Test
    fun driveRepliesMapToCloudErrors() {
        val table = listOf(
            Triple(401, null as String?, "") to "needsAuthorization",
            Triple(403, null, reason("rateLimitExceeded")) to "throttled 0",
            Triple(403, "7", reason("userRateLimitExceeded")) to "throttled 7000",
            Triple(403, null, reason("storageQuotaExceeded")) to "quotaExceeded",
            Triple(403, null, reason("insufficientPermissions")) to "needsAuthorization",
            Triple(403, null, reason("somethingElse")) to "rejected",
            Triple(429, "3", "") to "throttled 3000",
            Triple(429, null, "not json") to "throttled 0",
            Triple(500, null, "") to "unavailable",
            Triple(503, null, "") to "unavailable",
            Triple(408, null, "") to "unavailable",
            Triple(404, null, "") to "rejected",
            Triple(400, null, "") to "rejected",
        )
        for ((input, expected) in table) {
            assertEquals("status ${input.first}", expected, describe(driveError(input.first, input.second, input.third)))
        }
    }
}

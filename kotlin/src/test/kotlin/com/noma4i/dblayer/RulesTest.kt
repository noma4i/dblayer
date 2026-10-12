package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class RulesTest {
    @Test
    fun retryDelayDoublesAndCaps() {
        assertEquals(1.0, SubscriptionRetry.delay(1), 0.0)
        assertEquals(4.0, SubscriptionRetry.delay(3), 0.0)
        assertEquals(30.0, SubscriptionRetry.delay(20), 0.0)
        assertTrue(SubscriptionRetry.retries(isOnline = true, isForeground = true))
        assertFalse(SubscriptionRetry.retries(isOnline = true, isForeground = false))
    }

    @Test
    fun unauthorizedDecisionWatchesTheProfileRequest() {
        assertTrue(UnauthorizedDecision.logsOut("myProfile", 401, emptyList(), emptyList()))
        assertFalse(UnauthorizedDecision.logsOut("sendMessage", 401, emptyList(), emptyList()))
        assertTrue(UnauthorizedDecision.logsOut("myProfile", null, listOf("UNAUTHENTICATED"), emptyList()))
        assertFalse(UnauthorizedDecision.logsOut("myProfile", null, emptyList(), listOf("Server error")))
        assertTrue(UnauthorizedDecision.reportsFatal(502))
        assertFalse(UnauthorizedDecision.reportsFatal(null))
        assertTrue(UnauthorizedDecision.isSecurityCode("SIGNATURE_INVALID"))
    }

    @Test
    fun serverClockFollowsTheDateHeader() {
        val clock = ServerClock()
        clock.update("Tue, 14 Nov 2023 22:13:20 GMT", receivedAtMillis = 1_700_000_000_000L - 5_000)
        assertEquals(5.0, clock.offsetSeconds, 0.001)
        assertEquals(1_700_000_005_000L, clock.nowMillis(1_700_000_000_000L))
    }

    @Test
    fun identityIsTokenForPrivateAndDeviceForPublicOperations() {
        assertEquals("token", TransportIdentity.resolve("myProfile", "token", "device"))
        assertEquals("device", TransportIdentity.resolve("AuthSignIn", null, "device"))
        assertNull(TransportIdentity.authorizationToken("AuthSignIn", "token"))
        org.junit.Assert.assertThrows(GraphQLTransportError::class.java) { TransportIdentity.resolve("myProfile", null, "device") }
    }
}

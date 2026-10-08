package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class RulesTest {
    @Test
    fun subscriptionLifecycleMovesOnlyForwardFromReady() {
        assertEquals(SubscriptionLifecycle.Ready, SubscriptionRules.next(SubscriptionLifecycle.Requested, SubscriptionLifecycleEvent.ServerAck))
        assertEquals(SubscriptionLifecycle.Receiving, SubscriptionRules.next(SubscriptionLifecycle.Ready, SubscriptionLifecycleEvent.Payload))
        assertEquals(SubscriptionLifecycle.Requested, SubscriptionRules.next(SubscriptionLifecycle.Requested, SubscriptionLifecycleEvent.Payload))
        assertEquals(SubscriptionLifecycle.Failed, SubscriptionRules.next(SubscriptionLifecycle.Receiving, SubscriptionLifecycleEvent.Failure))
        assertTrue(SubscriptionRules.acceptsPayload(SubscriptionLifecycle.Receiving))
    }

    @Test
    fun retryDelayDoublesAndCaps() {
        assertEquals(1.0, SubscriptionRetry.delay(1), 0.0)
        assertEquals(4.0, SubscriptionRetry.delay(3), 0.0)
        assertEquals(30.0, SubscriptionRetry.delay(20), 0.0)
        assertTrue(SubscriptionRetry.retries(isOnline = true, isForeground = true))
        assertFalse(SubscriptionRetry.retries(isOnline = true, isForeground = false))
    }

    @Test
    fun activationNamesTheFirstBlocker() {
        val state = SessionState(preloadFinished = true, isAuthenticated = false, hasPendingAuthRoute = false, registrationCompleted = true)
        assertEquals("auth", SubscriptionActivation.blockingReason(state))
        assertEquals(emptyList<String>(), SubscriptionActivation.documentsToOpen(state, listOf("a")))
        assertEquals(listOf("a"), SubscriptionActivation.documentsToOpen(state.copy(isAuthenticated = true), listOf("a")))
        assertEquals("subscribe", SubscriptionActivation.transition(false, true))
        assertNull(SubscriptionActivation.blockingReason(state.copy(isAuthenticated = true)))
    }

    @Test
    fun presenceSchedulePingsOnlyWithAForegroundConsumer() {
        assertEquals(PresenceStep.StopPing, PresenceSchedule.step(isForeground = false, hasConsumer = true, isReconnecting = false))
        assertEquals(PresenceStep.SkipPing, PresenceSchedule.step(isForeground = true, hasConsumer = false, isReconnecting = false))
        assertEquals(PresenceStep.SendPing, PresenceSchedule.step(isForeground = true, hasConsumer = true, isReconnecting = false))
        assertEquals(0.0, PresenceSchedule.nextPingDelay(null, 10.0), 0.0)
        assertEquals(20.0, PresenceSchedule.nextPingDelay(0.0, 10.0), 0.0)
        assertTrue(PresenceSchedule.leaseExpired(0.0, 91.0))
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

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

private class BridgeCredential : CredentialStore {
    @Volatile var current: String? = null
    override fun get(): String? = current
    override fun set(credential: String) { current = credential }
    override fun clear() { current = null }
}

/** The value bridge through its public API: streams of snapshots and deltas, a command, and the epoch of the session. */
@OptIn(DBlayerTestingApi::class)
class ValueBridgeTest {
    private val account = AtomicReference<String?>(null)
    private val created = AtomicInteger(0)

    private fun json(text: String): JsonObject = Json.parseToJsonElement(text).jsonObject

    private val server = SealedServer { path, body ->
        if (path == "/api/auth/login_link") {
            val name = body.getValue("account").jsonPrimitive.content
            account.set(name)
            return@SealedServer json("""{"sessionId":"$name"}""")
        }
        if (path == "/api/logout") {
            account.set(null)
            return@SealedServer json("""{"revoked":true}""")
        }
        val name = account.get() ?: return@SealedServer json("""{"error":"UNAUTHENTICATED"}""")
        when (body["operationName"]?.jsonPrimitive?.content) {
            "SessionProfile" -> json("""{"data":{"sessionProfile":{"id":"$name"}}}""")
            "UserProfile" -> json("""{"data":{"me":{"__typename":"User","id":"1","name":"$name"}}}""")
            "CreateMoment" -> {
                val number = created.incrementAndGet()
                val text = body["variables"]?.jsonObject?.get("text")?.jsonPrimitive?.content ?: ""
                json("""{"data":{"createMoment":{"__typename":"Moment","id":"m$number","uuid":"u$number","text":"$text","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
            }
            else -> json("""{"errors":[{"message":"unexpected operation"}]}""")
        }
    }

    private suspend fun signIn(name: String) {
        DBlayer.database.sessionCall("login_link", """{"account":"$name"}""")
        DBlayer.call("SessionProfile", JsonObject(emptyMap()))
        User.profile.refresh()
    }

    private suspend fun openSession() {
        closeForCleanup()
        account.set(null)
        val encrypted = EncryptedConfig("https://api.example.com", SealedServer.SERVER_KEY, SealedServer.SERVER_PIN, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("login_link" to EncryptedSessionOperation("/api/auth/login_link", EncryptedOperationClass.AUTHENTICATED))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel")
        val transport = MockTransport()
        transport.cable = EncryptedCablePeer(server)
        DBlayer.openTesting(transport, withRawCalls(modelsJSON), encrypted = encrypted, credentials = BridgeCredential(), http = server)
        DBlayer.database.setOnline(true)
        DBlayer.database.registerOperations("""[{"name":"UserProfile","document":"query UserProfile { me { id name } }"},{"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"},{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid text likedByMe media createdAt } }"}]""")
    }

    private fun text(value: String): JsonObject = buildJsonObject { put("text", value) }

    private fun screens() = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    /** B01: after the login a stream starts with a snapshot of the account, in the epoch of the session. */
    @Test
    fun aStreamAfterTheLoginStartsWithASnapshotOfTheAccountInItsEpoch() = bounded {
        val screen = screens()
        try {
            openSession()
            signIn("A")
            val scope = AccountScope.current()
            val profile = Projected(scope.stream(User.profile), User, screen)
            profile.loaded()
            profile.until("the stream shows A") { it.rows.firstOrNull()?.name == "A" }
            assertTrue(profile.projection.value.revision != null && profile.projection.value.ended == null)
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }

    /** B02: the sign-out ends the stream with one outcome, the projection keeps no new value, and nothing stays open in the core. */
    @Test
    fun theSessionResetEndsEveryStreamWithOneOutcome() = bounded {
        val screen = screens()
        try {
            openSession()
            signIn("A")
            val profile = Projected(AccountScope.current().stream(User.profile), User, screen)
            profile.loaded()
            profile.until("the stream shows A") { it.rows.firstOrNull()?.name == "A" }

            DBlayer.sessionReset()
            profile.until("the stream ended") { it.ended == ReadEnd.SessionEnded }

            assertEquals(0uL, DBlayer.subscriptionCensus().subscriptions)
            assertEquals("A", profile.value?.name)
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }

    /** B03: the account switch gives new streams in a new epoch. B never shows A and A never shows B. */
    @Test
    fun anAccountSwitchGivesNewProjectionsAndNoValueOfAReachesB() = bounded {
        val screen = screens()
        try {
            openSession()
            signIn("A")
            val first = AccountScope.current()
            val projectionA = Projected(first.stream(User.profile), User, screen)
            projectionA.loaded()
            projectionA.until("A shows") { it.rows.firstOrNull()?.name == "A" }

            DBlayer.sessionReset()
            signIn("B")
            val second = AccountScope.current()
            assertNotEquals(first.epoch, second.epoch)
            val projectionB = Projected(second.stream(User.profile), User, screen)
            projectionB.loaded()
            projectionB.until("B shows") { it.rows.firstOrNull()?.name == "B" }

            assertEquals("A", projectionA.value?.name)
            assertEquals(ReadEnd.SessionEnded, projectionA.projection.value.ended)
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }

    /** B04: a message sent offline shows at once, goes out once when the network returns, and keeps its position when the server confirms it. */
    @Test
    fun aMessageSentOfflineGoesOutOnceAndKeepsItsPosition() = bounded {
        val screen = screens()
        try {
            openSession()
            signIn("A")
            val scope = AccountScope.current()
            val thread = Projected(scope.stream(Moment.recent), Moment, screen)
            thread.loaded()
            DBlayer.database.setOnline(false)

            val sent = scope.send(Moment, text("hello"))
            thread.until("the thread shows the queued message") { it.rows.size == 1 }
            assertEquals(sent.optimisticValue?.id, thread.projection.value.rows[0].id)
            assertEquals(SyncState.Pending, thread.projection.value.rows[0].sync.state)
            assertEquals(0, server.operationRequests("CreateMoment"))

            DBlayer.database.setOnline(true)
            assertEquals("m1", sent.confirmed().id)
            thread.until("the thread shows the confirmed message") { it.rows.firstOrNull()?.id == "m1" }

            assertEquals("one message, not a temporary row and a confirmed row", 1, thread.projection.value.rows.size)
            assertEquals(SyncState.Synced, thread.projection.value.rows[0].sync.state)
            assertEquals(1, server.operationRequests("CreateMoment"))
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }

    /** B05: a command and a call of the old epoch are refused or end as interrupted, and the new projection never shows what the old session was about to receive. */
    @Test
    fun aCommandAndALateCallOfTheOldEpochEndWithoutReachingTheNewAccount() = bounded {
        val screen = screens()
        try {
            openSession()
            signIn("A")
            val old = AccountScope.current()
            val stream = old.stream(User.profile)
            val projectionA = Projected(stream, User, screen)
            projectionA.loaded()
            projectionA.until("A shows") { it.rows.firstOrNull()?.name == "A" }

            server.hold("UserProfile")
            val late = async { runCatching { stream.refresh() } }
            eventually("the refresh waits at the server") { server.parkedCount > 0 }
            DBlayer.sessionReset()
            server.hold(null)
            signIn("B")
            val projectionB = Projected(AccountScope.current().stream(User.profile), User, screen)
            projectionB.loaded()
            projectionB.until("B shows") { it.rows.firstOrNull()?.name == "B" }

            val ended = late.await().exceptionOrNull()
            assertTrue("the late call of the old epoch ended as $ended", ended is DbException.Interrupted)
            val refused = runCatching { old.send(Moment, text("late")) }.exceptionOrNull()
            assertTrue("a command of the old epoch ran: $refused", refused is DbException.NoSession)
            assertEquals(0, server.operationRequests("CreateMoment"))
            assertEquals("B", projectionB.value?.name)
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }

    /** B06: the reader that stops ends the stream in the core. */
    @Test
    fun theReaderThatStopsEndsTheStreamInTheCore() = bounded {
        val screen = screens()
        try {
            openSession()
            signIn("A")
            val projected = Projected(AccountScope.current().stream(User.profile), User, screen)
            projected.loaded()
            assertEquals(1uL, DBlayer.subscriptionCensus().observedQueries)

            projected.close()
            projected.reading.join()
            assertEquals(0uL, DBlayer.subscriptionCensus().observedQueries)
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }

    /** boundary_reader_orders, boundary_reader_early_exit (D-175): a stream has one reader, the second collector is invalid, and a collector that takes one value releases the interest while the stream is held. */
    @Test
    fun aSecondReaderIsInvalidAndAReaderThatTakesOneValueFreesTheStream() = bounded {
        val screen = screens()
        try {
            openSession()
            signIn("A")
            val held = AccountScope.current().stream(User.profile)
            val reading = screen.launch { held.values.collect { } }
            eventually("the first reader holds the stream") { DBlayer.subscriptionCensus().observedQueries == 1uL }
            val refused = held.values.first()
            assertTrue("a second reader must end with Invalid: $refused", refused is ReadValue.Ended && refused.cause == ReadEnd.Invalid)
            assertEquals(1uL, DBlayer.subscriptionCensus().observedQueries)
            reading.cancel()
            reading.join()

            val early = AccountScope.current().stream(User.profile)
            assertTrue(early.values.take(1).toList().single() is ReadValue.Snapshot)
            eventually("the reader that left releases the interest while the stream is held") { DBlayer.subscriptionCensus().observedQueries == 0uL }
            early.close()
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }

    /** B07: a change of one row decodes one row, and the subscriptions are the readers. */
    @Test
    fun aChangeOfOneRowDecodesOneRowAndSubscriptionsEqualReaders() = bounded {
        val screen = screens()
        try {
            openSession()
            signIn("A")
            val scope = AccountScope.current()
            val thread = Projected(scope.stream(Moment.recent), Moment, screen)
            val profile = Projected(scope.stream(User.profile), User, screen)
            thread.loaded()
            val first = (1..5).map { number -> scope.send(Moment, text("m$number")) }
            first.forEach { it.outcome() }
            thread.until("the thread holds five confirmed rows") { it.rows.let { rows -> rows.size == 5 && rows.all { it.sync.state == SyncState.Synced } } }
            assertEquals(1uL, DBlayer.subscriptionCensus().subscriptions)

            val extra = scope.send(Moment, text("m6"))
            thread.until("the sixth row shows") { it.rows.size == 6 }
            extra.outcome()
            thread.until("the sixth row is confirmed") { it.rows.lastOrNull()?.sync?.state == SyncState.Synced }

            assertEquals(6, thread.projection.value.rows.size)
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }

    /** B10: the outcome of a command sent offline waits, and ends with the session of its receipt. */
    @Test
    fun theOutcomeOfAnOfflineCommandWaitsAndEndsWithTheEpochOfItsReceipt() = bounded {
        val screen = screens()
        try {
            openSession()
            signIn("A")
            val scope = AccountScope.current()
            DBlayer.database.setOnline(false)
            val sent = scope.send(Moment, text("queued"))
            val waiting = screen.async { sent.outcome() }

            DBlayer.sessionReset()
            assertEquals(OperationOutcome.Interrupted, awaitWithin("the outcome ends with the session") { waiting.await() })
            DBlayer.database.setOnline(true)
            signIn("B")
            assertEquals("a receipt of A answered in the epoch of B", OperationOutcome.Interrupted, sent.outcome())
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }

    /** B12: a stream whose source cannot be connected ends once with the invalid outcome and leaves nothing open. */
    @Test
    fun aStreamThatCannotConnectEndsOnceAndLeavesTheTable() = bounded {
        try {
            openSession()
            signIn("A")
            val reader = DBlayer.database.subscribeValues(DBlayer.database.sessionEpoch(), ReadRequest.Entity("Nope", "x", ReadFetch.NEVER))
            val ended = ArrayList<ReadEnd>()
            val claim = reader.claimReader()
            while (true) {
                val value = reader.pull(claim) ?: break
                if (value is ReadValue.Ended) ended.add(value.cause)
            }
            assertEquals(listOf(ReadEnd.Invalid), ended)
            assertEquals("a stream that ended gives nothing more", null, reader.pull(claim))
            assertEquals(0uL, DBlayer.subscriptionCensus().subscriptions)
        } finally {
            closeForCleanup()
        }
    }

    /** K02: a command and a one-off read that carry the scope of A are refused after the next sign-in and change nothing for B. */
    @Test
    fun aCommandAndAReadOfAnAccountThatLeftAreRefusedAndChangeNothing() = bounded {
        try {
            openSession()
            signIn("A")
            val scopeA = AccountScope.current()
            val row = User.profile.read(scopeA).first()
            DBlayer.sessionReset()
            signIn("B")

            assertTrue(runCatching { row.update(name = "hijacked", account = scopeA) }.exceptionOrNull() is DbException.NoSession)
            assertTrue(runCatching { User.profile.read(scopeA) }.exceptionOrNull() is DbException.NoSession)
            assertTrue(runCatching { User.profile.readRow(row.id, scopeA) }.exceptionOrNull() is DbException.NoSession)
            assertEquals("a command of A changed the row of B", "B", User.profile.read().first().name)
        } finally {
            closeForCleanup()
        }
    }

    /** K08: a row the platform cannot decode is an error of the projection and the next good snapshot clears it. A count stream and an events stream report their source error. */
    @Test
    fun aRowThatDoesNotDecodeThrowsAndAStreamReportsItsSourceEnd() = bounded {
        val screen = screens()
        try {
            openSession()
            signIn("A")
            val scope = AccountScope.current()
            val profile = Projected(scope.stream(User.profile), User, screen)
            profile.loaded()
            profile.until("the stream shows A") { it.rows.firstOrNull()?.name == "A" }
            val state = profile.projection.value.state!!
            val whole = DBlayer.database.readQuery(scope.epoch, User.profile.spec()).first()
            val bad = whole.copy(layout = "another layout")
            val empty = ListProjection<User>()
            assertTrue("a row that does not decode did not throw", runCatching { empty.apply(ReadValue.Snapshot(1L, listOf(bad), emptyList(), state), User) }.isFailure)
            val applied = empty.apply(ReadValue.Snapshot(2L, listOf(whole), emptyList(), state), User)!!
            assertTrue(applied.rows.size == 1 && applied.error == null)

            val first = DBlayer.database.openValueStream(scope.epoch, ReadRequest.Events("Nope")).events(User).first()
            assertTrue("the events stream did not end with the invalid cause: $first", first is ModelEventItem.Ended && first.cause == ReadEnd.Invalid)
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }

    /** B14, R6: a projection made in a scope that was already cancelled does not leave a stream without a reader. */
    @Test
    fun aProjectionInACancelledScopeLeavesNoStreamInTheCore() = bounded {
        try {
            openSession()
            signIn("A")
            val scope = AccountScope.current()
            val cancelled = CoroutineScope(SupervisorJob())
            cancelled.cancel()
            val projected = Projected(scope.stream(User.profile), User, cancelled)
            awaitWithin("the reading job ended") { projected.reading.join() }
            assertEquals(0uL, DBlayer.subscriptionCensus().subscriptions)
        } finally {
            closeForCleanup()
        }
    }

    /** K21 (C02): a scope keeps the database it was made for. After that database closed and another one opened with the same epoch number, a command and a read of the old scope are refused and the new database does not change. */
    @Test
    fun aScopeKeepsItsOwnDatabaseAfterTheDatabaseIsReplaced() = bounded {
        try {
            openSession()
            signIn("A")
            val oldScope = AccountScope.current()
            val row = User.profile.read(oldScope).first()
            closeForCleanup()
            openSession()
            signIn("A")

            assertTrue("a command of the old scope reached the new database", runCatching { row.update(name = "hijacked", account = oldScope) }.isFailure)
            assertTrue("a read of the old scope reached the new database", runCatching { User.profile.read(oldScope) }.isFailure)
            assertEquals("a command of the old scope changed the new database", "A", User.profile.read().first().name)
        } finally {
            closeForCleanup()
        }
    }

    /** K16 (B8): a reader that falls behind reads at most 256 values of a burst from the journal of the core and ends with the whole list from one new snapshot. */
    @Test
    fun aReaderThatFallsBehindKeepsAtMost256DeliveriesAndEndsWithTheWholeList() = bounded {
        try {
            openSession()
            signIn("A")
            val scope = AccountScope.current()
            val stream = scope.stream(Moment.recent)
            var projection = ListProjection<Moment>()
            val received = AtomicInteger(0)
            val rowCount = AtomicInteger(0)
            val settled = kotlinx.coroutines.CompletableDeferred<Unit>()
            val sending = kotlinx.coroutines.CompletableDeferred<Unit>()
            val reading = CoroutineScope(Dispatchers.Default).launch {
                stream.values.collect { value ->
                    if (settled.isCompleted) sending.await() else received.set(0)
                    received.incrementAndGet()
                    projection = projection.apply(value, Moment) ?: projection
                    rowCount.set(projection.rows.size)
                    TestPulse.pulse()
                    if (projection.state?.settled == true) settled.complete(Unit)
                }
            }
            try {
                awaitWithin("the first settled snapshot") { settled.await() }
                received.set(0)
                DBlayer.database.setOnline(false)
                for (number in 1..300) scope.send(Moment, text("m$number"))
                sending.complete(Unit)
                eventually("the reader holds the 300 rows") { rowCount.get() == 300 }
                assertTrue("the reader received ${received.get()} values of a burst of 300: the journal bounded nothing", received.get() <= 260)
            } finally {
                reading.cancel()
                stream.close()
            }
        } finally {
            closeForCleanup()
        }
    }
}

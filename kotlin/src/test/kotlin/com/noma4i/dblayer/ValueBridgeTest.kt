package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
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
        val fixtureOperations = """[{"name":"UserProfile","document":"query UserProfile { me { id name } }"},{"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"},{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid text likedByMe media createdAt } }"}]"""
        server.registerDocuments(fixtureOperations)
        registerStandOperations(DBlayer.database, fixtureOperations)
    }

    private fun text(value: String): JsonObject = buildJsonObject { put("text", value) }

    private fun screens() = CoroutineScope(SupervisorJob() + Dispatchers.Default)

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
            AccountScope.current().stream(User.profile).cancel()
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
            val firstValue = CompletableDeferred<Unit>()
            val reading = screen.launch { held.values.collect { firstValue.complete(Unit) } }
            awaitWithin("the first value of the first reader") { firstValue.await() }
            val refused = held.values.first()
            assertTrue("a second reader must end with Invalid: $refused", refused is ReadValue.Ended && refused.cause is EndCause.Invalid)
            assertEquals(1uL, DBlayer.subscriptionCensus().observedQueries)
            reading.cancel()
            reading.join()

            val early = AccountScope.current().stream(User.profile)
            assertTrue(early.values.take(1).toList().single() is ReadValue.Snapshot)
            held.cancel()
            assertEquals(0uL, DBlayer.subscriptionCensus().observedQueries)
            early.close()
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }

    @Test
    fun bridge_reader_release_parity_c9() = bounded {
        try {
            openSession()
            signIn("A")
            val stream = AccountScope.current().stream(User.profile)
            val first = awaitWithin("the first list value") { stream.values.first() }
            assertTrue("the first reader receives a snapshot: $first", first is ReadValue.Snapshot)
            val next = awaitWithin("the released stream ends closed") { stream.values.first() }
            assertTrue("the released stream ends closed: $next", next is ReadValue.Ended && next.cause is EndCause.Closed)
            val another = awaitWithin("another reader sees the closed stream") { stream.values.first() }
            assertTrue("the next reader sees closed: $another", another is ReadValue.Ended && another.cause is EndCause.Closed)
            stream.close()
        } finally {
            closeForCleanup()
        }
    }

    @Test
    fun bridge_reader_cancel_pending_pull_parity_c9() = bounded {
        try {
            openSession()
            signIn("A")
            val scope = AccountScope.current()
            val stream = scope.stream(Moment.recent)
            val first = CompletableDeferred<ReadValue>()
            val waiting = launch { stream.values.collect { first.complete(it) } }
            assertTrue(awaitWithin("the first snapshot") { first.await() } is ReadValue.Snapshot)
            awaitWithin("the pending collection ends after cancellation") { waiting.cancelAndJoin() }
            val closed = awaitWithin("the cancelled stream ends closed") { stream.values.first() }
            assertTrue("the cancelled stream ends closed: $closed", closed is ReadValue.Ended && closed.cause is EndCause.Closed)
            val reopened = scope.stream(Moment.recent)
            val values = Channel<ReadValue>(Channel.UNLIMITED)
            val next = launch { reopened.values.collect { values.send(it) } }
            val initial = awaitWithin("the new stream receives the current value") { values.receive() }
            assertTrue("the new stream receives a snapshot: $initial", initial is ReadValue.Snapshot)
            var projection = requireNotNull(ListProjection<Moment>().apply(initial, Moment))
            DBlayer.database.setOnline(false)
            val sent = scope.send(Moment, buildJsonObject { put("text", "after cancellation") })
            val id = requireNotNull(sent.optimisticValue).id
            val changed = awaitWithin("the new reader receives the write") { values.receive() }
            projection = requireNotNull(projection.apply(changed, Moment))
            assertTrue("the new reader receives the written row", projection.rows.any { it.id == id })
            next.cancelAndJoin()
            reopened.close()
            stream.close()
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
            assertTrue("a row that does not decode did not throw", runCatching { empty.apply(ReadValue.Snapshot(1L, listOf(bad), emptyList(), state), User) }.exceptionOrNull() is DbException.Invalid)
            val applied = empty.apply(ReadValue.Snapshot(2L, listOf(whole), emptyList(), state), User)!!
            assertTrue(applied.rows.size == 1 && applied.error == null)

            val first = DBlayer.database.openValueStream(scope.epoch, ReadRequest.Events("Nope")).events(User).first()
            assertTrue("the events stream did not end with the invalid cause: $first", first is ModelEventItem.Ended && first.cause is EndCause.Invalid)
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

            assertTrue("a command of the old scope reached the new database", runCatching { row.update(name = "hijacked", account = oldScope) }.exceptionOrNull() is DbException.Closed)
            assertTrue("a read of the old scope reached the new database", runCatching { User.profile.read(oldScope) }.exceptionOrNull() is DbException.Closed)
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

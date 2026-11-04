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
import kotlinx.coroutines.launch
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

/** The value bridge through its public API: streams of snapshots and deltas, a durable command, and the epoch of the session. */
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
        DBlayer.run("SessionProfile", JsonObject(emptyMap()))
        User.profile.refresh()
    }

    private suspend fun openSession() {
        DBlayer.close()
        account.set(null)
        val encrypted = EncryptedConfig("https://api.example.com", SealedServer.SERVER_KEY, SealedServer.SERVER_PIN, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("login_link" to EncryptedSessionOperation("/api/auth/login_link", false))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel")
        DBlayer.openTesting(MockTransport(), modelsJSON, encrypted = encrypted, credentials = BridgeCredential(), http = server)
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
            eventually("the stream shows A") { profile.value?.name == "A" }
            assertEquals(scope.epoch, profile.projection.value.epoch)
            assertTrue(profile.projection.value.revision != null && profile.projection.value.ended == null)
            assertTrue(profile.projection.value.decoded >= 1)
        } finally {
            screen.cancel()
            DBlayer.close()
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
            eventually("the stream shows A") { profile.value?.name == "A" }

            DBlayer.sessionReset()
            eventually("the stream ended") { profile.projection.value.ended == StreamEnd.SESSION_ENDED }

            assertEquals(0uL, streamCensus().open)
            assertEquals(0uL, DBlayer.subscriptionCensus().subscriptions)
            assertEquals("A", profile.value?.name)
        } finally {
            screen.cancel()
            DBlayer.close()
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
            eventually("A shows") { projectionA.value?.name == "A" }

            DBlayer.sessionReset()
            signIn("B")
            val second = AccountScope.current()
            assertNotEquals(first.epoch, second.epoch)
            val projectionB = Projected(second.stream(User.profile), User, screen)
            projectionB.loaded()
            eventually("B shows") { projectionB.value?.name == "B" }

            assertEquals("A", projectionA.value?.name)
            assertEquals(StreamEnd.SESSION_ENDED, projectionA.projection.value.ended)
            assertEquals(second.epoch, projectionB.projection.value.epoch)
        } finally {
            screen.cancel()
            DBlayer.close()
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
            eventually("the thread shows the queued message") { thread.projection.value.rows.size == 1 }
            assertEquals(sent.row.id, thread.projection.value.rows[0].id)
            assertEquals(SyncState.Pending, thread.projection.value.rows[0].sync.state)
            assertEquals(CommandStatus.Queued, sent.status())
            assertEquals(0, server.operationRequests("CreateMoment"))

            DBlayer.database.setOnline(true)
            assertEquals(CommandStatus.Delivered("m1"), sent.outcome())
            eventually("the thread shows the confirmed message") { thread.projection.value.rows.firstOrNull()?.id == "m1" }

            assertEquals("one message, not a temporary row and a confirmed row", 1, thread.projection.value.rows.size)
            assertEquals(SyncState.Synced, thread.projection.value.rows[0].sync.state)
            assertEquals(1, server.operationRequests("CreateMoment"))
        } finally {
            screen.cancel()
            DBlayer.close()
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
            eventually("A shows") { projectionA.value?.name == "A" }

            server.hold("UserProfile")
            val late = async { runCatching { stream.refresh() } }
            eventually("the refresh waits at the server") { server.parkedCount > 0 }
            DBlayer.sessionReset()
            server.hold(null)
            signIn("B")
            val projectionB = Projected(AccountScope.current().stream(User.profile), User, screen)
            projectionB.loaded()
            eventually("B shows") { projectionB.value?.name == "B" }

            val ended = late.await().exceptionOrNull()
            assertTrue("the late call of the old epoch ended as $ended", ended is DbException.Interrupted)
            val refused = runCatching { old.send(Moment, text("late")) }.exceptionOrNull()
            assertTrue("a command of the old epoch ran: $refused", refused is DbException.Interrupted)
            assertEquals(0, server.operationRequests("CreateMoment"))
            assertEquals("B", projectionB.value?.name)
        } finally {
            screen.cancel()
            DBlayer.close()
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
            assertEquals(1uL, streamCensus().open)
            assertEquals(1uL, DBlayer.subscriptionCensus().subscriptions)

            projected.close()
            eventually("the stream left the core") { streamCensus().open == 0uL }

            assertEquals(0uL, DBlayer.subscriptionCensus().subscriptions)
        } finally {
            screen.cancel()
            DBlayer.close()
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
            eventually("the thread holds five confirmed rows") { thread.projection.value.rows.let { rows -> rows.size == 5 && rows.all { it.sync.state == SyncState.Synced } } }
            assertEquals(2uL, streamCensus().open)
            assertEquals(2uL, DBlayer.subscriptionCensus().subscriptions)

            val decoded = thread.projection.value.decoded
            val deltas = thread.projection.value.deltas
            val extra = scope.send(Moment, text("m6"))
            eventually("the sixth row shows") { thread.projection.value.rows.size == 6 }
            extra.outcome()
            eventually("the sixth row is confirmed") { thread.projection.value.rows.lastOrNull()?.sync?.state == SyncState.Synced }

            val decodedNow = thread.projection.value.decoded - decoded
            val deltasNow = thread.projection.value.deltas - deltas
            assertTrue("each delta of one row decodes one row, not the six of the list: $decodedNow rows in $deltasNow deltas", deltasNow >= 1 && decodedNow == deltasNow)
            assertEquals(6, thread.projection.value.rows.size)
            assertTrue(profile.projection.value.decoded >= 1)
        } finally {
            screen.cancel()
            DBlayer.close()
        }
    }

    /** B08, R7: a delivery the reader never received leaves a gap. The reader asks for one new snapshot, drops the deltas that were already queued behind the gap, and continues from the snapshot. */
    @Test
    fun aMissedRevisionGivesOneNewSnapshotAndTheQueuedDeltasWaitForIt() = bounded {
        try {
            openSession()
            signIn("A")
            val scope = AccountScope.current()
            val reader = LossyReader()
            val stream = DBlayer.database.subscribe(scope.epoch, StreamRequest.Query(Moment.recent.json), reader)
            reader.stream = stream
            DBlayer.database.streamLoaded(stream)
            eventually("the first snapshot arrives") { reader.snapshots.get() == 1 }

            DBlayer.database.setOnline(false)
            reader.dropNext()
            for (number in 1..3) scope.send(Moment, text("m$number"))
            eventually("the reader holds the three rows again") { reader.rows() == 3 && reader.snapshots.get() == 2 }

            assertEquals("the queue of later deltas asked for more than one new snapshot", 1, reader.resyncs.get())
            assertEquals("a missed revision gives exactly one new snapshot", 2, reader.snapshots.get())
            DBlayer.database.cancelStream(stream)
        } finally {
            DBlayer.close()
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
            assertEquals(CommandStatus.Queued, sent.status())

            DBlayer.sessionReset()
            assertEquals(CommandStatus.Ended, awaitWithin("the outcome ends with the session") { waiting.await() })
            DBlayer.database.setOnline(true)
            signIn("B")
            assertEquals(CommandStatus.Ended, sent.status())
        } finally {
            screen.cancel()
            DBlayer.close()
        }
    }

    /** B12: a stream whose source cannot be connected ends once with the invalid outcome and leaves nothing open. */
    @Test
    fun aStreamThatCannotConnectEndsOnceAndLeavesTheTable() = bounded {
        try {
            openSession()
            signIn("A")
            val reader = LossyReader()
            val stream = DBlayer.database.subscribe(DBlayer.database.sessionEpoch(), StreamRequest.Entity("Nope", "x", EntityFetch.NONE), reader)
            eventually("the failed stream ended") { reader.ended().isNotEmpty() }

            assertEquals(listOf(StreamEnd.INVALID), reader.ended())
            assertEquals(0uL, streamCensus().open)
            assertTrue(runCatching { DBlayer.database.streamLoaded(stream) }.isFailure)
        } finally {
            DBlayer.close()
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

            assertTrue(runCatching { row.update(name = "hijacked", account = scopeA) }.exceptionOrNull() is DbException.Interrupted)
            assertTrue(runCatching { User.profile.read(scopeA) }.exceptionOrNull() is DbException.Interrupted)
            assertTrue(runCatching { User.profile.readRow(row.id, scopeA) }.exceptionOrNull() is DbException.Interrupted)
            assertEquals("a command of A changed the row of B", "B", User.profile.read().first().name)
        } finally {
            DBlayer.close()
        }
    }

    /** K08: a row the platform cannot decode is an error of the projection and the next good snapshot clears it. A count stream and an events stream report their source error. */
    @Test
    fun aProjectionKeepsTheDecodeErrorAndAStreamKeepsItsSourceError() = bounded {
        val screen = screens()
        try {
            openSession()
            signIn("A")
            val scope = AccountScope.current()
            val profile = Projected(scope.stream(User.profile), User, screen)
            profile.loaded()
            eventually("the stream shows A") { profile.value?.name == "A" }
            val state = profile.projection.value.state!!
            val whole = DBlayer.database.readQuery(scope.epoch, User.profile.json).first()
            val bad = whole.copy(layout = "another layout")
            val broken = ListProjection<User>().apply(StreamDelivery(1uL, scope.epoch, 1uL, StreamValue.Snapshot(1L, listOf(bad), state)), User).projection
            assertTrue("a row that does not decode was dropped without an error", broken.rows.isEmpty() && broken.error != null)
            val healed = broken.apply(StreamDelivery(1uL, scope.epoch, 2uL, StreamValue.Snapshot(2L, listOf(whole), state)), User).projection
            assertTrue("a good snapshot did not clear the error", healed.rows.size == 1 && healed.error == null)

            val first = scope.events(User).events(User).first()
            assertTrue("the events stream did not report its source error: $first", first is ModelEventItem.Failed)
        } finally {
            screen.cancel()
            DBlayer.close()
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
            Projected(scope.stream(User.profile), User, cancelled)
            eventually("the stream left the core") { streamCensus().open == 0uL }
            assertEquals(0uL, DBlayer.subscriptionCensus().subscriptions)
        } finally {
            DBlayer.close()
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
            DBlayer.close()
            openSession()
            signIn("A")

            assertTrue("a command of the old scope reached the new database", runCatching { row.update(name = "hijacked", account = oldScope) }.isFailure)
            assertTrue("a read of the old scope reached the new database", runCatching { User.profile.read(oldScope) }.isFailure)
            assertEquals("a command of the old scope changed the new database", "A", User.profile.read().first().name)
        } finally {
            DBlayer.close()
        }
    }

    /** K15 (iOS findings b and c): the first delivery of a projection is a snapshot with no list of edits, edits come with a delta, and one missed revision gives exactly one new snapshot to each of two streams. */
    @Test
    fun theFirstDeliveryHasNoEditsAndAMissedRevisionGivesEachStreamOneSnapshot() = bounded {
        val screen = screens()
        try {
            openSession()
            signIn("A")
            val scope = AccountScope.current()
            val thread = Projected(scope.stream(Moment.recent), Moment, screen)
            thread.loaded()
            eventually("the first snapshot arrives") { thread.projection.value.revision != null }
            assertTrue("the first delivery carried a list of edits", thread.projection.value.lastEdits == null)
            assertTrue("a screen that asks before the first change must read the rows, not apply nothing", thread.takeEdits() == null)

            val readers = listOf(LossyReader(), LossyReader())
            val streams = readers.map { reader ->
                DBlayer.database.subscribe(scope.epoch, StreamRequest.Query(Moment.recent.json), reader).also { reader.stream = it }
            }
            streams.forEach { DBlayer.database.streamLoaded(it) }
            eventually("both first snapshots arrive") { readers.all { it.snapshots.get() == 1 } }

            DBlayer.database.setOnline(false)
            readers.forEach { it.dropNext() }
            for (number in 1..3) scope.send(Moment, text("m$number"))
            eventually("both readers hold the three rows again") { readers.all { it.rows() == 3 } }

            assertTrue("a missed revision gives exactly one new snapshot per stream", readers.all { it.snapshots.get() == 2 && it.resyncs.get() == 1 })
            streams.forEach { DBlayer.database.cancelStream(it) }
        } finally {
            screen.cancel()
            DBlayer.close()
        }
    }

    /** K16 (B8): a reader that falls behind keeps at most 256 deliveries, learns that it missed some, and ends with the whole list after one new snapshot. */
    @Test
    fun aReaderThatFallsBehindKeepsAtMost256DeliveriesAndEndsWithTheWholeList() = bounded {
        try {
            openSession()
            signIn("A")
            val scope = AccountScope.current()
            val stream = scope.stream(Moment.recent)
            stream.loaded()
            DBlayer.database.setOnline(false)
            for (number in 1..300) scope.send(Moment, text("m$number"))
            var projection = ListProjection<Moment>()
            val received = AtomicInteger(0)
            val missed = java.util.concurrent.atomic.AtomicLong(0)
            val rowCount = AtomicInteger(0)
            val reading = CoroutineScope(Dispatchers.Default).launch {
                stream.deliveries.collect { delivery ->
                    received.incrementAndGet()
                    (delivery.value as? StreamValue.Missed)?.let { missed.addAndGet(it.count.toLong()) }
                    val step = projection.apply(delivery, Moment)
                    projection = step.projection
                    rowCount.set(projection.rows.size)
                    if (step.applied == Applied.Resync) stream.resync()
                }
            }
            try {
                eventually("the reader holds the 300 rows") { rowCount.get() == 300 }
                assertTrue("the reader received ${received.get()} deliveries of a burst of 300: the buffer bounded nothing", received.get() <= 260)
            } finally {
                reading.cancel()
                stream.close()
            }
        } finally {
            DBlayer.close()
        }
    }

    /** K16b: the missed counts plus the deliveries a paused reader receives equal the deliveries the core sent, also when the buffer evicts markers. */
    @Test
    fun missedCountsPlusReceivedDeliveriesEqualTheDeliveriesSentWhenMarkersAreEvicted() {
        val state = StreamState(LivePhase.LOADED, false, false, false, false, true, null, null, null, null)
        val dropped = java.util.concurrent.atomic.AtomicLong()
        val channel = lossyChannel(dropped)
        val sink = Forward(channel, dropped)
        val sent = 700L
        for (seq in 1..sent) sink.delivered(StreamDelivery(1uL, 1uL, seq.toULong(), StreamValue.State(state)))
        var received = 0L
        var missed = 0L
        fun read(delivery: StreamDelivery) { (delivery.value as? StreamValue.Missed)?.let { missed += it.count.toLong() } ?: run { received += 1 } }
        repeat(2) { read(channel.tryReceive().getOrThrow()) }
        sink.delivered(StreamDelivery(1uL, 1uL, (sent + 1).toULong(), StreamValue.Ended(StreamEnd.CLOSED)))
        while (true) read(channel.tryReceive().getOrNull() ?: break)
        assertEquals("the core sent ${sent + 1} deliveries, the reader received $received and was told about $missed missed ones", sent + 1, received + missed)
    }
}

private class LossyReader : ValueSink {
    private var projection = ListProjection<Moment>()
    private var dropping = false
    private val endings = java.util.concurrent.CopyOnWriteArrayList<StreamEnd>()
    val snapshots = AtomicInteger(0)
    val resyncs = AtomicInteger(0)
    @Volatile var stream: ULong = 0uL

    fun rows(): Int = synchronized(this) { projection.rows.size }
    fun ended(): List<StreamEnd> = endings.toList()
    fun dropNext() = synchronized(this) { dropping = true }

    override fun delivered(delivery: StreamDelivery) {
        val ask = synchronized(this) {
            if (dropping) { dropping = false; return@synchronized false }
            if (delivery.value is StreamValue.Snapshot) snapshots.incrementAndGet()
            (delivery.value as? StreamValue.Ended)?.let { endings.add(it.reason) }
            val step = projection.apply(delivery, Moment)
            projection = step.projection
            if (step.applied == Applied.Resync) resyncs.incrementAndGet()
            step.applied == Applied.Resync
        }
        if (ask) DBlayer.database.resyncStream(stream)
    }
}


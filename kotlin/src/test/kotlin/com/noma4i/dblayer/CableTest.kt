package com.noma4i.dblayer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class TestCableSocket(frames: List<String>) : CableSocket {
    private val incoming = Channel<String>(Channel.UNLIMITED).also { channel -> frames.forEach { channel.trySend(it) } }
    private val recorded = java.util.concurrent.CopyOnWriteArrayList<String>()
    private var closed = false
    private val waiters = java.util.concurrent.CopyOnWriteArrayList<Pair<Int, CompletableDeferred<Unit>>>()

    override suspend fun open() {}
    override fun close() { closed = true; incoming.close() }
    override suspend fun send(text: String) {
        if (closed) throw CableSocketException(CableSocketException.Reason.Closed)
        recorded.add(text)
        waiters.filter { it.first <= recorded.size }.forEach { it.second.complete(Unit) }
    }
    override suspend fun receive(): String = incoming.receiveCatching().getOrNull() ?: throw CableSocketException(CableSocketException.Reason.Closed)

    fun texts(): List<String> = recorded.toList()
    suspend fun waitForRecordedCount(count: Int) {
        if (recorded.size >= count) return
        val waiter = CompletableDeferred<Unit>().also { waiters.add(count to it) }
        withTimeout(5000) { waiter.await() }
    }
}

private fun confirmation(identifier: String) = """{"type":"confirm_subscription","identifier":${CableCommand.quoted(identifier)}}"""
private fun rejection(identifier: String) = """{"type":"reject_subscription","identifier":${CableCommand.quoted(identifier)}}"""
private fun message(identifier: String, payload: String) = """{"identifier":${CableCommand.quoted(identifier)},"message":$payload}"""
private const val WELCOME = """{"type":"welcome"}"""

class CableProtocolTest {
    @Test
    fun decodesServerFrames() {
        assertEquals(CableFrame.Welcome, CableFrame.parse(WELCOME))
        assertEquals(CableFrame.Ping(1_700_000_000.0), CableFrame.parse("""{"type":"ping","message":1700000000}"""))
        assertEquals(CableFrame.Confirmed("""{"channel":"GraphqlChannel"}"""), CableFrame.parse("""{"type":"confirm_subscription","identifier":"{\"channel\":\"GraphqlChannel\"}"}"""))
        assertEquals(CableFrame.Rejected("c"), CableFrame.parse("""{"type":"reject_subscription","identifier":"c"}"""))
        assertEquals(CableFrame.Disconnect("unauthorized", false), CableFrame.parse("""{"type":"disconnect","reason":"unauthorized","reconnect":false}"""))
        assertEquals(CableFrame.Disconnect("remote", true), CableFrame.parse("""{"type":"disconnect","reason":"remote"}"""))
        assertEquals(CableFrame.Message("chan", JsonObject(mapOf("result" to JsonObject(mapOf("data" to JsonNull))))), CableFrame.parse("""{"identifier":"chan","message":{"result":{"data":null}}}"""))
        assertNull(CableFrame.parse("""{"type":"unknown"}"""))
        assertNull(CableFrame.parse("not json"))
        assertNull(CableFrame.parse("{}"))
    }

    @Test
    fun encodesClientCommands() {
        assertEquals("""{"command":"subscribe","identifier":"chan"}""", CableCommand.Subscribe("chan").json)
        assertEquals("""{"command":"message","identifier":"chan","data":"{\"action\":\"execute\"}"}""", CableCommand.Message("chan", """{"action":"execute"}""").json)
        assertEquals("""{"channel":"GraphqlChannel","channelId":"abc"}""", CableCommand.identifier("GraphqlChannel", listOf("channelId" to "abc")))
        assertEquals("""{"channel":"OnlineChannel"}""", CableCommand.identifier("OnlineChannel", emptyList()))
    }

    @Test
    fun channelCommandsCarryServerWireFormat() {
        assertEquals("""{"channel":"GraphqlChannel","channelId":"abc","api_version":"2"}""", GraphQLChannel.identifier("abc"))
        assertEquals("abc:2", GraphQLChannel.requestId("abc", 2))
        assertEquals(
            CableCommand.Message(GraphQLChannel.identifier("abc"), """{"action":"execute","requestId":"abc:1","query":"subscription X { x }","variables":{},"operationName":"X"}"""),
            GraphQLChannel.execute("abc", 1, "subscription X { x }", "X"),
        )
    }

    @Test
    fun serverFramesBecomeChannelEvents() {
        fun event(json: String, requestId: String = "abc:1") = GraphQLChannel.event(parseJson(json), requestId)
        assertEquals(GraphQLSubscriptionEvent.Completed, event("""{"result":{"data":null},"more":false}"""))
        assertEquals(GraphQLSubscriptionEvent.Ready, event("""{"result":{"data":null},"more":true,"requestId":"abc:1","subscriptionReady":true}"""))
        assertNull(event("""{"result":{"data":null},"more":true,"requestId":"abc:1","subscriptionReady":true}""", "abc:2"))
        assertEquals(GraphQLSubscriptionEvent.Payload(parseJson("""{"chatUpdated":{"id":"1"}}""")), event("""{"result":{"data":{"chatUpdated":{"id":"1"}}},"more":true}"""))
        assertEquals(GraphQLSubscriptionEvent.Errors(listOf("boom")), event("""{"result":{"errors":[{"message":"boom"}]},"more":true}"""))
        assertNull(event("""{"type":"pong"}"""))
        assertNull(event("""{"result":{"data":{}},"more":true}"""))
    }

    @Test
    fun heartbeatAndReconnectDelays() {
        assertEquals(false, CableHeartbeat.isStale(0.0, 5.9))
        assertEquals(true, CableHeartbeat.isStale(0.0, 6.1))
        assertEquals(true, CableHeartbeat.isStale(null, 0.0))
        assertEquals(6.0, CableHeartbeat.pollInterval(0, 0.0), 0.0)
        assertEquals(6 * 1.15, CableHeartbeat.pollInterval(1, 0.0), 0.0001)
        assertEquals(CableHeartbeat.pollInterval(10, 0.0), CableHeartbeat.pollInterval(25, 0.0), 0.0)
        assertEquals(9.0, CableHeartbeat.pollInterval(0, 0.5), 0.0)
        assertEquals(1.0, CableHeartbeat.jitterBound(0), 0.0)
        assertEquals(0.15, CableHeartbeat.jitterBound(7), 0.0)
        assertEquals(0.0, CableReconnect.delay(1, 1.0), 0.0)
        assertEquals(1.0, CableReconnect.delay(2, 0.0), 0.0001)
        assertEquals(4.0, CableReconnect.delay(4, 0.0), 0.0001)
        assertEquals(10.0, CableReconnect.delay(30, 0.0), 0.0001)
        assertEquals(1.3, CableReconnect.delay(2, 1.0), 0.0001)
        assertTrue(OkHttpCableSocket.isSupported("actioncable-v1-json"))
        assertEquals(false, OkHttpCableSocket.isSupported("actioncable-unsupported"))
        assertEquals(false, OkHttpCableSocket.isSupported(null))
    }
}

class CableClientTest {
    private val identifier = GraphQLChannel.identifier("abc")
    private val document = "subscription X { x }"

    private fun client(socket: TestCableSocket) = CableClient("/test", "token", { _, _ -> socket }, { "abc" })

    private suspend fun events(stream: Flow<GraphQLSubscriptionEvent>, ignoringFailure: Boolean = false): List<GraphQLSubscriptionEvent> =
        withTimeout(5000) { stream.let { if (ignoringFailure) it.catch { } else it }.toList() }

    @Test
    fun sendsExecuteAfterConfirmation() = runBlocking {
        val socket = TestCableSocket(listOf(WELCOME, confirmation(identifier)))
        val client = client(socket)
        val stream = client.subscribeGraphQL(document, "X")
        val start = launch { client.start() }
        socket.waitForRecordedCount(2)
        assertEquals(listOf(CableCommand.Subscribe(identifier).json, GraphQLChannel.execute("abc", 1, document, "X").json), socket.texts())
        client.stop()
        assertTrue(events(stream).isEmpty())
        start.join()
    }

    @Test
    fun emitsReadyOnceThenPayloadEvents() = runBlocking {
        val ready = """{"result":{"data":null},"more":true,"requestId":"abc:1","subscriptionReady":true}"""
        val payload = """{"result":{"data":{"chatUpdated":{"id":"1"}}},"more":true}"""
        val socket = TestCableSocket(listOf(WELCOME, confirmation(identifier), message(identifier, ready), message(identifier, ready), message(identifier, payload), rejection(identifier)))
        val client = client(socket)
        val stream = client.subscribeGraphQL(document, "X")
        val start = launch { client.start() }
        val received = events(stream, ignoringFailure = true)
        client.stop()
        start.join()
        assertEquals(listOf(GraphQLSubscriptionEvent.Ready, GraphQLSubscriptionEvent.Payload(parseJson("""{"chatUpdated":{"id":"1"}}"""))), received)
    }

    @Test
    fun finishesOnReject() = runBlocking {
        val socket = TestCableSocket(listOf(WELCOME, rejection(identifier)))
        val client = client(socket)
        val stream = client.subscribeGraphQL(document, "X")
        val start = launch { client.start() }
        var failure: Throwable? = null
        try { events(stream) } catch (error: Throwable) { failure = error }
        client.stop()
        start.join()
        assertEquals(CableClientException.Reason.Rejected, (failure as CableClientException).reason)
    }

    @Test
    fun finishesAndUnsubscribesOnMoreFalse() = runBlocking {
        val socket = TestCableSocket(listOf(WELCOME, confirmation(identifier), message(identifier, """{"result":{"data":null},"more":false}""")))
        val client = client(socket)
        val stream = client.subscribeGraphQL(document, "X")
        val start = launch { client.start() }
        assertTrue(events(stream).isEmpty())
        socket.waitForRecordedCount(3)
        client.stop()
        start.join()
        assertEquals(CableCommand.Unsubscribe(identifier).json, socket.texts().last())
    }

    @Test
    fun subscribesPresenceAfterWelcomeAndPingsOnConfirmation() = runBlocking {
        val presence = OnlineChannel.identifier("54")
        val socket = TestCableSocket(listOf(WELCOME, confirmation(presence)))
        val client = CableClient("/test", "token", { _, _ -> socket }, { "abc" }, pingInterval = 0.01)
        client.openPresence("54")
        val start = launch { client.start() }
        socket.waitForRecordedCount(4)
        client.stop()
        start.join()
        val texts = socket.texts()
        assertEquals(OnlineChannel.subscribe("54").json, texts.first())
        assertTrue(texts.drop(1).all { it.contains("\\\"action\\\":\\\"ping\\\"") })
    }

    @Test
    fun resubscribesAfterSecondWelcome() = runBlocking {
        val socket = TestCableSocket(listOf(WELCOME, confirmation(identifier), WELCOME, confirmation(identifier)))
        val client = client(socket)
        val stream = client.subscribeGraphQL(document, "X")
        val start = launch { client.start() }
        socket.waitForRecordedCount(4)
        assertEquals(
            listOf(
                CableCommand.Subscribe(identifier).json,
                GraphQLChannel.execute("abc", 1, document, "X").json,
                CableCommand.Subscribe(identifier).json,
                GraphQLChannel.execute("abc", 2, document, "X").json,
            ),
            socket.texts(),
        )
        client.stop()
        assertTrue(events(stream).isEmpty())
        start.join()
    }
}

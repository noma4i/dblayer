package com.noma4i.dblayer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.withTimeout
import okhttp3.OkHttpClient
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import okio.ByteString.Companion.encodeUtf8
import org.junit.Assert.*
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class EncryptedSocketTest {
    private suspend fun Transport.opened(url: String): Pair<PlatformSocket, RecordingEvents> {
        val events = RecordingEvents()
        val outcome = openCable(url, 10_000uL, events)
        return (outcome as CableOutcome.Open).connection to events
    }

    @Test
    fun anAddressThatIsNoWebSocketUrlFailsWithoutAConnection() = bounded {
        val outcome = OkHttpTransport("https://example.test/").openCable("ftp://example.test/api/cable", 10_000uL, RecordingEvents())
        assertEquals(RawFailureKind.IO, (outcome as CableOutcome.Failed).failure.kind)
    }

    @Test
    fun cancellationDuringHandshakeCancelsTheNativeCall() = bounded {
        MockWebServer().use { server ->
            val cancelled = CompletableDeferred<Boolean>()
            val client = OkHttpClient.Builder().addInterceptor { chain ->
                try {
                    chain.proceed(chain.request())
                } finally {
                    cancelled.complete(chain.call().isCanceled())
                }
            }.build()
            server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE))
            val transport = OkHttpTransport(server.url("/").toString(), client = client)
            val pending = async(start = CoroutineStart.UNDISPATCHED) { transport.opened(cableUrl(server)) }
            try {
                assertEquals("/api/cable", server.takeRequest().path)
                pending.cancelAndJoin()
                assertTrue(withTimeout(2000) { cancelled.await() })
            } finally {
                pending.cancelAndJoin()
                client.dispatcher.cancelAll()
                client.connectionPool.evictAll()
                client.dispatcher.executorService.shutdown()
            }
        }
    }

    @Test
    fun exchangesTextOnTheGivenAddressWithoutAuthorizationHeaders() = bounded {
        MockWebServer().use { server ->
            val received = CompletableDeferred<String>()
            server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) {
                    webSocket.send("server challenge")
                }
                override fun onMessage(webSocket: WebSocket, text: String) {
                    received.complete(text)
                }
            }))
            val transport: Transport = OkHttpTransport(server.url("/api/graphql").toString())
            val (connection, events) = transport.opened(cableUrl(server))
            try {
                assertEquals(RecordingEvents.Event.Frame("server challenge"), events.next())
                assertTrue(connection.send("encrypted hello"))
                assertEquals("encrypted hello", received.await())
                val request = server.takeRequest()
                assertEquals("/api/cable", request.path)
                assertNull(request.getHeader("Authorization"))
                assertNull(request.getHeader("Sec-WebSocket-Protocol"))
            } finally {
                connection.close(1000u, "")
            }
        }
    }

    @Test
    fun retainsRemoteCloseCodeAndReason() = bounded {
        MockWebServer().use { server ->
            val idle = CompletableDeferred<Unit>()
            val client = OkHttpClient()
            client.dispatcher.idleCallback = Runnable { idle.complete(Unit) }
            server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) {
                    webSocket.close(4401, "UNAUTHENTICATED")
                }
            }))
            val transport = OkHttpTransport(server.url("/").toString(), client = client)
            val (connection, events) = transport.opened(cableUrl(server))
            assertEquals(RecordingEvents.Event.Closed(4401u, "UNAUTHENTICATED"), events.next())
            withTimeout(2000) { idle.await() }
            connection.close(1000u, "")
        }
    }

    @Test
    fun reportsBinaryAndDisconnectedSocketsAsTheyShowThemselves() = bounded {
        for (binary in listOf(true, false)) {
            MockWebServer().use { server ->
                server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                    override fun onOpen(webSocket: WebSocket, response: Response) {
                        if (binary) webSocket.send("not text".encodeUtf8()) else webSocket.cancel()
                    }
                    override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                        webSocket.close(code, reason)
                    }
                }))
                val transport = OkHttpTransport(server.url("/").toString())
                val (connection, events) = transport.opened(cableUrl(server))
                val event = events.next()
                if (binary) assertTrue(event is RecordingEvents.Event.Binary && event.bytes.decodeToString() == "not text") else assertEquals(RecordingEvents.Event.Failed(RawFailureKind.IO), event)
                connection.close(1000u, "")
            }
        }
    }

    @Test
    fun aRefusedHandshakeReportsItsStatus() = bounded {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setResponseCode(403))
            val outcome = OkHttpTransport(server.url("/").toString()).openCable(cableUrl(server), 10_000uL, RecordingEvents())
            val failure = (outcome as CableOutcome.Failed).failure
            assertEquals(RawFailureKind.PROTOCOL, failure.kind)
            assertEquals(403L, failure.code)
        }
    }

    @Test
    fun mockConnectionsHaveIndependentLifetimes() = bounded {
        val transport = MockTransport()
        val firstEvents = RecordingEvents()
        val secondEvents = RecordingEvents()
        val first = (transport.scriptedCable("ws://localhost/api/cable", firstEvents) as CableOutcome.Open).connection
        val second = (transport.scriptedCable("ws://localhost/api/cable", secondEvents) as CableOutcome.Open).connection
        assertTrue(second.send("hello"))
        assertTrue(first.send("first"))
        assertEquals(2, transport.cableFrames.size)
        val token = transport.cableFrames.first().first
        transport.emitCable(token, "reply")
        transport.closeCable(token, 4401u, "UNAUTHENTICATED")
        assertEquals(RecordingEvents.Event.Frame("reply"), secondEvents.next())
        assertEquals(RecordingEvents.Event.Closed(4401u, "UNAUTHENTICATED"), secondEvents.next())
        try {
            transport.emitCable(token, "late")
            fail("expected a closed socket refusal")
        } catch (_: DbException.Network) {
        }
    }
}

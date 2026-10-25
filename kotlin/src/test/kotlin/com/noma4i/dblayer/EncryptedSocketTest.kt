package com.noma4i.dblayer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okio.ByteString.Companion.encodeUtf8
import org.junit.Assert.*
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class EncryptedSocketTest {
    @Test
    fun exchangesTextOnTheConfiguredPathWithoutLegacyAuthentication() = bounded {
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
                val endpoint = server.url("/api/graphql").toString()
                val transport: Transport = OkHttpTransport(endpoint, cablePath = "/api/cable")
                val token = transport.openedCable()
                try {
                    assertEquals(EncryptedSocketEvent.Text("server challenge"), transport.cableNext(token))
                    transport.cableSend(token, "encrypted hello")
                    assertEquals("encrypted hello", received.await())
                    val request = server.takeRequest()
                    assertEquals("/api/cable", request.path)
                    assertNull(request.getHeader("Authorization"))
                    assertNull(request.getHeader("Sec-WebSocket-Protocol"))
                } finally {
                    transport.cableClose(token)
                }
            }
    }

    @Test
    fun retainsRemoteCloseCodeAndReason() = bounded {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) {
                    webSocket.close(4401, "UNAUTHENTICATED")
                }
            }))
            val transport = OkHttpTransport(server.url("/").toString(), cablePath = "/api/cable")
            val token = transport.openedCable()
            assertEquals(EncryptedSocketEvent.Closed(4401u, "UNAUTHENTICATED"), transport.cableNext(token))
            assertNull(transport.cableNext(token))
            transport.cableClose(token)
        }
    }

    @Test
    fun reportsBinaryAndDisconnectedSocketsAsCloseEvents() = bounded {
        for (binary in listOf(true, false)) {
            MockWebServer().use { server ->
                val closed = CompletableDeferred<Int>()
                server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                    override fun onOpen(webSocket: WebSocket, response: Response) {
                        if (binary) webSocket.send("not text".encodeUtf8()) else webSocket.cancel()
                    }
                    override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                        closed.complete(code)
                        webSocket.close(code, reason)
                    }
                }))
                val transport = OkHttpTransport(server.url("/").toString(), cablePath = "/api/cable")
                val token = transport.openedCable()
                val event = transport.cableNext(token) as EncryptedSocketEvent.Closed
                assertEquals(if (binary) 1003.toUShort() else 1006.toUShort(), event.code)
                assertTrue(event.reason.isNotBlank())
                assertNull(transport.cableNext(token))
                if (binary) assertEquals(1003, closed.await())
                transport.cableClose(token)
            }
        }
    }

    @Test
    fun closesOnlyTheSelectedSocketAndReleasesItsPendingRead() = bounded {
        MockWebServer().use { server ->
            val disconnected = CompletableDeferred<Unit>()
            server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                    disconnected.complete(Unit)
                }
            }))
            server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) {
                    webSocket.send("second")
                }
            }))
            val transport = OkHttpTransport(server.url("/").toString(), cablePath = "/api/cable")
            val first = transport.openedCable()
            val second = transport.openedCable()
            assertNotEquals(first, second)
            val pending = async(start = CoroutineStart.UNDISPATCHED) { transport.cableNext(first) }
            transport.cableClose(first)
            assertNull(pending.await())
            disconnected.await()
            assertEquals(EncryptedSocketEvent.Text("second"), transport.cableNext(second))
            transport.cableClose(second)
        }
    }

    @Test
    fun mockConnectionsHaveIndependentLifetimes() = bounded {
        val transport = MockTransport()
        val first = transport.openedCable()
        val second = transport.openedCable()
        assertNotEquals(first, second)
        transport.cableSend(second, "hello")
        val pending = async(start = CoroutineStart.UNDISPATCHED) { transport.cableNext(first) }
        transport.cableClose(first)
        assertNull(pending.await())
        try {
            transport.cableSend(first, "after close")
            fail("expected closed socket refusal")
        } catch (_: DbException.Network) {
        }
        transport.cableSend(second, "still open")
        assertEquals(listOf(second to "hello", second to "still open"), transport.cableFrames)
        transport.emitCable(second, EncryptedSocketEvent.Text("reply"))
        transport.emitCable(second, EncryptedSocketEvent.Closed(4401u, "UNAUTHENTICATED"))
        assertEquals(EncryptedSocketEvent.Text("reply"), transport.cableNext(second))
        assertEquals(EncryptedSocketEvent.Closed(4401u, "UNAUTHENTICATED"), transport.cableNext(second))
        assertNull(transport.cableNext(second))
        transport.cableClose(second)
        assertNull(transport.cableNext(second))
    }

    @Test
    fun classifiesHandshakeRefusalAndSendingAfterCancellation() = bounded {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setResponseCode(403))
            val transport = OkHttpTransport(server.url("/").toString(), cablePath = "/api/cable")
            try {
                transport.openedCable()
                fail("expected HTTP refusal")
            } catch (error: DbException.Http) {
                assertEquals(403.toUShort(), error.status)
            }
            try {
                transport.cableSend("closed", "hello")
                fail("expected network failure")
            } catch (_: DbException.Network) {
                Unit
            }
        }
    }
}

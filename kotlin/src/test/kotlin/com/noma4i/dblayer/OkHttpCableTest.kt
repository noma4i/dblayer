package com.noma4i.dblayer

import kotlinx.coroutines.CompletableDeferred
import okhttp3.OkHttpClient
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.assertEquals
import org.junit.Test
import okio.ByteString.Companion.toByteString

class OkHttpCableTest {
    @Test
    fun cable_inbound_frame_limit_c9() = bounded {
        for (binary in listOf(false, true)) {
            MockWebServer().use { server ->
                val peer = CompletableDeferred<WebSocket>()
                val frame = CompletableDeferred<ByteArray>()
                val ended = CompletableDeferred<RawFailure?>()
                val events = object : CableEvents {
                    override fun frame(text: String) { frame.complete(text.toByteArray()) }
                    override fun binary(bytes: ByteArray) { frame.complete(bytes) }
                    override fun ended(closeCode: UShort?, reason: String?, failure: RawFailure?) { ended.complete(failure) }
                }
                server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                    override fun onOpen(webSocket: WebSocket, response: Response) { peer.complete(webSocket) }
                }))
                server.start()
                val connection = (OkHttpCable(OkHttpClient()).open(server.url("/cable").toString().replace("http", "ws"), emptyMap(), 10_000uL, events) as CableOutcome.Open).connection
                val socket = awaitWithin("the peer opens") { peer.await() }
                val bytes = ByteArray(cableFrameLimit().toInt()) { 97 }
                if (binary) socket.send(bytes.toByteString()) else socket.send(bytes.toString(Charsets.UTF_8))
                assertTrue(awaitWithin("the allowed frame arrives") { frame.await() }.contentEquals(bytes))
                val large = bytes + byteArrayOf(97)
                if (binary) socket.send(large.toByteString()) else socket.send(large.toString(Charsets.UTF_8))
                val failure = awaitWithin("the oversized frame ends the socket") { ended.await() }
                assertEquals("DBlayer.ResponseLimit", failure?.domain)
                assertEquals(1L, failure?.code)
                connection.cancel()
            }
        }
    }
    private class Ended(val closeCode: UShort?, val failure: RawFailure?)

    private class Events : CableEvents {
        val ended = CompletableDeferred<Ended>()
        override fun frame(text: String) {}
        override fun binary(bytes: ByteArray) {}
        override fun ended(closeCode: UShort?, reason: String?, failure: RawFailure?) {
            ended.complete(Ended(closeCode, failure))
        }
    }

    /** bridge_prop_raw_io (P-BR-08): a send on a socket the peer broke reaches the core as the end of the socket with the native failure. */
    @Test
    fun bridge_prop_raw_io_aSendOnABrokenSocketReachesTheCoreWithTheNativeFailure() = bounded {
        MockWebServer().use { server ->
            val peer = CompletableDeferred<WebSocket>()
            server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) { peer.complete(webSocket) }
            }))
            server.start()
            val events = Events()
            val outcome = OkHttpCable(OkHttpClient()).open(server.url("/cable").toString().replace("http", "ws"), emptyMap(), 10_000uL, events)
            val connection = (outcome as CableOutcome.Open).connection
            awaitWithin("the peer opens") { peer.await() }
            server.shutdown()
            connection.send("client frame")
            val ended = awaitWithin("the end of the socket") { events.ended.await() }
            assertNull(ended.closeCode)
            assertNotNull(ended.failure)
            assertTrue(ended.failure!!.domain.isNotEmpty())
            connection.cancel()
        }
    }
}

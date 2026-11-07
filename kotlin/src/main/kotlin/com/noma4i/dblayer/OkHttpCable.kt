package com.noma4i.dblayer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString

internal class OkHttpCable(private val client: OkHttpClient, private val headers: () -> Map<String, String> = { emptyMap() }) {
    private class Connection : PlatformSocket {
        lateinit var socket: WebSocket

        override fun send(text: String): Boolean = socket.send(text)

        override fun close(code: UShort, reason: String) {
            socket.cancel()
        }
    }

    suspend fun open(url: String, connectMs: ULong, events: CableEvents): CableOutcome {
        val request = try {
            Request.Builder().url(url).apply { headers().forEach { (field, value) -> header(field, value) } }.build().withUserAgent()
        } catch (error: IllegalArgumentException) {
            return CableOutcome.Failed(RawFailure(RawFailureKind.IO, null, error.message ?: "invalid cable address"))
        }
        val connection = Connection()
        val opened = CompletableDeferred<CableOutcome?>()
        connection.socket = client.timed(connectMs, 0u).newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                opened.complete(null)
            }

            override fun onMessage(webSocket: WebSocket, text: String) = events.frame(text)

            override fun onMessage(webSocket: WebSocket, bytes: ByteString) = events.binary(bytes.toByteArray())

            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                events.closed(code.toUShort(), reason)
                webSocket.close(code, reason)
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                val failure = if (response != null && response.code != 101) {
                    RawFailure(RawFailureKind.PROTOCOL, response.code.toLong(), t.message ?: "cable handshake refused")
                } else {
                    rawFailure(t)
                }
                if (!opened.complete(CableOutcome.Failed(failure))) events.failed(failure)
            }
        })
        return try {
            opened.await() ?: CableOutcome.Open(connection)
        } catch (error: CancellationException) {
            connection.socket.cancel()
            throw error
        }
    }
}

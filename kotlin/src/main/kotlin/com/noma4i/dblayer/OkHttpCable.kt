package com.noma4i.dblayer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import okio.utf8Size

internal class OkHttpCable(private val client: OkHttpClient) {
    private class Connection : PlatformSocket {
        lateinit var socket: WebSocket

        override fun send(text: String): Boolean = socket.send(text)

        override fun close(code: UShort, reason: String) {
            socket.close(code.toInt(), reason)
        }

        override fun cancel() {
            socket.cancel()
        }
    }

    private fun tooLarge(webSocket: WebSocket, events: CableEvents) {
        webSocket.cancel()
        events.ended(null, null, rawFailure(ResponseLimitException(cableFrameLimit())))
    }

    suspend fun open(url: String, headers: Map<String, String>, connectMs: ULong, events: CableEvents): CableOutcome {
        val request = try {
            Request.Builder().url(url).apply { headers.forEach { (field, value) -> header(field, value) } }.build()
        } catch (error: IllegalArgumentException) {
            return CableOutcome.Failed(urlFailure(), null)
        }
        val connection = Connection()
        val opened = CompletableDeferred<CableOutcome?>()
        connection.socket = client.timed(connectMs, 0u).newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                opened.complete(null)
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                if (text.utf8Size().toULong() <= cableFrameLimit()) events.frame(text) else tooLarge(webSocket, events)
            }

            override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
                if (bytes.size.toULong() <= cableFrameLimit()) events.binary(bytes.toByteArray()) else tooLarge(webSocket, events)
            }

            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) = events.ended(code.toUShort(), reason, null)

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                val failure = rawFailure(t)
                if (!opened.complete(CableOutcome.Failed(failure, response?.code?.toUShort()))) events.ended(null, null, failure)
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

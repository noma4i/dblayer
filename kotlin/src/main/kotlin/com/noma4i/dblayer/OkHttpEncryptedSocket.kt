package com.noma4i.dblayer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import java.io.IOException
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

internal class OkHttpEncryptedSocket(private val client: OkHttpClient, private val headers: () -> Map<String, String> = { emptyMap() }) {
    private class Connection {
        val opened = CompletableDeferred<Unit>()
        val events = Channel<EncryptedSocketEvent>(Channel.UNLIMITED)
        lateinit var socket: WebSocket
        private val ended = AtomicBoolean(false)

        fun finish(event: EncryptedSocketEvent.Closed? = null) {
            if (!ended.compareAndSet(false, true)) return
            event?.let { events.trySend(it) }
            events.close()
        }
    }

    private val connections = AtomicReference<Map<String, Connection>>(emptyMap())

    suspend fun open(address: String): String {
        val url = try {
            address.replaceFirst("ws", "http").toHttpUrl()
        } catch (error: IllegalArgumentException) {
            throw DbException.Invalid(error.message ?: "invalid cable url")
        }
        val token = UUID.randomUUID().toString()
        val connection = Connection()
        val request = Request.Builder().url(url).apply { headers().forEach { (field, value) -> header(field, value) } }.build().withUserAgent()
        try {
            connection.socket = client.newWebSocket(request, object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) {
                    connection.opened.complete(Unit)
                }

                override fun onMessage(webSocket: WebSocket, text: String) {
                    connection.events.trySend(EncryptedSocketEvent.Text(text))
                }

                override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
                    val reason = "binary cable frame is unsupported"
                    connection.finish(EncryptedSocketEvent.Closed(1003u, reason))
                    webSocket.close(1003, reason)
                }

                override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                    connection.finish(EncryptedSocketEvent.Closed(code.toUShort(), reason))
                    webSocket.close(code, reason)
                }

                override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                    val error = if (response != null && response.code != 101) {
                        DbException.Http(response.code.toUShort(), t.message ?: "cable handshake refused")
                    } else {
                        classifyTransportFailure(t as? Exception ?: IOException(t), GraphQLOperationKind.Query)
                    }
                    connection.opened.completeExceptionally(error)
                    connection.finish(EncryptedSocketEvent.Closed(1006u, error.message ?: "cable connection lost"))
                }
            })
            connections.updateAndGet { it + (token to connection) }
            connection.opened.await()
            return token
        } catch (error: CancellationException) {
            close(token)
            throw error
        } catch (error: Exception) {
            close(token)
            throw if (error is DbException) error else classifyTransportFailure(error, GraphQLOperationKind.Query)
        }
    }

    fun send(token: String, text: String) {
        try {
            val connection = connections.get()[token] ?: throw IOException("cable socket is closed")
            if (!connection.socket.send(text)) throw IOException("cable socket refused the text frame")
        } catch (error: Exception) {
            throw classifyTransportFailure(error, GraphQLOperationKind.Query)
        }
    }

    suspend fun next(token: String): EncryptedSocketEvent? =
        connections.get()[token]?.events?.receiveCatching()?.getOrNull()

    fun close(token: String) {
        val connection = connections.get()[token] ?: return
        connections.updateAndGet { it - token }
        connection.finish()
        connection.socket.cancel()
    }
}

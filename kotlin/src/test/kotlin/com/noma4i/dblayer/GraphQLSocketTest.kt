package com.noma4i.dblayer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeout
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertTrue
import org.junit.Assert.assertEquals
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class GraphQLSocketTest {
    private class Signals : SubscriptionSink(NoHandle) {
        val token = CompletableDeferred<String>()
        val readySignal = CompletableDeferred<Unit>()
        val closedSignal = CompletableDeferred<Unit>()
        val failure = CompletableDeferred<DbException>()
        val events = Channel<String>(Channel.UNLIMITED)
        override fun opened(token: String) { this.token.complete(token) }
        override fun ready() { readySignal.complete(Unit) }
        override fun event(json: String) { events.trySend(json) }
        override fun failed(error: DbException) { failure.complete(error); readySignal.completeExceptionally(error) }
        override fun closed() { closedSignal.complete(Unit) }
    }

    @Test
    fun framesDeliverPayloadsAnswerPingAndComplete() = bounded {
        MockWebServer().use { server ->
            val received = Channel<kotlinx.serialization.json.JsonObject>(Channel.UNLIMITED)
            server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                    webSocket.close(code, reason)
                }
                override fun onMessage(webSocket: WebSocket, text: String) {
                    val message = parseJson(text) as kotlinx.serialization.json.JsonObject
                    received.trySend(message)
                    when (message["type"].toString()) {
                        "\"connection_init\"" -> webSocket.send("""{"type":"connection_ack"}""")
                        "\"subscribe\"" -> {
                            for (frame in listOf("""{"type":"ignored"}""", "[]", """{"type":"next"}""", """{"type":"next","payload":{"data":{"id":"7"}}}""", """{"type":"error","payload":[{"message":"denied"}]}""", """{"type":"ping"}""")) {
                                webSocket.send(frame)
                            }
                        }
                        "\"pong\"" -> webSocket.send("""{"type":"complete"}""")
                    }
                }
            }))
            val client = OkHttpClient()
            val transport = OkHttpTransport(server.url("/graphql").toString(), { mapOf("Authorization" to "Bearer probe") }, client)
            val sink = Signals()
            transport.subscribe(TransportRequest("subscription Probe(\$id: ID!) { node(id: \$id) { id } }", "Probe", """{"id":"7"}""", NetworkPurpose.STANDARD), sink)
            try {
                val initial = received.receive()
                assertEquals(parseJson("""{"Authorization":"Bearer probe"}"""), initial["payload"])
                val subscription = received.receive()
                val payload = subscription["payload"] as kotlinx.serialization.json.JsonObject
                assertEquals(sink.token.await(), (subscription["id"] as kotlinx.serialization.json.JsonPrimitive).content)
                assertEquals("Probe", (payload["operationName"] as kotlinx.serialization.json.JsonPrimitive).content)
                assertEquals(parseJson("""{"id":"7"}"""), payload["variables"])
                sink.readySignal.await()
                assertEquals(parseJson("""{"data":{"id":"7"}}"""), parseJson(sink.events.receive()))
                assertEquals(parseJson("""{"errors":[{"message":"denied"}]}"""), parseJson(sink.events.receive()))
                assertEquals(parseJson("""{"type":"pong"}"""), received.receive())
                withTimeout(2000) { sink.closedSignal.await() }
            } finally {
                transport.unsubscribe(sink.token.await())
                client.dispatcher.cancelAll()
                client.connectionPool.evictAll()
                client.dispatcher.executorService.shutdown()
            }
        }
    }

    @Test
    fun terminalFramesEndTheGraphQLSubscription() = bounded {
      for (terminal in listOf("close", "malformed")) {
        MockWebServer().use { server ->
            val closeSent = CompletableDeferred<Boolean>()
            server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                override fun onMessage(webSocket: WebSocket, text: String) {
                    when ((parseJson(text) as kotlinx.serialization.json.JsonObject)["type"].toString()) {
                        "\"connection_init\"" -> webSocket.send("""{"type":"connection_ack"}""")
                        "\"subscribe\"" -> closeSent.complete(if (terminal == "close") webSocket.close(1000, "finished") else webSocket.send("{"))
                    }
                }
            }))
            val transport = OkHttpTransport(server.url("/graphql").toString())
            val sink = Signals()
            transport.subscribe(TransportRequest("subscription Probe { id }", "Probe", "{}", NetworkPurpose.STANDARD), sink)
            try {
                sink.readySignal.await()
                assertTrue(closeSent.await())
                withTimeout(2000) { sink.closedSignal.await() }
                if (terminal == "malformed") assertTrue(sink.failure.await() is DbException.Network)
            } finally {
                transport.unsubscribe(sink.token.await())
            }
        }
      }
    }
}

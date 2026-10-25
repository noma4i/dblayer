package com.noma4i.dblayer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.withTimeout
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertTrue
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class GraphQLSocketTest {
    private class Signals : SubscriptionSink(NoHandle) {
        val token = CompletableDeferred<String>()
        val readySignal = CompletableDeferred<Unit>()
        val closedSignal = CompletableDeferred<Unit>()
        val failure = CompletableDeferred<DbException>()
        override fun opened(token: String) { this.token.complete(token) }
        override fun ready() { readySignal.complete(Unit) }
        override fun event(json: String) {}
        override fun failed(error: DbException) { failure.complete(error); readySignal.completeExceptionally(error) }
        override fun closed() { closedSignal.complete(Unit) }
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
            transport.subscribe(TransportRequest("subscription Probe { id }", "Probe", "{}"), sink)
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

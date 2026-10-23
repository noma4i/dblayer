package com.noma4i.dblayer

import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Test

class UserAgentTest {
    private class Progress : ByteProgress(NoHandle) {
        override fun sent(bytes: ULong) {}
        override fun isCancelled() = false
    }

    private class Sink : SubscriptionSink(NoHandle) {
        @Volatile var token: String? = null
        override fun ready() {}
        override fun failed(error: DbException) {}
        override fun closed() {}
        override fun opened(token: String) { this.token = token }
    }

    @Test
    fun everyRequestIdentifiesTheLibrary() = bounded {
        val failures = mutableListOf<String>()
        for (path in listOf("encrypted", "graphql", "put", "encryptedSocket", "graphqlSocket")) {
            for (application in listOf(null, "Example/3")) {
                MockWebServer().use { server ->
                    server.enqueue(MockResponse().setResponseCode(400).setBody("busy"))
                    server.start()
                    val origin = server.url("/").toString()
                    val headers = application?.let { mapOf("uSeR-aGeNt" to it) } ?: emptyMap()
                    val client = OkHttpClient()
                    val transport = OkHttpTransport(origin, { headers }, client, cablePath = "/api/cable")
                    val operation = TransportRequest("query Probe { id }", "Probe", "{}")
                    when (path) {
                        "encrypted" -> OkHttpEncryptedHttp(origin, client).post("/api/session", "jwe")
                        "graphql" -> runCatching { transport.query(operation) }
                        "put" -> transport.put(origin, headers, byteArrayOf(1, 2), Progress())
                        "encryptedSocket" -> runCatching { transport.openCable(RecordingLease()) }
                        "graphqlSocket" -> {
                            val sink = Sink()
                            transport.subscribe(operation, sink)
                            eventually("GraphQL handshake received") { server.requestCount == 1 }
                            transport.unsubscribe(requireNotNull(sink.token))
                        }
                        else -> error("unexpected request kind")
                    }
                    val request = server.takeRequest()
                    val supportsHeaders = path in listOf("graphql", "put", "graphqlSocket", "encryptedSocket")
                    val expected = listOf(userAgent(if (supportsHeaders) application else null))
                    if (request.headers.values("User-Agent") != expected) failures.add("$path application=$application expected=$expected actual=${request.headers.values("User-Agent")}")
                    client.connectionPool.evictAll()
                    client.dispatcher.executorService.shutdown()
                }
            }
        }
        assertEquals(emptyList<String>(), failures)
    }
}

package com.noma4i.dblayer

import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class UserAgentTest {

    @Test
    fun life_prop_user_agent() = bounded {
        val failures = mutableListOf<String>()
        for (path in listOf("encrypted", "graphql", "encryptedSocket")) {
            for (application in listOf(null, "", "Reader")) {
                MockWebServer().use { server ->
                    server.enqueue(MockResponse().setResponseCode(400).setBody("busy"))
                    server.start()
                    val origin = server.url("/").toString()
                    val headers = application?.let { mapOf("uSeR-aGeNt" to it) } ?: emptyMap()
                    val client = OkHttpClient()
                    val transport = OkHttpTransport(origin, { headers }, client)
                    when (path) {
                        "encrypted" -> OkHttpEncryptedHttp(client) { headers }.send(probeRequest(origin + "api/session"))
                        "graphql" -> transport.send(probeRequest(origin + "graphql"))
                        "encryptedSocket" -> transport.openCable(origin.replaceFirst("http", "ws") + "api/cable", 10_000uL, RecordingEvents())
                        else -> error("unexpected request kind")
                    }
                    val request = server.takeRequest()
                    val expected = listOf(userAgent(application))
                    if (request.headers.values("User-Agent") != expected) failures.add("$path application=$application expected=$expected actual=${request.headers.values("User-Agent")}")
                    client.connectionPool.evictAll()
                    client.dispatcher.executorService.shutdown()
                }
            }
        }
        assertEquals(emptyList<String>(), failures)
    }

    @Test
    fun life_prop_headers_per_request() = bounded {
        MockWebServer().use { server ->
            repeat(4) { server.enqueue(MockResponse().setResponseCode(400)) }
            server.start()
            val origin = server.url("/").toString()
            val value = java.util.concurrent.atomic.AtomicReference("first")
            val headers = { mapOf("X-Application" to value.get(), "User-Agent" to "Reader") }
            val client = OkHttpClient()
            val http = OkHttpEncryptedHttp(client, headers)
            val transport = OkHttpTransport(origin, headers, client)
            http.send(probeRequest(origin + "api/session"))
            value.set("second")
            http.send(probeRequest(origin + "api/session"))
            transport.openCable(origin.replaceFirst("http", "ws") + "api/cable", 10_000uL, RecordingEvents())
            val file = java.io.File.createTempFile("upload", ".bin").apply { writeText("file"); deleteOnExit() }
            val progress = object : ByteProgress(NoHandle) {
                override fun sent(bytes: ULong) {}
                override fun isCancelled() = false
            }
            putFile(client, server.url("/upload").toString(), mapOf("X-Presigned" to "yes"), PutFile(file.path, 0u, 4u), progress)
            val requests = List(4) { server.takeRequest() }
            assertEquals("first", requests[0].getHeader("X-Application"))
            assertEquals("second", requests[1].getHeader("X-Application"))
            assertEquals("second", requests[2].getHeader("X-Application"))
            assertEquals("yes", requests[3].getHeader("X-Presigned"))
            assertEquals(null, requests[3].getHeader("X-Application"))
            client.connectionPool.evictAll()
            client.dispatcher.executorService.shutdown()
        }
    }
}

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
        val headers = mapOf("User-Agent" to "Core/1 DBlayer/x", "X-Core" to "yes")
        for (path in listOf("encrypted", "graphql", "encryptedSocket", "put")) {
            MockWebServer().use { server ->
                server.enqueue(MockResponse().setResponseCode(400).setBody("busy"))
                server.start()
                val origin = server.url("/").toString()
                val client = OkHttpClient()
                val transport = OkHttpTransport(client = client)
                when (path) {
                    "encrypted" -> OkHttpTransport(client = client).send(probeRequest(origin + "api/session").copy(headers = headers))
                    "graphql" -> transport.send(probeRequest(origin + "graphql").copy(headers = headers))
                    "encryptedSocket" -> transport.openCable(origin.replaceFirst("http", "ws") + "api/cable", headers, 10_000uL, RecordingEvents())
                    else -> {
                        val file = java.io.File.createTempFile("upload", ".bin").apply { writeText("file"); deleteOnExit() }
                        val progress = object : ByteProgress(NoHandle) {
                            override fun sent(bytes: ULong) {}
                            override fun isCancelled() = false
                        }
                        putFile(client, origin + "upload", headers, 10_000uL, 30_000uL, PutFile(file.path, 0u, 4u), progress, 64uL * 1024uL)
                    }
                }
                val request = server.takeRequest()
                if (request.headers.values("User-Agent") != listOf("Core/1 DBlayer/x") || request.getHeader("X-Core") != "yes") failures.add("$path ${request.headers}")
                client.connectionPool.evictAll()
                client.dispatcher.executorService.shutdown()
            }
        }
        assertEquals(emptyList<String>(), failures)
    }

    @Test
    fun life_prop_headers_per_request() {
        val value = java.util.concurrent.atomic.AtomicReference("first")
        val headers = { mapOf("X-Application" to value.get(), "User-Agent" to "Reader") }
        val http = OkHttpTransport(headers = headers)
        val transport = OkHttpTransport(headers)
        assertEquals(listOf("first", "first"), listOf(http.appHeaders()["X-Application"], transport.appHeaders()["X-Application"]))
        value.set("second")
        assertEquals(listOf("second", "second"), listOf(http.appHeaders()["X-Application"], transport.appHeaders()["X-Application"]))
    }
}

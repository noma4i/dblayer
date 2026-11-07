package com.noma4i.dblayer

import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class RedirectTest {
    @Test
    fun life_prop_no_redirect() = bounded {
        for (path in listOf("query", "encrypted", "cable", "put")) {
            MockWebServer().use { server ->
                server.enqueue(MockResponse().setResponseCode(302).setHeader("Location", "/redirect"))
                server.enqueue(MockResponse().setResponseCode(200).setBody("followed"))
                server.start()
                val origin = server.url("/").toString()
                val client = OkHttpClient()
                val transport = OkHttpTransport(origin, client = client)
                when (path) {
                    "query" -> {
                        assertEquals(path, 302.toUShort(), transport.send(probeRequest(origin + "graphql")).status())
                    }
                    "encrypted" -> assertEquals(path, 302.toUShort(), OkHttpEncryptedHttp(client).send(probeRequest(origin + "api/session")).status())
                    "cable" -> {
                        val failure = (transport.openCable(cableUrl(server), 10_000uL, RecordingEvents()) as CableOutcome.Failed).failure
                        assertEquals(path, 302L, failure.code)
                        assertEquals(path, RawFailureKind.PROTOCOL, failure.kind)
                    }
                    else -> {
                        val file = java.io.File.createTempFile("upload", ".bin").apply { writeText("file"); deleteOnExit() }
                        val progress = object : ByteProgress(NoHandle) {
                            override fun sent(bytes: ULong) {}
                            override fun isCancelled() = false
                        }
                        assertEquals(path, 302.toUShort(), transport.put(origin + "upload", emptyMap(), PutFile(file.path, 0u, 4u), TransferIdentity(1L, "a", TransferStep.Put), progress).status())
                    }
                }
                assertEquals(path, 1, server.requestCount)
                client.connectionPool.evictAll()
                client.dispatcher.executorService.shutdown()
            }
        }
    }
}

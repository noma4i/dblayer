package com.noma4i.dblayer

import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Test

class ResponseLimitTest {
    @Test
    fun boundary_put_response_limit_c9() = bounded {
        val file = java.io.File.createTempFile("response-limit", ".bin")
        try {
            file.writeText("file")
            MockWebServer().use { server ->
                server.start()
                val progress = object : ByteProgress(NoHandle) {
                    override fun sent(bytes: ULong) {}
                    override fun isCancelled() = false
                }
                for (length in listOf(16, 17)) for (chunked in listOf(false, true)) {
                    val body = "a".repeat(length)
                    val response = MockResponse().setResponseCode(200)
                    if (chunked) response.setChunkedBody(body, 1) else response.setBody(body)
                    server.enqueue(response)
                    val outcome = putFile(OkHttpClient(), server.url("/part").toString(), emptyMap(), 10_000uL, 30_000uL, PutFile(file.path, 0u, 4u), progress, 16uL)
                    if (length == 16) assertEquals(RawOutcome.Response(200u, null, body), outcome)
                    else {
                        assertEquals("DBlayer.ResponseLimit", outcome.rawFailure()?.domain)
                        assertEquals(1L, outcome.rawFailure()?.code)
                    }
                }
            }
        } finally { file.delete() }
    }
    @Test
    fun boundary_http_response_limit_c9() = bounded {
        MockWebServer().use { server ->
            server.start()
            for (length in listOf(16, 17)) for (chunked in listOf(false, true)) {
                val body = "a".repeat(length)
                val response = MockResponse().setResponseCode(200)
                if (chunked) response.setChunkedBody(body, 1) else response.setBody(body)
                server.enqueue(response)
                val request = HttpRequest(Author.SESSION, 0uL, 1u, server.url("/graphql").toString(), "POST", emptyMap(), "{}", 10_000uL, 30_000uL, NetworkPurpose.STANDARD, 16uL)
                val outcome = OkHttpTransport(client = OkHttpClient()).send(request)
                if (length == 16) assertEquals(RawOutcome.Response(200u, null, body), outcome)
                else {
                    assertEquals("DBlayer.ResponseLimit", outcome.rawFailure()?.domain)
                    assertEquals(1L, outcome.rawFailure()?.code)
                }
            }
        }
    }
}

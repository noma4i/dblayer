package com.noma4i.dblayer

import java.io.File
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Test

class FilePutTest {
    @Test
    fun putSendsTheRangeOfTheFileAndReportsTheBytesAndTheEtag() = bounded {
        val file = File.createTempFile("range", ".bin").apply { writeBytes("0123456789".toByteArray()); deleteOnExit() }
        MockWebServer().use { server ->
            server.enqueue(MockResponse().addHeader("ETag", "\"abc\""))
            server.start()
            val sent = mutableListOf<ULong>()
            val progress = object : ByteProgress(NoHandle) {
                override fun sent(bytes: ULong) { sent.add(bytes) }
                override fun isCancelled() = false
            }
            val reply = putFile(OkHttpClient(), server.url("/part").toString(), mapOf("x-a" to "1"), PutFile(file.path, 3u, 4u), progress)
            val request = server.takeRequest()
            assertEquals("3456", request.body.readUtf8())
            assertEquals("1", request.headers["x-a"])
            assertEquals(RawOutcome.Response(200u, "\"abc\"", ""), reply)
            assertEquals(4uL, sent.last())
        }
    }
}

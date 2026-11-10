package com.noma4i.dblayer

import java.io.File
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Test

class FilePutTest {
    @Test
    fun boundary_put_source_read_fails() = bounded {
        val file = File.createTempFile("short-put", ".bin")
        try {
            file.writeText("part")
            MockWebServer().use { server ->
                server.enqueue(MockResponse())
                server.start()
                val progress = object : ByteProgress(NoHandle) {
                    override fun sent(bytes: ULong) {}
                    override fun isCancelled() = false
                }
                val outcome = putFile(OkHttpClient(), server.url("/short").toString(), emptyMap(), 10_000uL, 30_000uL, PutFile(file.path, 0u, 8u), progress, 1024uL)
                assertEquals(rawFailure(java.io.EOFException("the upload file is shorter than 8 bytes")), outcome.rawFailure())
            }
        } finally { file.delete() }
    }

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
            val reply = putFile(OkHttpClient(), server.url("/part").toString(), mapOf("x-a" to "1"), 10_000uL, 30_000uL, PutFile(file.path, 3u, 4u), progress, 64uL * 1024uL)
            val request = server.takeRequest()
            assertEquals("3456", request.body.readUtf8())
            assertEquals("1", request.headers["x-a"])
            assertEquals(RawOutcome.Response(200u, "\"abc\"", ""), reply)
            assertEquals(4uL, sent.last())
        }
    }

    /** bridge_prop_raw_io (P-BR-08): the body of a PUT answer reaches the core as it is, the empty one too. */
    @Test
    fun putCarriesTheBodyOfTheAnswerToTheCore() = bounded {
        val file = File.createTempFile("body", ".bin").apply { writeBytes("0123".toByteArray()); deleteOnExit() }
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setBody("stored"))
            server.enqueue(MockResponse())
            server.start()
            val progress = object : ByteProgress(NoHandle) {
                override fun sent(bytes: ULong) {}
                override fun isCancelled() = false
            }
            suspend fun put() = putFile(OkHttpClient(), server.url("/part").toString(), emptyMap(), 10_000uL, 30_000uL, PutFile(file.path, 0u, 4u), progress, 64uL * 1024uL)
            assertEquals(RawOutcome.Response(200u, null, "stored"), put())
            assertEquals(RawOutcome.Response(200u, null, ""), put())
        }
    }

    /** bridge.md HTTP of the platform (D-285): a part leaves the file in chunks, the request names its length. */
    @Test
    fun aPartOfTheFileIsSentInChunks() = bounded {
        val megabyte = 1024 * 1024
        val (offset, length) = megabyte.toLong() to 4L * megabyte
        val file = File.createTempFile("part", ".bin").apply { deleteOnExit() }
        file.outputStream().use { output -> repeat(6) { block -> output.write(ByteArray(megabyte) { index -> ((block.toLong() * megabyte + index) % 251).toByte() }) } }
        MockWebServer().use { server ->
            server.enqueue(MockResponse())
            server.start()
            val reports = mutableListOf<Long>()
            val progress = object : ByteProgress(NoHandle) {
                override fun sent(bytes: ULong) { reports.add(bytes.toLong()) }
                override fun isCancelled() = false
            }
            putFile(OkHttpClient(), server.url("/part").toString(), emptyMap(), 10_000uL, 30_000uL, PutFile(file.path, offset.toULong(), length.toULong()), progress, 64uL * 1024uL)
            val request = server.takeRequest()
            assertEquals(length.toString(), request.headers["Content-Length"])
            val body = request.body.readByteArray()
            assertEquals(length.toInt(), body.size)
            assertEquals(null, body.indices.firstOrNull { index -> body[index] != ((offset + index) % 251).toByte() })
            val steps = (listOf(0L) + reports).zipWithNext { before, after -> after - before }
            assertEquals(length, reports.last())
            assertEquals(true, steps.max() <= 64 * 1024)
        }
    }
}

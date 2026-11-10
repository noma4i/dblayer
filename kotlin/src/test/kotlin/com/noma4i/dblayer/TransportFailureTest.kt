package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.IOException
import java.net.ConnectException
import java.net.SocketTimeoutException
import javax.net.ssl.SSLHandshakeException

class TransportFailureTest {
    private val table: List<Throwable> = listOf(
        kotlinx.coroutines.CancellationException("cancelled"),
        SSLHandshakeException("handshake"),
        ConnectException("refused"),
        SocketTimeoutException("timeout"),
        IOException("unexpected end of stream"),
        IllegalStateException("closed"),
    )

    @Test
    fun bridge_prop_classifier_only_theWrapperReportsTheClassNameAndTheTextOfThePlatformError() {
        for (error in table) {
            val raw = rawFailure(error)
            assertEquals(error.toString(), error.javaClass.name, raw.domain)
            assertNull(raw.code)
            assertEquals(error.message, raw.message)
        }
    }

    @Test
    fun theExecutorAppliesTheRequestTimeoutOfTheCore() = bounded {
        okhttp3.mockwebserver.MockWebServer().use { server ->
            server.enqueue(okhttp3.mockwebserver.MockResponse().setSocketPolicy(okhttp3.mockwebserver.SocketPolicy.NO_RESPONSE))
            server.start()
            val request = HttpRequest(Author.SESSION, 0uL, 1u, server.url("/graphql").toString(), "POST", emptyMap(), "{}", 10_000uL, 300uL, NetworkPurpose.STANDARD, 16uL * 1024uL * 1024uL)
            val failure = OkHttpTransport().send(request).rawFailure()
            assertTrue("$failure", failure?.domain in listOf("java.net.SocketTimeoutException", "java.io.InterruptedIOException"))
        }
    }

    @Test
    fun anAddressThatIsNoHttpUrlFailsBeforeTheNetwork() = bounded {
        val request = HttpRequest(Author.SESSION, 0uL, 1u, "ftp://example.test/graphql", "POST", emptyMap(), "{}", 10_000uL, 10_000uL, NetworkPurpose.STANDARD, 16uL * 1024uL * 1024uL)
        assertEquals("url", OkHttpTransport().send(request).rawFailure()?.domain)
        val file = java.io.File.createTempFile("upload", ".bin").apply { writeText("file"); deleteOnExit() }
        val progress = object : ByteProgress(NoHandle) {
            override fun sent(bytes: ULong) {}
            override fun isCancelled() = false
        }
        val put = putFile(okhttp3.OkHttpClient(), "ftp://example.test/upload", emptyMap(), 10_000uL, 10_000uL, PutFile(file.path, 0u, 4u), progress, 64uL * 1024uL)
        assertEquals("url", put.rawFailure()?.domain)
    }

    @Test
    fun bridge_prop_classifier_only_theWrapperNeverBuildsAClassOfItsOwn() {
        val sources = java.io.File("src/main/kotlin/com/noma4i/dblayer").walkTopDown().filter { it.extension == "kt" && it.name != "dblayer.kt" }.toList()
        assertTrue(sources.isNotEmpty())
        for (file in sources) {
            val offenders = file.readLines().filter { "DbException.Network(" in it || "DbException.Interrupted(" in it || "DbException.Http(" in it }
            assertEquals(file.name, emptyList<String>(), offenders)
        }
    }
}

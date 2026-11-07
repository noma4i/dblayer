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
    fun theWrapperReportsTheClassNameAndTheTextOfThePlatformError() {
        for (error in table) {
            val raw = rawFailure(error)
            assertEquals(error.toString(), error.javaClass.name, raw.domain)
            assertNull(raw.code)
            assertEquals(error.message, raw.message)
        }
    }

    @Test
    fun theWrapperNeverBuildsAClassOfItsOwn() {
        val sources = java.io.File("src/main/kotlin/com/noma4i/dblayer").walkTopDown().filter { it.extension == "kt" && it.name != "dblayer.kt" }.toList()
        assertTrue(sources.isNotEmpty())
        for (file in sources) {
            val offenders = file.readLines().filter { "DbException.Network(" in it || "DbException.Interrupted(" in it || "DbException.Http(" in it }
            assertEquals(file.name, emptyList<String>(), offenders)
        }
    }
}

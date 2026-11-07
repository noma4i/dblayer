package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.IOException
import java.net.ConnectException
import java.net.NoRouteToHostException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import javax.net.ssl.SSLHandshakeException
import javax.net.ssl.SSLPeerUnverifiedException

class TransportFailureTest {
    private val table: List<Pair<Throwable, RawFailureKind>> = listOf(
        kotlinx.coroutines.CancellationException("cancelled") to RawFailureKind.CANCELLED,
        SSLHandshakeException("handshake") to RawFailureKind.TLS,
        SSLPeerUnverifiedException("peer") to RawFailureKind.TLS,
        UnknownHostException("host") to RawFailureKind.UNREACHABLE,
        ConnectException("refused") to RawFailureKind.UNREACHABLE,
        NoRouteToHostException("route") to RawFailureKind.UNREACHABLE,
        SocketTimeoutException("timeout") to RawFailureKind.TIMEOUT,
        IOException("unexpected end of stream") to RawFailureKind.IO,
        IllegalStateException("closed") to RawFailureKind.IO,
    )

    @Test
    fun aPlatformErrorMapsToTheKindItShows() {
        for ((error, kind) in table) {
            assertEquals(error.toString(), kind, rawFailure(error).kind)
            assertEquals(error.message, rawFailure(error).message)
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

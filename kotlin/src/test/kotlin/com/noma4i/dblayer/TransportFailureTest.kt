package com.noma4i.dblayer

import java.io.IOException
import java.net.ConnectException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class TransportFailureTest {
    @Test
    fun encryptedHttpTrustFailuresKeepTheirSecurityCode() {
        for (error in listOf(javax.net.ssl.SSLHandshakeException("untrusted"), javax.net.ssl.SSLPeerUnverifiedException("unverified"))) {
            val actual = classifyEncryptedHttpFailure(error)
            assertTrue(actual is DbException.Security)
            actual as DbException.Security
            assertEquals(SecurityCode.TLS_TRUST, actual.code)
            assertEquals(error.message, actual.detail)
        }
    }

    @Test
    fun sessionFailureKeepsItsCodeAndDetail() {
        for (code in listOf(SessionCode.UNAUTHENTICATED, SessionCode.ACCOUNT_UNAVAILABLE, SessionCode.SESSION_ENDED)) {
            val error = DbException.Session(code, "session_unknown")
            for (kind in GraphQLOperationKind.entries) {
                assertEquals(error, classifyTransportFailure(error, kind))
            }
        }
    }

    private fun describe(error: DbException): String = when (error) {
        is DbException.Network -> "Network"
        is DbException.Interrupted -> "Interrupted"
        is DbException.Http -> "Http ${error.status}"
        is DbException.Graphql -> "Graphql ${error.messages.joinToString(",")}"
        else -> error.javaClass.simpleName
    }

    private val cases: List<Triple<String, Pair<Exception, GraphQLOperationKind>, String>> = listOf(
        Triple("mutation graphQL errors", DbException.Graphql(listOf("too large"), null) to GraphQLOperationKind.Mutation, "Graphql too large"),
        Triple("mutation unauthenticated", DbException.Http(401u, "refused") to GraphQLOperationKind.Mutation, "Http 401"),
        Triple("mutation server 503", DbException.Http(503u, "busy") to GraphQLOperationKind.Mutation, "Http 503"),
        Triple("query server 503", DbException.Http(503u, "busy") to GraphQLOperationKind.Query, "Http 503"),
        Triple("mutation unknown host", UnknownHostException("host") to GraphQLOperationKind.Mutation, "Network"),
        Triple("mutation cannot connect", ConnectException("refused") to GraphQLOperationKind.Mutation, "Network"),
        Triple("mutation timeout", SocketTimeoutException("timeout") to GraphQLOperationKind.Mutation, "Interrupted"),
        Triple("query timeout", SocketTimeoutException("timeout") to GraphQLOperationKind.Query, "Network"),
        Triple("mutation connection lost", IOException("unexpected end of stream") to GraphQLOperationKind.Mutation, "Interrupted"),
        Triple("query connection lost", IOException("unexpected end of stream") to GraphQLOperationKind.Query, "Network"),
    )

    @Test
    fun aFailureReachesTheCoreAsTheClassThatTellsIfTheServerAnswered() {
        for ((name, input, expected) in cases) {
            assertEquals(name, expected, describe(classifyTransportFailure(input.first, input.second)))
        }
    }

    @Test
    fun onlyTheClassifierAndTheBlobPutBuildTheRetryableNetworkError() {
        val sources = java.io.File("src/main/kotlin/com/noma4i/dblayer")
        for (file in sources.listFiles { f -> f.extension == "kt" }!!) {
            if (file.name in listOf("dblayer.kt", "TransportFailure.kt", "MockTransport.kt", "Transports.kt")) continue
            val offenders = file.readLines().filter { it.contains("DbException.Network(") }
            assertEquals("${file.name}: $offenders", emptyList<String>(), offenders)
        }
        val blobPut = java.io.File(sources, "Transports.kt").readLines().filter { it.contains("DbException.Network(") }
        assertEquals(blobPut.toString(), 1, blobPut.size)
    }
}

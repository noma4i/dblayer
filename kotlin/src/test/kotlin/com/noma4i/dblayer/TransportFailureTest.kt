package com.noma4i.dblayer

import java.io.IOException
import java.net.ConnectException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import org.junit.Assert.assertEquals
import org.junit.Test

class TransportFailureTest {
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
        Triple("mutation graphQL errors", GraphQLTransportError.GraphQL(listOf("too large"), null) to GraphQLOperationKind.Mutation, "Graphql too large"),
        Triple("mutation unauthenticated", GraphQLTransportError.Unauthenticated to GraphQLOperationKind.Mutation, "Http 401"),
        Triple("mutation server 503", GraphQLTransportError.Server(503) to GraphQLOperationKind.Mutation, "Http 503"),
        Triple("query server 503", GraphQLTransportError.Server(503) to GraphQLOperationKind.Query, "Http 503"),
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
        val file = java.io.File("src/main/kotlin/com/noma4i/dblayer/Transports.kt").readText()
        val offenders = file.lines().filter { it.contains("DbException.Network(") && !it.contains("the subscription socket is not open") }
        assertEquals(listOf("        throw DbException.Network(error.message ?: error.toString())"), offenders)
    }
}

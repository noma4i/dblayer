package com.noma4i.dblayer

import java.net.ConnectException
import java.net.UnknownHostException

internal fun classifyEncryptedHttpFailure(error: Exception): DbException {
    if (error is DbException) return error
    val detail = error.message ?: error.toString()
    return when (error) {
        is kotlinx.coroutines.CancellationException -> DbException.Interrupted(detail)
        is javax.net.ssl.SSLHandshakeException, is javax.net.ssl.SSLPeerUnverifiedException -> DbException.Security(SecurityCode.TLS_TRUST, detail)
        else -> DbException.Network(detail)
    }
}

internal fun classifyTransportFailure(error: Exception, kind: GraphQLOperationKind): DbException {
    if (error is DbException) return error
    val detail = error.message ?: error.toString()
    return when (error) {
        is GraphQLTransportError.Unauthenticated -> DbException.Http(401u, detail)
        is GraphQLTransportError.PayloadTransport -> DbException.Http((if (error.status >= 400) error.status else 500).toUShort(), detail)
        is GraphQLTransportError.Server -> DbException.Http((if (error.status >= 400) error.status else 500).toUShort(), detail)
        is GraphQLTransportError.GraphQL -> DbException.Graphql(error.messages, error.extensions)
        is UnknownHostException, is ConnectException -> DbException.Network(detail)
        else -> if (kind == GraphQLOperationKind.Query) DbException.Network(detail) else DbException.Interrupted(detail)
    }
}

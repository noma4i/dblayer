package com.noma4i.dblayer

import java.net.ConnectException
import java.net.UnknownHostException

internal fun classifyTransportFailure(error: Exception, kind: GraphQLOperationKind): DbException {
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

package com.noma4i.dblayer

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

sealed interface RequestLogItem {
    data object Ready : RequestLogItem
    class Request(val operation: String, val variablesJson: String, val reason: RequestReason) : RequestLogItem
    class Lost(val first: ULong, val last: ULong) : RequestLogItem
    class Ended(val cause: EndCause) : RequestLogItem
}

fun DBlayer.requests(): Flow<RequestLogItem> = flow {
    val database = database
    val stream = ValueStream(database.sessionEpoch(), database.requests())
    stream.values.collect { value ->
        when (value) {
            is ReadValue.Ready -> emit(RequestLogItem.Ready)
            is ReadValue.Request -> emit(RequestLogItem.Request(value.operation, value.variablesJson, value.reason))
            is ReadValue.Lost -> emit(RequestLogItem.Lost(value.first, value.last))
            is ReadValue.Ended -> emit(RequestLogItem.Ended(value.cause))
            else -> {}
        }
    }
}

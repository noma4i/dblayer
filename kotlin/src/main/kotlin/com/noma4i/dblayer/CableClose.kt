package com.noma4i.dblayer

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

sealed interface CableCloseItem {
    data object Ready : CableCloseItem
    class Close(val position: ULong, val event: CableClose) : CableCloseItem
    class Lost(val first: ULong, val last: ULong) : CableCloseItem
    class Ended(val cause: EndCause) : CableCloseItem
}

fun DBlayer.cableCloses(): Flow<CableCloseItem> = flow {
    val database = database
    val stream = ValueStream(database.sessionEpoch(), database.cableCloses())
    stream.values.collect { value ->
        when (value) {
            is ReadValue.Ready -> emit(CableCloseItem.Ready)
            is ReadValue.CableClose -> emit(CableCloseItem.Close(value.position, value.event))
            is ReadValue.Lost -> emit(CableCloseItem.Lost(value.first, value.last))
            is ReadValue.Ended -> emit(CableCloseItem.Ended(value.cause))
            else -> {}
        }
    }
}

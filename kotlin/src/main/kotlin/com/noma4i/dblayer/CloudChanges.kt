package com.noma4i.dblayer

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

internal fun cloudChanges(database: Database): Flow<CloudStatus> = flow {
    val subscription = database.cloudChanges()
    if (subscription == null) {
        emit(database.cloudStatus())
    } else {
        try {
            while (true) emit(subscription.next() ?: break)
        } finally {
            subscription.cancel()
        }
    }
}

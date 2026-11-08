package com.noma4i.dblayer

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/** The snapshots of the encrypted HTTP calls that wait for a retry, pulled one by one from the core. The first is the current snapshot. A slow collector gets the newest one. Cancelling the collector ends only that reader. */
val RetryProgress.flow: Flow<RetrySnapshot>
    get() = flow {
        val subscription = subscribe()
        try {
            while (true) emit(subscription.next() ?: break)
        } finally {
            subscription.cancel()
        }
    }

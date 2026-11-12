package com.noma4i.dblayer

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/** The last network value of the platform, `null` before the first. */
val NetworkStatus.isOnline: Boolean?
    get() = current()

/** The values of the network of the phone as the platform reports them, pulled one by one from the core. The first is the current value, and there is none before the platform gave the first. A slow collector gets the newest one. Cancelling the collector ends only that reader. */
val NetworkStatus.flow: Flow<Boolean>
    get() = flow {
        val subscription = subscribe()
        try {
            while (true) emit(subscription.next() ?: break)
        } finally {
            subscription.cancel()
        }
    }

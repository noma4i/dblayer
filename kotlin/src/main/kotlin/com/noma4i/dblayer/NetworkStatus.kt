package com.noma4i.dblayer

import android.content.Context
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/** The first call starts the network observer of the platform once for the process. */
fun NetworkStatus(context: Context): NetworkStatus = NetworkStatus(StatusObserver(context))

/** The last path of the platform, `null` before the first. */
val NetworkStatus.path: NetworkPath?
    get() = current()

val NetworkStatus.isOnline: Boolean?
    get() = path?.online

/** The paths of the network of the phone as the platform reports them, pulled one by one from the core. The first is the current value, and there is none before the platform gave the first. A slow collector gets the newest one. Cancelling the collector ends only that reader. */
val NetworkStatus.flow: Flow<NetworkPath>
    get() = flow {
        val subscription = subscribe()
        try {
            while (true) emit(subscription.next() ?: break)
        } finally {
            subscription.cancel()
        }
    }

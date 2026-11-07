package com.noma4i.dblayer

import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow

internal class RetrySnapshotListener(private val body: (RetrySnapshot) -> Unit) : RetryProgressListener {
    override fun changed(snapshot: RetrySnapshot) = body(snapshot)
}

/** The encrypted HTTP calls that wait for a retry. The core delivers the current snapshot first, then each newer one. Cancelling the collector ends only that reader. */
val RetryProgress.flow: Flow<RetrySnapshot>
    get() = callbackFlow {
        val subscription = subscribe(RetrySnapshotListener { trySend(it) })
        awaitClose { subscription.cancel() }
    }

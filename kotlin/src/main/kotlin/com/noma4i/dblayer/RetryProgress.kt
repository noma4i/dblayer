package com.noma4i.dblayer

import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.channels.BufferOverflow

internal class RetrySnapshotListener(private val body: (RetrySnapshot) -> Unit) : RetryProgressListener {
    override fun changed(snapshot: RetrySnapshot) = body(snapshot)
}

/** The encrypted HTTP calls that wait for a retry. A collector gets the current snapshot first, then the newest one. Cancelling the collector ends only that reader. */
val RetryProgress.flow: Flow<RetrySnapshot>
    get() = callbackFlow {
        trySend(current())
        val subscription = subscribe(RetrySnapshotListener { trySend(it) })
        awaitClose { subscription.cancel() }
    }.buffer(1, BufferOverflow.DROP_OLDEST).distinctUntilChanged()

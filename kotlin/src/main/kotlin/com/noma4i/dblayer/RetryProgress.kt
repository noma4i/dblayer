package com.noma4i.dblayer

import kotlinx.coroutines.flow.Flow

internal class RetrySnapshotListener(private val body: (RetrySnapshot) -> Unit) : RetryProgressListener {
    override fun changed(snapshot: RetrySnapshot) = body(snapshot)
}

/** The encrypted HTTP calls that wait for a retry. A collector gets the current snapshot first, then the newest one. Cancelling the collector ends only that reader. */
val RetryProgress.flow: Flow<RetrySnapshot>
    get() = LiveSource(current(), null) { emit ->
        val subscription = subscribe(RetrySnapshotListener(emit))
        val cancel: () -> Unit = { subscription.cancel() }
        cancel
    }.flow

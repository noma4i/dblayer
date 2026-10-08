package com.noma4i.dblayer

import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.first
import kotlinx.serialization.KSerializer

private class ChangeListener(private val body: (LiveChange) -> Unit) : QueryListener {
    override fun changed(change: LiveChange) = body(change)
}

internal fun <Model> LiveChange.decode(serializer: KSerializer<Model>): List<Model> =
    rows.mapNotNull { runCatching { dbJson.decodeFromString(serializer, it) }.getOrNull() }

class LiveSource<Value>(
    val initial: Value,
    private val attach: (QueryListener) -> LiveSubscription,
    private val map: (LiveChange) -> Value,
) {
    val flow: Flow<Value>
        get() = callbackFlow {
            val subscription = attach(ChangeListener { trySend(map(it)) })
            awaitClose { subscription.cancel() }
        }

    suspend fun current(): Value = flow.first()
}

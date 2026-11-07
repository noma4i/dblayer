package com.noma4i.dblayer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

object ChangePump {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    suspend fun start(database: Database) {
        val opened = CompletableDeferred<Unit>()
        scope.launch { run(database, opened) }
        opened.await()
    }

    private suspend fun run(database: Database, opened: CompletableDeferred<Unit>) {
        var ended: ULong? = null
        while (scope.isActive) {
            val epoch = database.sessionEpoch()
            if (epoch == ended) {
                TestPulse.next(TestPulse.current)
                continue
            }
            var cause: ReadEnd? = null
            database.openValueStream(epoch, ReadRequest.Changes).values.collect { value ->
                if (value is ReadValue.Ended) cause = value.cause
                opened.complete(Unit)
                TestPulse.pulse()
            }
            opened.complete(Unit)
            ended = epoch
            if (cause == ReadEnd.Closed) return
        }
    }
}

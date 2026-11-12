package com.noma4i.dblayer

import com.noma4i.dblayer.models.CurrentRegion
import com.noma4i.dblayer.models.CurrentUser
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

private class HeldProfileTransport(override val base: MockTransport = MockTransport()) : ScriptedTransport, WrapsMock {
    override suspend fun mutate(request: TransportRequest): String = base.mutate(request)
    override suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply = base.scriptedPut(url, headers, file, identity, progress)
    override suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome = base.scriptedCable(url, events)
    val arrived = CompletableDeferred<Unit>()
    val released = CompletableDeferred<Unit>()
    val ended = CompletableDeferred<Unit>()
    private val first = AtomicBoolean(true)

    override suspend fun query(request: TransportRequest): String {
        if (!first.getAndSet(false)) return base.query(request)
        try {
            arrived.complete(Unit)
            released.await()
            return """{"data":{"me":{"__typename":"User","id":"1","name":"A"}}}"""
        } finally {
            ended.complete(Unit)
        }
    }
}

@OptIn(DBlayerTestingApi::class)
class SessionRestoreTest {
}

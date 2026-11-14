package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import java.nio.file.Files
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Test

private class HeldUploadTransport(val base: MockTransport = MockTransport()) : ScriptedTransport {
    override suspend fun query(request: TransportRequest): String = base.query(request)
    override suspend fun mutate(request: TransportRequest): String = base.mutate(request)
    override suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome = base.scriptedCable(url, events)
    val arrived = CompletableDeferred<Unit>()
    val released = CompletableDeferred<Unit>()
    val finished = CompletableDeferred<Unit>()
    val puts = java.util.concurrent.atomic.AtomicInteger()

    override suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply {
        puts.incrementAndGet()
        try {
            arrived.complete(Unit)
            released.await()
            return PutReply(200u, null)
        } finally {
            finished.complete(Unit)
        }
    }
}

class UploadSessionTest {

    @Test
    fun resetDuringPutEndsTheWaitAndCancelsThePut() = bounded {
        val file = Files.createTempFile("clip", ".mp4").toFile().apply { writeText("clip") }
        val transport = HeldUploadTransport()
        try {
            transport.base.enqueue("CreateBlob", """{"data":{"blobCreate":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}""")
            openSigned(Config(null), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"}]""")
            val sent = AccountScope.current().send(Moment, buildJsonObject { put("text", "hello") }, upload = UploadCommand("upload_clip", UploadRequest(file.path)))
            transport.arrived.await()
            val waiting = async(start = CoroutineStart.UNDISPATCHED) { sent.endedWithSession() }
            DBlayer.sessionReset()
            waiting.await()
            transport.finished.await()
            assertEquals("a second PUT started after the release", 1, transport.puts.get())
        } finally {
            transport.released.complete(Unit)
            closeForCleanup()
            file.delete()
        }
    }
}

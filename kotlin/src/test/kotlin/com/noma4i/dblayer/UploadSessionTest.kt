package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import java.nio.file.Files
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
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
    fun uploadRoundKeepsEachGroupsOriginalEpoch() = bounded {
        val file = Files.createTempFile("clip", ".mp4").toFile().apply { writeText("clip") }
        val transport = MockTransport()
        try {
            transport.enqueue("UserProfile", """{"data":{"me":{"__typename":"User","id":"1","name":"B"}}}""")
            val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"},{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"""
            val database = openSigned(Config(null, null), modelsJSON, transport, operations = operations)
            database.setOnline(false)
            val first = database.sessionEpoch()
            val sentFirst = AccountScope.current().send(Moment, buildJsonObject { put("text", "A") }, upload = UploadCommand("upload_clip", UploadRequest(file.path)))
            DBlayer.sessionReset()
            loginAgain()
            User.profile.load(AccountScope.current())
            database.setOnline(false)
            val second = database.sessionEpoch()
            assertTrue(first != second)
            val sentSecond = AccountScope.current().send(Moment, buildJsonObject { put("text", "B") }, upload = UploadCommand("upload_clip", UploadRequest(file.path)))
            assertEquals("each upload keeps the epoch it was sent in", listOf(first, second), listOf(sentFirst.epoch, sentSecond.epoch))
        } finally {
            closeForCleanup()
            file.delete()
        }
    }

    @Test
    fun resetDuringPutEndsTheWaitAndRemovesTheOldUpload() = bounded {
        val file = Files.createTempFile("clip", ".mp4").toFile().apply { writeText("clip") }
        val transport = HeldUploadTransport()
        try {
            transport.base.enqueue("CreateBlob", """{"data":{"blobCreate":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}""")
            transport.base.enqueue("UserProfile", """{"data":{"me":{"__typename":"User","id":"1","name":"B"}}}""")
            val database = openSigned(Config(null, null), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"},{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]""")
            val sent = AccountScope.current().send(Moment, buildJsonObject { put("text", "hello") }, upload = UploadCommand("upload_clip", UploadRequest(file.path)))
            val row = sent.optimisticValue!!
            transport.arrived.await()
            val waiting = async(start = CoroutineStart.UNDISPATCHED) { sent.outcome() }
            DBlayer.sessionReset()
            assertEquals(OperationOutcome.Ended(EndCause.SessionEnded), waiting.await())
            val refused = runCatching { database.upload(database.sessionEpoch(), "Moment", row.id, "upload_clip", UploadRequest(file.path).wire()) }.exceptionOrNull()
            assertTrue("upload without a session returned $refused", refused is DbException.NoSession)
            loginAgain()
            User.profile.load(AccountScope.current())
            assertEquals("B", User.profile.read(AccountScope.current()).single().name)
            transport.finished.await()
            assertNull(database.readRow(database.sessionEpoch(), "Moment", row.id))
            assertTrue(transport.base.calls.none { it.operationName == "CreateMoment" })
            assertEquals("a second PUT started after the release", 1, transport.puts.get())
        } finally {
            transport.released.complete(Unit)
            closeForCleanup()
            file.delete()
        }
    }
}

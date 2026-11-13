package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import java.nio.file.Files
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

private class RecordingPutTransport(val base: MockTransport = MockTransport()) : ScriptedTransport {
    val bodies = java.util.concurrent.CopyOnWriteArrayList<String>()

    override suspend fun query(request: TransportRequest): String = base.query(request)
    override suspend fun mutate(request: TransportRequest): String = base.mutate(request)
    override suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome = base.scriptedCable(url, events)
    override suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply {
        bodies += java.io.File(file.path).readText()
        return base.scriptedPut(url, headers, file, identity, progress)
    }
}

private const val UPLOAD_OPERATIONS = """[{"name":"CreateBlob","document":"mutation CreateBlob(${'$'}filename: String!, ${'$'}contentType: String!, ${'$'}byteSize: Int!, ${'$'}checksum: String!) { createBlob(filename: ${'$'}filename, contentType: ${'$'}contentType, byteSize: ${'$'}byteSize, checksum: ${'$'}checksum) { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]"""

private fun enqueueUpload(transport: RecordingPutTransport) {
    transport.base.enqueue("CreateBlob", """{"data":{"blobCreate":{"url":"https://s3/x","headers":{},"signedId":"sid","blobId":"b1"}}}""")
    transport.base.enqueuePut("https://s3/x")
    transport.base.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
}

class UploadPrepareStandTest {
    @Test
    fun aSourceIsSentAsIsAndRefusedAboveItsLimit() = bounded {
        val file = Files.createTempFile("clip", ".bin").toFile().apply { writeText("abcdef") }
        val transport = RecordingPutTransport()
        try {
            enqueueUpload(transport)
            openSigned(Config(null), modelsJSON, transport, operations = UPLOAD_OPERATIONS)
            val prepare = Prepare.image(PhotoEdit(null, false, null, null, false, 0u))
            val sent = AccountScope.current().send(Moment, buildJsonObject { put("text", "hello") }, upload = UploadCommand("upload_clip", UploadRequest(file.path, prepare = prepare)))
            assertEquals("m77", sent.confirmed().value?.id)
            assertEquals(listOf("abcdef"), transport.bodies.toList())
            val refused = AccountScope.current().send(Moment, buildJsonObject { put("text", "large") }, upload = UploadCommand("upload_clip", UploadRequest(file.path, maxBytes = 5uL)))
            val id = refused.optimisticValue!!.id
            eventually("the refused size is shown") { Moment.find(id).current()?.uploads?.get("upload_clip")?.error != null }
            assertEquals("too_large", Moment.find(id).current()?.uploads?.get("upload_clip")?.error?.kind)
            assertEquals(listOf("abcdef"), transport.bodies.toList())
        } finally {
            closeForCleanup()
            file.delete()
        }
    }

    @Test
    fun aDeviceThatCannotDecodeShowsTheReasonOfTheFailure() = bounded {
        val file = Files.createTempFile("clip", ".bin").toFile().apply { writeText("abcdef") }
        val transport = RecordingPutTransport()
        try {
            enqueueUpload(transport)
            openSigned(Config(null), modelsJSON, transport, operations = UPLOAD_OPERATIONS, preparer = ScriptedPreparer(PrepareOutcome.CannotDecode("no decoder")))
            val sent = AccountScope.current().send(Moment, buildJsonObject { put("text", "hello") }, upload = UploadCommand("upload_clip", UploadRequest(file.path, prepare = Prepare.video(mirror = true))))
            val id = sent.optimisticValue!!.id
            eventually("the failure of the prepare step is shown") { Moment.find(id).current()?.uploads?.get("upload_clip")?.error != null }
            val failed = Moment.find(id).current()?.uploads?.get("upload_clip")?.error
            assertEquals("prepare_failed", failed?.kind)
            assertEquals("device_cannot_decode", failed?.reason)
            assertTrue(transport.bodies.isEmpty())
        } finally {
            closeForCleanup()
            file.delete()
        }
    }
}

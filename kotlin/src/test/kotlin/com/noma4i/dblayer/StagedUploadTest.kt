package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import java.nio.file.Files
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

private const val STAGED_OPERATIONS = """[{"name":"CreateBlob","document":"mutation CreateBlob(${'$'}filename: String!, ${'$'}contentType: String!, ${'$'}byteSize: Int!, ${'$'}checksum: String!) { createBlob(filename: ${'$'}filename, contentType: ${'$'}contentType, byteSize: ${'$'}byteSize, checksum: ${'$'}checksum) { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]"""

private fun enqueueStagedSteps(transport: MockTransport) {
    transport.enqueue("InitMultipart", """{"data":{"initMultipart":{"multipartUpload":{"key":"k","uploadId":"u"}}}}""")
    transport.enqueue("PresignParts", """{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://s3/p1"}]}}}""")
    transport.enqueuePut("https://s3/p1", "e1")
}

private fun enqueueCommitSteps(transport: MockTransport) {
    transport.enqueue("CompleteMultipart", """{"data":{"completeMultipart":{"signedId":"sid","blobId":"b1"}}}""")
    transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
}

private suspend fun untilHeld(scope: AccountScope, staged: StagedUpload<Moment>): Boolean =
    scope.uploadStatus(staged).uploadStatus.firstOrNull { it is TransferItem.Progress && it.progress.progress >= 0.99 } != null

class StagedUploadTest {
    @Test
    fun aStagedUploadShowsItsStatusBeforeTheCommitAndTheCommitCreatesTheRow() = bounded {
        val file = Files.createTempFile("staged", ".bin").toFile().apply { writeText("abcdef") }
        val transport = MockTransport()
        try {
            enqueueStagedSteps(transport)
            enqueueCommitSteps(transport)
            openSigned(Config(null, null), modelsJSON, transport, operations = STAGED_OPERATIONS)
            val scope = AccountScope.current()
            val staged = scope.stage(Moment, "upload_clip", UploadRequest(file.path))
            assertEquals("upload_clip", staged.action)
            assertTrue(untilHeld(scope, staged))
            assertNull(Moment.find(staged.id).current())
            val receipt = staged.commit(buildJsonObject { put("text", "hello") })
            assertEquals(staged.id, receipt.optimisticValue!!.id)
            assertEquals("hello", receipt.optimisticValue!!.text)
            assertEquals("m77", receipt.confirmed().value?.id)
        } finally {
            closeForCleanup()
            file.delete()
        }
    }

    @Test
    fun aDiscardedStagedUploadEndsItsStatusAndLeavesNoRow() = bounded {
        val file = Files.createTempFile("staged", ".bin").toFile().apply { writeText("abcdef") }
        val transport = MockTransport()
        try {
            enqueueStagedSteps(transport)
            transport.enqueue("AbortMultipart", """{"data":{"abortMultipart":{"ok":true}}}""")
            openSigned(Config(null, null), modelsJSON, transport, operations = STAGED_OPERATIONS)
            val scope = AccountScope.current()
            val staged = scope.stage(Moment, "upload_clip", UploadRequest(file.path))
            assertTrue(untilHeld(scope, staged))
            assertTrue(staged.discard())
            assertTrue(scope.uploadStatus(staged).uploadStatus.firstOrNull { it is TransferItem.Progress && it.progress.state == "gone" } != null)
            assertNull(Moment.find(staged.id).current())
        } finally {
            closeForCleanup()
            file.delete()
        }
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.MomentUploadClipResult
import com.noma4i.dblayer.models.modelsJSON
import java.nio.file.Files
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.mapNotNull
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class UploadRowlessTest {
    @Test
    fun aPartialUploadAnswerMergesOnlyAnExistingRow() = bounded {
        for (existing in listOf(false, true)) {
            val source = Files.createTempFile("intro", ".mp4").toFile().apply { writeText("video") }
            try {
                val transport = MockTransport()
                val definitions = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt successMessage } }"}]"""
                openSigned(Config(null, null), modelsJSON, transport, operations = definitions)
                if (existing) {
                    transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"original","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
                    val seeded = Moment.create(text = "intro", account = AccountScope.current())
                    assertEquals("m77", seeded.confirmed().value?.id)
                }
                transport.enqueue("CreateBlob", """{"data":{"blobCreate":{"url":"https://storage.test/video","headers":{},"signedId":"signed","blobId":"blob"}}}""")
                transport.enqueuePut("https://storage.test/video")
                transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","likedByMe":true}}}""")
                val sent = Moment.uploadClip(AccountScope.current(), id = "m77", source = source.path)
                val confirmed = sent.confirmed()
                val rows = Moment.ids(listOf("m77")).read()
                if (existing) {
                    assertEquals("m77", confirmed.value?.id)
                    assertEquals(true, confirmed.value?.likedByMe)
                    assertEquals("original", confirmed.value?.uuid)
                    assertEquals(1, rows.size)
                    assertEquals(true, rows.single().likedByMe)
                    assertEquals("original", rows.single().uuid)
                } else {
                    assertNull(confirmed.value)
                    assertTrue(rows.isEmpty())
                }
                assertTrue(transport.unscripted.isEmpty())
            } finally {
                closeForCleanup()
                source.delete()
            }
        }
    }

    /** D-287: the call on the type uploads to a row that does not exist: the receipt shows no optimistic value, the queue shows the transfer by its key, and the answer of the final mutation lands the row and reaches `confirmed()`. */
    @Test
    fun uploadToAMissingRowThroughTheCallOnTheType() = bounded {
        val source = Files.createTempFile("intro", ".mp4").toFile().apply { writeText("video") }
        try {
            val transport = MockTransport()
            val row = """"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z""""
            val database = openSigned(Config(null, null), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt successMessage } }"}]""")
            database.setOnline(false)
            val sent = Moment.uploadClip(AccountScope.current(), id = "absent", source = source.path)
            assertNull(sent.optimisticValue)
            val queue = AccountScope.current().operations().mapNotNull { (it as? OperationQueueItem.Queue)?.items }.first()
            val shown = requireNotNull(queue.firstOrNull()?.uploadInfo)
            assertTrue("the queue shows the transfer before it ends: $shown", (shown.status == UploadStatus.Waiting || shown.status == UploadStatus.Queued) && shown.totalBytes == 5uL)
            transport.enqueue("CreateBlob", """{"data":{"blobCreate":{"url":"https://storage.test/video","headers":{},"signedId":"signed","blobId":"blob"}}}""")
            transport.enqueuePut("https://storage.test/video")
            transport.enqueue("CreateMoment", """{"data":{"createMoment":{$row,"successMessage":"Intro received"}}}""")
            database.setOnline(true)
            assertEquals("m77", sent.confirmed().value?.id)
            assertEquals("Intro received", sent.answer<MomentUploadClipResult>().successMessage)
            assertEquals(listOf("CreateBlob", "CreateMoment"), transport.calls.map { it.operationName })
            assertTrue(transport.unscripted.isEmpty())
        } finally {
            closeForCleanup()
            source.delete()
        }
    }
    @Test
    fun aSecondUploadOfTheSameActionAfterConfirmation() = bounded {
        val first = Files.createTempFile("first", ".mp4").toFile().apply { writeText("first") }
        val second = Files.createTempFile("second", ".mp4").toFile().apply { writeText("second") }
        try {
            val transport = MockTransport()
            val row = """"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z""""
            openSigned(Config(null, null), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt successMessage } }"}]""")
            for (name in listOf("first", "second")) {
                transport.enqueue("CreateBlob", """{"data":{"blobCreate":{"url":"https://storage.test/$name","headers":{},"signedId":"$name","blobId":"$name"}}}""")
                transport.enqueuePut("https://storage.test/$name")
                transport.enqueue("CreateMoment", """{"data":{"createMoment":{$row,"successMessage":"$name"}}}""")
            }
            val firstReceipt = Moment.uploadClip(AccountScope.current(), id = "absent", source = first.path)
            assertEquals("m77", firstReceipt.confirmed().value?.id)
            val secondReceipt = Moment.uploadClip(AccountScope.current(), id = "absent", source = second.path)
            assertEquals("m77", secondReceipt.confirmed().value?.id)
            assertEquals("second", secondReceipt.answer<MomentUploadClipResult>().successMessage)
            assertEquals(listOf("CreateBlob", "CreateMoment", "CreateBlob", "CreateMoment"), transport.calls.map { it.operationName })
            assertTrue(transport.unscripted.isEmpty())
        } finally {
            closeForCleanup()
            first.delete()
            second.delete()
        }
    }

}

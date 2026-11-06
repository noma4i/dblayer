package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.MomentUploadClipResult
import com.noma4i.dblayer.models.modelsJSON
import java.nio.file.Files
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class UploadAnswerTest {
    @Test
    fun inv_upload_14_a_waiting_upload_reports_the_copied_file_size() = bounded {
        val source = Files.createTempFile("intro", ".mp4").toFile().apply { writeText("video") }
        try {
            val transport = MockTransport()
            val database = reopen(Config(null, null, 32u), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"}]""")
            database.setOnline(false)
            val local = Moment.send(AccountScope.current(), text = "intro").optimisticValue!!
            local.uploadClip(AccountScope.current(), UploadRequest(source.path))
            val current = requireNotNull(Moment.find(local.id).current())
            val waiting = requireNotNull(current.uploads["upload_clip"])
            assertEquals(UploadStatus.Waiting, waiting.status)
            assertEquals(5uL, waiting.totalBytes)
            assertEquals(0uL, waiting.sentBytes)
            assertEquals(0.0, waiting.percent, 0.0)
            assertTrue(transport.calls.isEmpty())
        } finally {
            closeForCleanup()
            source.delete()
        }
    }

    @Test
    fun inv_upload_21_an_upload_answers_with_the_server_text_and_null_when_the_server_left_it_out() = bounded {
        val source = Files.createTempFile("intro", ".mp4").toFile().apply { writeText("video") }
        try {
            val transport = MockTransport()
            val row = """"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z""""
            transport.enqueue("CreateMoment", """{"data":{"createMoment":{$row}}}""")
            reopen(Config(null, null, 32u), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt successMessage } }"}]""")
            val moment = requireNotNull(Moment.find(Moment.send(AccountScope.current(), text = "intro").confirmed().id).current())
            for (expected in listOf("Intro received", null)) {
                transport.enqueue("CreateBlob", """{"data":{"createBlob":{"url":"https://storage.test/video","headers":{},"signedId":"signed","blobId":"blob"}}}""")
                val field = expected?.let { ",\"successMessage\":\"$it\"" } ?: ""
                transport.enqueue("CreateMoment", """{"data":{"createMoment":{$row$field}}}""")
                val uploaded = moment.uploadClip(AccountScope.current(), UploadRequest(source.path))
                assertEquals("m77", uploaded.optimisticValue?.id)
                assertEquals(expected, uploaded.answer<MomentUploadClipResult>().successMessage)
            }
            assertEquals(listOf("CreateMoment", "CreateBlob", "CreateMoment", "CreateBlob", "CreateMoment"), transport.calls.map { it.operationName })
            assertTrue(transport.unscripted.isEmpty())
        } finally {
            closeForCleanup()
            source.delete()
        }
    }
}

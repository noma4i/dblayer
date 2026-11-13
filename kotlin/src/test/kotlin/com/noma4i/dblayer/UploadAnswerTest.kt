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
    fun inv_upload_21_an_upload_answers_with_the_server_text_and_null_when_the_server_left_it_out() = bounded {
        val source = Files.createTempFile("intro", ".mp4").toFile().apply { writeText("video") }
        try {
            val transport = MockTransport()
            val row = """"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z""""
            transport.enqueue("CreateMoment", """{"data":{"createMoment":{$row}}}""")
            openSigned(Config(null), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt successMessage } }"}]""")
            val moment = requireNotNull(Moment.find(requireNotNull(Moment.create(text = "intro", account = AccountScope.current()).confirmed().value).id).current())
            for ((turn, expected) in listOf("Intro received", null).withIndex()) {
                if (turn == 0) transport.enqueue("CreateBlob", """{"data":{"blobCreate":{"url":"https://storage.test/video","headers":{},"signedId":"signed","blobId":"blob"}}}""")
                if (turn == 0) transport.enqueuePut("https://storage.test/video")
                val field = expected?.let { ",\"successMessage\":\"$it\"" } ?: ""
                transport.enqueue("CreateMoment", """{"data":{"createMoment":{$row$field}}}""")
                val uploaded = moment.uploadClip(AccountScope.current(), UploadRequest(source.path))
                assertEquals("m77", uploaded.optimisticValue?.id)
                assertEquals(expected, uploaded.answer<MomentUploadClipResult>().successMessage)
            }
            assertEquals(listOf("CreateMoment", "CreateBlob", "CreateMoment", "CreateMoment"), transport.calls.map { it.operationName })
            assertTrue(transport.unscripted.isEmpty())
        } finally {
            closeForCleanup()
            source.delete()
        }
    }
}

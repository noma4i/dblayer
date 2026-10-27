package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import java.nio.file.Files
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class UploadAnswerTest {
    @Test
    fun anUploadAnswersWithTheServerTextAndNullWhenTheServerLeftItOut() = bounded {
        val source = Files.createTempFile("intro", ".mp4").toFile().apply { writeText("video") }
        try {
            val transport = MockTransport()
            val row = """"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z""""
            transport.enqueue("CreateMoment", """{"data":{"createMoment":{$row}}}""")
            reopen(Config(null, null, 32u), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt successMessage } }"}]""", uploads = UploadOperations(createBlob = UploadOperation("CreateBlob")))
            val local = Moment.create(text = "intro")
            local.settled(null)
            val moment = requireNotNull(Moment.find(local.confirmedId()).current())
            for (expected in listOf("Intro received", null)) {
                transport.enqueue("CreateBlob", """{"data":{"createBlob":{"url":"https://storage.test/video","headers":{},"signedId":"signed","blobId":"blob"}}}""")
                val field = expected?.let { ",\"successMessage\":\"$it\"" } ?: ""
                transport.enqueue("CreateMoment", """{"data":{"createMoment":{$row$field}}}""")
                val uploaded = moment.uploadClip(UploadRequest(source.path))
                assertEquals("m77", uploaded.model.id)
                assertEquals(expected, uploaded.answer().successMessage)
            }
            assertEquals(listOf("CreateMoment", "CreateBlob", "CreateMoment", "CreateBlob", "CreateMoment"), transport.calls.map { it.operationName })
            assertTrue(transport.unscripted.isEmpty())
        } finally {
            DBlayer.close()
            source.delete()
        }
    }
}

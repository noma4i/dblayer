package com.noma4i.dblayer

import com.noma4i.dblayer.models.*
import java.io.File
import kotlinx.serialization.json.*
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class BlobTest {
    @Test
    fun blobPlatforms() = bounded {
        val current = File(requireNotNull(System.getProperty("user.dir")))
        val root = if (current.name == "kotlin") current.parentFile else current
        val document = File(root, "fixtures/operations/queries/blob.graphql").readText()
        val transport = MockTransport()
        reopen(Config(null, null, 64u), File(root, "fixtures/blob-upload.json").readText(), transport)
        DBlayer.database.registerOperations(JsonArray(listOf("BlobCreate", "BlobProfileCurrent", "BlobSetVideo").map { name -> buildJsonObject {
            put("name", JsonPrimitive(name)); put("document", JsonPrimitive(document))
            put("rootPath", JsonPrimitive(when (name) { "BlobCreate" -> "blobCreate"; "BlobProfileCurrent" -> "blobProfile"; else -> "setVideo" }))
        } }).toString())
        transport.enqueue("BlobProfileCurrent", """{"data":{"blobProfile":{"id":"p1","label":"before"}}}""")
        transport.enqueue("BlobCreate", """{"data":{"blobCreate":{"url":"https://s3/blob","headers":{},"signedId":"signed-video","blobId":"b1"}}}""")
        transport.enqueue("BlobSetVideo", """{"data":{"setVideo":{"profile":{"id":"p1","label":"after"}}}}""")
        BlobProfile.current.load()
        val blobScope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob())
        val source = File.createTempFile("blob", ".mp4")
        source.writeText("hello")
        try {
            val blob = DBlayer.upload(BlobUpload.VideoIntro, UploadRequest(source.path), blobScope)
            val restored = DBlayer.blob(blob.id, blobScope)
            assertEquals(blob.id, restored.id)
            restored.close()
            assertEquals("signed-video", blob.waitUntilUploaded())
            eventually("uploaded Blob snapshot") { blob.snapshot.value.state == BlobState.UPLOADED }
            assertEquals("signed-video", blob.snapshot.value.signedId)
            assertEquals(1.0, blob.snapshot.value.progress, 0.0)
            assertTrue(transport.calls.none { it.operationName == "BlobSetVideo" })
            BlobProfile.setVideo(blob)
            eventually("Blob consumer sent") { transport.calls.count { it.operationName == "BlobSetVideo" } == 1 }
            val call = transport.calls.single { it.operationName == "BlobSetVideo" }
            assertEquals(Json.parseToJsonElement("""{"blobProfileId":"p1","signedId":"signed-video"}"""), Json.parseToJsonElement(call.variablesJson))
            eventually("consumed Blob snapshot") { blob.snapshot.value.state == BlobState.CONSUMED || blob.snapshot.value.error != null }
            assertEquals(BlobState.CONSUMED, blob.snapshot.value.state)
            assertEquals(null, blob.snapshot.value.error)
            blobScope.cancel()
            eventually("the status stream ended with the scope of its reader") { streamCensus().open == 0uL }
        } finally { blobScope.cancel(); source.delete() }
    }
}

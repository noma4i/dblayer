package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import java.nio.file.Files
import org.junit.Assert.assertEquals
import org.junit.Test

class UploadItemTest {
    @Test
    fun ownedSingleFileSurvivesRestartOrCancellation() = bounded {
        for (cancel in listOf(false, true)) {
            val directory = Files.createTempDirectory("upload-item").toFile()
            try {
                val source = java.io.File(directory, "clip.mp4").apply { writeText("abcdef") }
                val config = Config(java.io.File(directory, "db.sqlite").path, null, 32u)
                val definitions = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt } }"}]"""
                val transport = MockTransport()
                transport.enqueue("CreateBlob", """{"data":{"createBlob":{"url":"https://s3/x","headers":{},"signedId":"sid","blobId":"b1"}}}""")
                transport.enqueue("CreateMoment", DbException.Interrupted("lost answer"))
                var database = openDatabase(config, modelsJSON, transport)
                database.registerOperations(definitions)
                if (cancel) database.setOnline(false)
                val request = UploadRequest(source.path, "CreateMoment", createBlob = "CreateBlob")
                val local = database.uploadCreate("Moment", """{"text":"clip"}""", request.json)
                val seen = java.util.concurrent.atomic.AtomicReference<WireRow?>()
                val listener = object : QueryListener {
                    override fun changed(change: LiveChange) {
                        if (change.full || change.rows.isNotEmpty()) seen.set(change.rows.firstOrNull())
                    }
                }
                val live = database.liveFind("Moment", local.id, listener)
                eventually("the pending record is observed") { seen.get() != null }
                assertEquals(true, source.delete())
                if (cancel) {
                    assertEquals(true, database.cancelUpload("Moment", local.id))
                    eventually("cancellation clears the observed upload") { seen.get() != null && seen.get()?.upload == null }
                    assertEquals(local.id, seen.get()?.id)
                    assertEquals(0, transport.calls.size)
                } else {
                    eventually("the transferred file keeps its unknown delivery") {
                        seen.get()?.upload?.status == WireUploadStatus.DELIVERY_UNKNOWN
                    }
                    val progress = seen.get()!!.upload!!
                    assertEquals(6uL, progress.sentBytes)
                    assertEquals(6uL, progress.totalBytes)
                    live.cancel()
                    database.close()
                    database = openDatabase(config, modelsJSON, transport)
                    database.registerOperations(definitions)
                    transport.enqueue("CreateMoment", """{"data":{"createMoment":{"id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
                    database.retry("Moment", local.id)
                    assertEquals("m77", database.confirmed("Moment", local.id))
                    assertEquals(1, transport.calls.count { it.operationName == "CreateBlob" })
                    val final = transport.calls.filter { it.operationName == "CreateMoment" }
                    assertEquals(2, final.size)
                    assertEquals(true, final.all { it.variablesJson.contains("\"signedId\":\"sid\"") })
                }
                live.cancel()
                database.close()
            } finally {
                directory.deleteRecursively()
            }
        }
    }
}

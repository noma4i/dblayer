package com.noma4i.dblayer

import com.noma4i.dblayer.models.Asset
import com.noma4i.dblayer.models.modelsJSON
import java.nio.file.Files
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class UploadBatchTest {
    @Test
    fun batchDiscardRenumbersSnapshots() = bounded {
        for (cancel in listOf(false, true)) {
            val source = Files.createTempFile("renumber", ".png").toFile().apply { writeText("file") }
            val database = openDatabase(Config(null, null, 32u), modelsJSON, MockTransport())
            try {
                adopt(database)
                database.setOnline(false)
                val items = (0..3).map { UploadItem(values = buildJsonObject { put("clientKey", "key-$it"); put("position", it); put("size", 4) }, path = source.path) }
                val rows = Asset.uploadCreate(items, UploadBatchRequest(operation = "create_files", createBlob = "CreateBlob"))
                if (cancel) assertTrue(rows[1].cancelUpload()) else rows[1].discard()
                database.warm("""{"model":"Asset"}""")
                listOf(0, 2, 3).forEachIndexed { position, original ->
                    val snapshot = Asset.read(requireNotNull(database.peekRow("Asset", rows[original].id)))
                    assertEquals(position.toLong(), snapshot.position)
                    assertEquals(3L, snapshot.size)
                }
            } finally {
                DBlayer.close()
                source.delete()
            }
        }
    }

    @Test
    fun batchOwnsSourcesAndPreservesMembership() = bounded {
        for (cancel in listOf(false, true)) {
            val directory = Files.createTempDirectory("upload-batch").toFile()
            try {
                val source = java.io.File(directory, "image.png").apply { writeText("image") }
                val transport = MockTransport()
                val database = openDatabase(Config(null, null, 32u), modelsJSON, transport)
                adopt(database)
                database.setOnline(false)
                database.registerOperations("""[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"}]""")
                val items = (0..1).map { UploadItem(values = buildJsonObject { put("clientKey", "key-$it"); put("size", 2) }, path = source.path) }
                val rows = Asset.uploadCreate(items, UploadBatchRequest(operation = "create_files", createBlob = "CreateBlob"))
                assertEquals(2, rows.size)
                assertTrue(rows.all { it.id.startsWith("temp-") && it.sync.state == SyncState.Sending && it.upload != null })
                assertTrue(source.delete())
                if (cancel) {
                    assertTrue(rows[0].cancelUpload())
                    database.warm("""{"model":"Asset"}""")
                    val first = database.peekRow("Asset", rows[0].id)
                    val second = database.peekRow("Asset", rows[1].id)
                    assertTrue(first != null && first.upload == null)
                    assertTrue(second?.upload != null)
                    assertTrue(transport.calls.isEmpty())
                } else {
                    repeat(2) { transport.enqueue("CreateBlob", """{"data":{"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}""") }
                    transport.enqueue("CreateAssets", """{"data":{"createAssets":{"assets":[{"id":"a1","clientKey":"key-1","size":2},{"id":"a0","clientKey":"key-0","size":2}]}}}""")
                    database.setOnline(true)
                    rows.forEachIndexed { position, row -> assertEquals("a$position", row.confirmedId()) }
                    val calls = transport.calls.filter { it.operationName == "CreateAssets" }
                    assertEquals(1, calls.size)
                    val sent = Json.parseToJsonElement(calls.single().variablesJson).jsonObject.getValue("items").jsonArray
                    assertEquals(listOf("key-0", "key-1"), sent.map { it.jsonObject.getValue("clientKey").jsonPrimitive.content })
                    assertTrue(sent.all { it.jsonObject.getValue("signedId").jsonPrimitive.content == "signed" })
                }
                DBlayer.close()
            } finally {
                directory.deleteRecursively()
            }
        }
    }
}

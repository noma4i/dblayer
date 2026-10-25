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
    fun batchOwnsSourcesAndConfirmsEveryMemberWithOneRequest() = bounded {
        val directory = Files.createTempDirectory("upload-batch").toFile()
        try {
            val source = java.io.File(directory, "image.png").apply { writeText("image") }
            val transport = MockTransport()
            val database = openDatabase(Config(null, null, 32u), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"}]""", uploads = UploadOperations(createBlob = UploadOperation("CreateBlob")))
            adopt(database)
            database.setOnline(false)
            val items = (0..1).map { UploadItem(values = buildJsonObject { put("clientKey", "key-$it"); put("size", 2) }, path = source.path) }
            val rows = Asset.uploadCreate(items, UploadBatchRequest(operation = "create_files"))
            assertEquals(2, rows.size)
            assertTrue(rows.all { it.id.startsWith("temp-") && it.sync.state == SyncState.Sending && it.uploads.isNotEmpty() })
            assertTrue(source.delete())
            repeat(2) { transport.enqueue("CreateBlob", """{"data":{"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}""") }
            transport.enqueue("CreateAssets", """{"data":{"createAssets":{"assets":[{"id":"a1","clientKey":"key-1","size":2},{"id":"a0","clientKey":"key-0","size":2}]}}}""")
            database.setOnline(true)
            rows.forEachIndexed { position, row -> assertEquals("a$position", row.confirmedId()) }
            val calls = transport.calls.filter { it.operationName == "CreateAssets" }
            assertEquals(1, calls.size)
            val sent = Json.parseToJsonElement(calls.single().variablesJson).jsonObject.getValue("items").jsonArray
            assertEquals(listOf("key-0", "key-1"), sent.map { it.jsonObject.getValue("clientKey").jsonPrimitive.content })
            assertTrue(sent.all { it.jsonObject.getValue("signedId").jsonPrimitive.content == "signed" })
            DBlayer.close()
        } finally {
            directory.deleteRecursively()
        }
    }
}

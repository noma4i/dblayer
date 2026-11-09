package com.noma4i.dblayer

import com.noma4i.dblayer.models.Asset
import com.noma4i.dblayer.models.modelsJSON
import java.nio.file.Files
import kotlinx.coroutines.withTimeout
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
    fun pendingBatchAcceptsAndSuppliesSources() = bounded {
        val source = Files.createTempFile("pending-source", ".bin").toFile()
        try {
            source.writeText("image")
            val transport = MockTransport()
            val database = openSigned(Config(null, null), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size } } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"}]""")
            database.setOnline(false)
            val items = (0..1).map { UploadItem(buildJsonObject { put("clientKey", "key-$it"); put("size", 2) }, UploadRequest.pending()) }
            val receipt = AccountScope.current().uploadBatch(Asset, items, UploadBatchRequest("create_files"))
            assertTrue(receipt.items.all { it.optimisticValue.uploads["create_files"]?.waitReason == "source_pending" })
            receipt.items[1].optimisticValue.supplySource("create_files", UploadSource(source.path, values = buildJsonObject { put("size", 3) }))
            receipt.items[0].optimisticValue.failSource("create_files", "preparation")
            val retry = receipt.items[0].optimisticValue.retry("create_files", UploadSource(source.path))
            assertTrue(retry.optimisticValue != null)
            assertTrue(transport.calls.isEmpty())
            closeForCleanup()
        } finally {
            source.delete()
        }
    }

    @Test
    fun batchOwnsSourcesAndConfirmsEveryMemberWithOneRequest() = bounded {
        val directory = Files.createTempDirectory("upload-batch").toFile()
        try {
            val source = java.io.File(directory, "image.png").apply { writeText("image") }
            val transport = MockTransport()
            val database = openSigned(Config(null, null), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size } } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"}]""")
            database.setOnline(false)
            val items = (0..1).map { UploadItem(values = buildJsonObject { put("clientKey", "key-$it"); put("size", 2) }, path = source.path) }
            val receipt = AccountScope.current().uploadBatch(Asset, items, UploadBatchRequest(operation = "create_files"))
            val rows = receipt.items.map { it.optimisticValue }
            assertEquals(2, rows.size)
            assertEquals(listOf("key-0", "key-1"), receipt.items.map { it.item.value })
            assertTrue(rows.all { it.id.startsWith("temp-") && it.sync.state == SyncState.Sending && it.uploads.isNotEmpty() })
            assertTrue(source.delete())
            repeat(2) { transport.enqueue("CreateBlob", """{"data":{"blobCreate":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}""") }
            repeat(2) { transport.enqueuePut("https://storage.test/file") }
            transport.enqueue("CreateAssets", """{"data":{"createAssets":{"assets":[{"id":"a1","clientKey":"key-1","size":2},{"id":"a0","clientKey":"key-0","size":2}]}}}""")
            database.setOnline(true)
            val ids = withTimeout(5000) { confirmedPairs(receipt) }.map { "${it!!.first}:${it.second}" }
            assertEquals(listOf("key-0:a0", "key-1:a1"), ids)
            val calls = transport.calls.filter { it.operationName == "CreateAssets" }
            assertEquals(1, calls.size)
            val sent = Json.parseToJsonElement(calls.single().variablesJson).jsonObject.getValue("items").jsonArray
            assertEquals(listOf("key-0", "key-1"), sent.map { it.jsonObject.getValue("clientKey").jsonPrimitive.content })
            assertTrue(sent.all { it.jsonObject.getValue("signedId").jsonPrimitive.content == "signed" })
            closeForCleanup()
        } finally {
            directory.deleteRecursively()
        }
    }
}

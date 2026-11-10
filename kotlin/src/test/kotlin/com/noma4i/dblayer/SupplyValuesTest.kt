package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Test

class SupplyValuesTest {
    private val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob(${'$'}filename: String!, ${'$'}contentType: String!, ${'$'}byteSize: Int!, ${'$'}checksum: String!) { createBlob(filename: ${'$'}filename, contentType: ${'$'}contentType, byteSize: ${'$'}byteSize, checksum: ${'$'}checksum) { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]"""

    /** The app order: a single send with a pending source, the file supplied while the server holds the blob request. The call returns and the row shows the supplied value at once. */
    @Test
    fun supplySourceOfASingleSendReturnsAndShowsItsValuesWhileTheBlobRequestIsHeld() = bounded {
        val file = java.io.File.createTempFile("clip", ".mp4").apply { writeText("abcdef") }
        val transport = MockTransport()
        openSigned(Config(null, null), modelsJSON, transport, operations = operations)
        val held = transport.hold("CreateBlob", """{"data":{"blobCreate":{"url":"https://s3/x","headers":{},"signedId":"sid","blobId":"b1"}}}""")
        transport.enqueuePut("https://s3/x")
        val text = buildJsonObject { put("text", JsonPrimitive("first")) }
        val sent = AccountScope.current().send(Moment, text, upload = UploadCommand("upload_clip", UploadRequest.pending(text)))
        val local = sent.optimisticValue!!

        local.supplySource("upload_clip", UploadSource(file.path, values = buildJsonObject { put("text", "supplied") }))

        assertEquals("supplied", withTimeout(5000) { Moment.ids(listOf(local.id)).snapshots.flow.first { it.rows.isNotEmpty() }.rows.first().text })
        held.entered()
        assertEquals("supplied", withTimeout(5000) { Moment.ids(listOf(local.id)).snapshots.flow.first { it.rows.isNotEmpty() }.rows.first().text })
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m1","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        held.release()
        sent.confirmed()
        closeForCleanup()
        file.delete()
        Unit
    }
}

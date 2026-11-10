package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import java.io.File
import java.net.URI
import java.nio.file.Files
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.mapNotNull
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class SealedUploadRefusalTest {
    @Test fun sealedUploadRefusalDiscardAllowsAnotherUpload() = bounded {
        for (dataPresent in listOf(false, true)) scenario(dataPresent, true)
    }

    @Test fun sealedUploadRefusalKeepsTheKeyBusy() = bounded {
        for (dataPresent in listOf(false, true)) scenario(dataPresent, false)
    }

    private suspend fun scenario(dataPresent: Boolean, discard: Boolean) = kotlinx.coroutines.coroutineScope {
        val first = Files.createTempFile("first", ".mp4").toFile().apply { writeText("first") }
        val second = Files.createTempFile("second", ".mp4").toFile().apply { writeText("second") }
        try {
            val transport = MockTransport()
            val definitions = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt successMessage } }"}]"""
            openSigned(Config(null, null), modelsJSON, transport, operations = definitions)
            transport.enqueue("CreateBlob", """{"data":{"blobCreate":{"url":"https://storage.test/first","headers":{},"signedId":"first","blobId":"first"}}}""")
            transport.enqueuePut("https://storage.test/first")
            val data = if (dataPresent) """, "data":{"createMoment":null}""" else ""
            val refusal = """{"errors":[{"message":"no face","extensions":{"code":"FORBIDDEN","reasonCode":"no_face_detected"}}]""" + data + "}"
            val held = transport.hold("CreateMoment", reply = refusal)
            val receipt = Moment.uploadClip(AccountScope.current(), id = "absent", source = first.path)
            awaitWithin("the final mutation reached the server") { held.entered() }
            val items = AccountScope.current().operations().mapNotNull { (it as? OperationQueueItem.Queue)?.items }.first()
            val copy = File(URI(requireNotNull(items.first().uploadInfo?.localUrl)))
            assertTrue(copy.exists())
            val waiting = async { runCatching { receipt.confirmed(discardOnFailure = discard) }.exceptionOrNull() }
            held.release()
            val error = awaitWithin("the refused upload answers") { waiting.await() } as? DbException.Rejected
                ?: throw AssertionError("the refusal did not reach confirmed, requests=${transport.calls.map { it.operationName }}")
            assertEquals("no face", error.failure.message)
            assertEquals("FORBIDDEN", error.failure.extensions?.code)
            assertEquals("\"no_face_detected\"", error.failure.extensions?.fields?.get("reasonCode"))
            assertEquals(!discard, error.kept)
            val remaining = AccountScope.current().operations().mapNotNull { (it as? OperationQueueItem.Queue)?.items }.first()
            assertEquals("requests=${transport.calls.map { it.operationName }}", discard, remaining.isEmpty())
            if (discard) {
                assertFalse(copy.exists())
                transport.enqueue("CreateBlob", """{"data":{"blobCreate":{"url":"https://storage.test/second","headers":{},"signedId":"second","blobId":"second"}}}""")
                transport.enqueuePut("https://storage.test/second")
                transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z","successMessage":"second"}}}""")
                val sent = awaitWithin("the second upload is accepted") { Moment.uploadClip(AccountScope.current(), id = "absent", source = second.path) }
                assertEquals("m77", awaitWithin("the second upload is confirmed") { sent.confirmed() }.value?.id)
            } else {
                val blocked = runCatching { awaitWithin("the failed key refuses the second upload") { Moment.uploadClip(AccountScope.current(), id = "absent", source = second.path) } }.exceptionOrNull()
                assertTrue("expected InFlight, got $blocked, requests=${transport.calls.map { it.operationName }}", blocked is DbException.InFlight)
                AccountScope.current().discard("Moment", "absent", "upload_clip")
            }
            assertTrue("requests=${transport.calls.map { it.operationName }}", transport.unscripted.isEmpty())
        } finally {
            closeForCleanup()
            first.delete()
            second.delete()
        }
    }
}

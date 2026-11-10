package com.noma4i.dblayer

import com.noma4i.dblayer.models.CurrentUser
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

class SealedSingletonUploadRefusalTest {
    @Test fun sealedSingletonUploadRefusalDiscardAllowsAnotherUpload() = bounded {
        for (dataPresent in listOf(false, true)) scenario(dataPresent, true)
    }

    @Test fun sealedSingletonUploadRefusalIsReplaced() = bounded {
        for (dataPresent in listOf(false, true)) scenario(dataPresent, false)
    }

    private suspend fun scenario(dataPresent: Boolean, discard: Boolean) = kotlinx.coroutines.coroutineScope {
        val first = Files.createTempFile("first", ".mp4").toFile().apply { writeText("first") }
        val second = Files.createTempFile("second", ".mp4").toFile().apply { writeText("second") }
        try {
            val transport = MockTransport()
            val definitions = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"UserPhotoUpdate","document":"mutation UserPhotoUpdate { userPhotoUpdate { userPhoto { reason media { thumbUrl } } viewer { user { id name } } } }"}]"""
            openSigned(Config(null, null), modelsJSON, transport, operations = definitions)
            transport.enqueue("CreateBlob", """{"data":{"blobCreate":{"url":"https://storage.test/first","headers":{},"signedId":"first","blobId":"first"}}}""")
            transport.enqueuePut("https://storage.test/first")
            val data = if (dataPresent) """, "data":{"userPhotoUpdate":null}""" else ""
            val refusal = """{"errors":[{"message":"no face","extensions":{"code":"FORBIDDEN","reasonCode":"no_face_detected"}}]""" + data + "}"
            val held = transport.hold("UserPhotoUpdate", reply = refusal)
            val receipt = CurrentUser.updatePhoto(AccountScope.current(), source = first.path)
            assertEquals(null, receipt.optimisticValue)
            assertTrue(CurrentUser.ids(listOf("singleton")).read().isEmpty())
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
            transport.enqueue("CreateBlob", """{"data":{"blobCreate":{"url":"https://storage.test/second","headers":{},"signedId":"second","blobId":"second"}}}""")
            transport.enqueuePut("https://storage.test/second")
            val next = transport.hold("UserPhotoUpdate", reply = """{"data":{"userPhotoUpdate":{"userPhoto":{"reason":"ok","media":{"thumbUrl":"https://photo/2"}},"viewer":{"user":{"id":"u1","name":"Bea"}}}}}""")
            val sent = awaitWithin("the second upload is accepted") { CurrentUser.updatePhoto(AccountScope.current(), source = second.path) }
            awaitWithin("the replacement mutation reaches the server") { next.entered() }
            val nextItems = AccountScope.current().operations().mapNotNull { (it as? OperationQueueItem.Queue)?.items }.first()
            assertEquals(1, nextItems.size)
            assertTrue(requireNotNull(nextItems.first().uploadInfo?.localUrl) != items.first().uploadInfo?.localUrl)
            assertFalse(copy.exists())
            if (!discard) {
                val old = runCatching { awaitWithin("the replaced receipt ends") { receipt.confirmed() } }.exceptionOrNull()
                assertTrue("expected discarded, got $old", old is DbException.Interrupted && old.detail == "Discarded")
            }
            next.release()
            assertEquals("u1", awaitWithin("the second upload is confirmed") { sent.confirmed() }.value?.id)
            assertTrue("requests=${transport.calls.map { it.operationName }}", transport.unscripted.isEmpty())
        } finally {
            closeForCleanup()
            first.delete()
            second.delete()
        }
    }
}

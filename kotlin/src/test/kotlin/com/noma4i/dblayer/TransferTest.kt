package com.noma4i.dblayer

import java.nio.file.Files
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.Asset
import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.selects.select
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertNull
import org.junit.Assert.fail
import org.junit.Test
import com.sun.jna.Library
import com.sun.jna.Pointer
import com.sun.jna.ptr.PointerByReference

internal interface LegacySqlite : Library {
    fun sqlite3_open(path: String, database: PointerByReference): Int
    fun sqlite3_exec(database: Pointer, sql: String, callback: Pointer?, context: Pointer?, error: Pointer?): Int
    fun sqlite3_close(database: Pointer): Int
}

private class MultipartTransfer(val mock: MockTransport = MockTransport()) : Transport by mock {
    val puts = java.util.concurrent.CopyOnWriteArrayList<String>()
    val identities = java.util.concurrent.CopyOnWriteArrayList<TransferIdentity>()
    val paths = java.util.concurrent.CopyOnWriteArrayList<Pair<String, java.nio.file.Path>>()
    override suspend fun put(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply {
        puts.add(url)
        identities.add(identity)
        paths.add(identity.itemId to java.io.File(file.path).canonicalFile.toPath())
        return PutReply(200u, "etag")
    }
}

private class TransferCredential : CredentialStore {
    @Volatile private var value: String? = null
    override fun get(): String? = value
    override fun set(credential: String) { value = credential }
    override fun clear() { value = null }
}

private class HoldSecondFile(private val server: SealedServer, private val mock: MockTransport = MockTransport()) : Transport by mock {
    private val count = java.util.concurrent.atomic.AtomicInteger()
    val identities = java.util.Collections.synchronizedList(mutableListOf<TransferIdentity>())
    override suspend fun put(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply {
        identities.add(identity)
        if (count.incrementAndGet() == 1) server.hold("CreateBlob")
        progress.sent(file.length)
        return PutReply(200u, null)
    }
}

private const val REFUSED_BLOB = """{"errors":[{"message":"no storage","extensions":{"code":"FORBIDDEN"}}]}"""

private fun createdBlob(index: Int) = """{"data":{"createBlob":{"url":"https://storage/file","headers":{},"signedId":"signed-$index","blobId":"b$index"}}}"""

private class RefuseFirstFile(private val mock: MockTransport) : Transport by mock {
    private val refused = java.util.concurrent.atomic.AtomicBoolean(false)
    override suspend fun mutate(request: TransportRequest): String {
        val answer = mock.mutate(request)
        val file = Json.parseToJsonElement(request.variablesJson).jsonObject["filename"]?.jsonPrimitive?.content
        return if (request.operationName == "CreateBlob" && file == "0.jpg" && refused.compareAndSet(false, true)) REFUSED_BLOB else answer
    }
}

/** The item outcomes of a batch, read from the core: the API has no item outcome yet (`holes` of the d4 report). */
internal suspend fun <Model : DBModel> confirmedPairs(receipt: OperationReceipt<Model>): List<Pair<String, String>?> {
    val outcome = receipt.database.batchOutcome(receipt.wire)
    assertEquals(WireBatchResult.Confirmed, outcome.result)
    return outcome.items.map {
        when (it) {
            is WireItemOutcome.Confirmed -> it.item.value to receipt.decoder.read(it.value).id
            is WireItemOutcome.Discarded -> null
        }
    }
}

private suspend fun <Model : DBModel> confirmedIds(receipt: OperationReceipt<Model>): List<String?> = confirmedPairs(receipt).map { it?.second }

private suspend fun finishTransfers() = DBlayer.database.resumeUploads(DBlayer.database.sessionEpoch())

class TransferTest {
    @Test
    fun multipartBatchAcceptsACorrelationKeyWithPathCharacters() = bounded {
        val directory = Files.createTempDirectory("multipart-key").toFile()
        try {
            val source = directory.resolve("video").apply { writeBytes(ByteArray(11 * 1024 * 1024)) }
            val transport = MultipartTransfer()
            repeat(2) {
                transport.mock.enqueue("InitMultipart", """{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}""")
                transport.mock.enqueue("PresignParts", """{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}""")
                transport.mock.enqueue("CompleteMultipart", """{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}""")
            }
            transport.mock.enqueue("CreateAssets", """{"data":{"createAssets":{"assets":[{"id":"a0","clientKey":"../../../outside","size":2,"position":0},{"id":"a1","clientKey":"other","size":2,"position":1}]}}}""")
            val operations = """[{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }"}]"""
            val uploads = UploadOperations(multipart = UploadOperations.Multipart(UploadOperation("InitMultipart"), UploadOperation("PresignParts"), UploadOperation("CompleteMultipart"), UploadOperation("AbortMultipart")))
            reopen(Config(directory.resolve("db.sqlite").path, null, 8u), modelsJSON, transport, operations, uploads)
            val items = listOf("../../../outside", "other").mapIndexed { index, key -> UploadItem(values = buildJsonObject { put("clientKey", key); put("size", 2); put("position", index) }, path = source.path) }
            val receipt = AccountScope.current().uploadBatch(Asset, items, UploadBatchRequest(operation = "create_files"))
            assertEquals(listOf("a0", "a1"), confirmedIds(receipt))
            assertEquals(3, transport.identities.count { it.itemId == "../../../outside" })
            val ranges = directory.resolve("db.sqlite.cache/ranges").canonicalFile.toPath()
            assertTrue(transport.paths.filter { it.first == "../../../outside" }.all { it.second.startsWith(ranges) })
        } finally {
            DBlayer.close()
            directory.deleteRecursively()
        }
    }

    @OptIn(DBlayerTestingApi::class)
    @Test
    fun shiftingABatchDuringAHeldEncryptedRequestKeepsTheFileOwner() = bounded {
        val directory = Files.createTempDirectory("encrypted-album").toFile()
        try {
            for (case in listOf("live", "reopen")) {
                val lose = case != "live"
                val server = SealedServer(deduplicate = true) { path, body ->
                    when {
                        path == "/api/auth/login_link" -> Json.parseToJsonElement("""{"sessionId":"A"}""").jsonObject
                        body["operationName"]?.jsonPrimitive?.content == "CreateBlob" -> {
                            val name = body.getValue("variables").jsonObject.getValue("filename").jsonPrimitive.content
                            Json.parseToJsonElement("""{"data":{"createBlob":{"url":"https://storage/file","headers":{},"signedId":"signed-$name","blobId":"blob-$name"}}}""").jsonObject
                        }
                        body["operationName"]?.jsonPrimitive?.content == "CreateAssets" -> Json.parseToJsonElement("""{"data":{"createAssets":{"assets":[{"id":"a2","clientKey":"key-2","size":2,"position":1},{"id":"a1","clientKey":"key-1","size":2,"position":0}]}}}""").jsonObject
                        else -> Json.parseToJsonElement("""{"errors":[{"message":"unexpected operation"}]}""").jsonObject
                    }
                }
                try {
                    val encrypted = EncryptedConfig("https://api.example.com", SealedServer.SERVER_KEY, SealedServer.SERVER_PIN, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("login_link" to EncryptedSessionOperation("/api/auth/login_link", false))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel")
                    val transport = HoldSecondFile(server)
                    val credential = TransferCredential()
                    val path = directory.resolve("db-$case.sqlite").path
                    val uploads = UploadOperations(createBlob = UploadOperation("CreateBlob"))
                    val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }"}]"""
                    DBlayer.close()
                    DBlayer.openTesting(transport, modelsJSON, path = path, uploads = uploads, encrypted = encrypted, credentials = credential, http = server)
                    DBlayer.database.registerOperations(operations)
                    DBlayer.database.sessionCall("login_link", """{"account":"A"}""")
                    val files = (0..2).map { index -> directory.resolve("$index.jpg").apply { writeText("image-$index") } }
                    val items = files.mapIndexed { index, file -> UploadItem(values = buildJsonObject { put("clientKey", "key-$index"); put("size", 3); put("position", index) }, path = file.path) }
                    val receipt = AccountScope.current().uploadBatch(Asset, items, UploadBatchRequest(operation = "create_files"))
                    val rows = receipt.items.map { it.optimisticValue }
                    server.heldRequest()
                    rows[0].discard("create_files")
                    if (lose) { DBlayer.database.setOnline(false); server.loseAnswer("1.jpg") }
                    server.hold(null)
                    if (lose) {
                        server.lostAnswer()
                        DBlayer.close()
                        DBlayer.openTesting(transport, modelsJSON, path = path, uploads = uploads, encrypted = encrypted, credentials = credential, http = server)
                        DBlayer.database.registerOperations(operations)
                        DBlayer.database.sessionResume()
                        finishTransfers()
                        DBlayer.database.flush()
                        assertEquals(listOf("a1", "a2"), listOf("a1", "a2").filter { Asset.find(it).current() != null })
                    } else {
                        assertEquals(listOf(null, "a1", "a2"), confirmedIds(receipt))
                    }
                    val blobs = server.effects.filter { it["operationName"]?.jsonPrimitive?.content == "CreateBlob" }
                    assertEquals(1, blobs.count { it.getValue("variables").jsonObject.getValue("filename").jsonPrimitive.content == "1.jpg" })
                    val attempts = server.requests.map { it.second }.count { it["operationName"]?.jsonPrimitive?.content == "CreateBlob" && it.getValue("variables").jsonObject.getValue("filename").jsonPrimitive.content == "1.jpg" }
                    assertEquals(if (lose) 2 else 1, attempts)
                    assertTrue(server.routes.filter { it.first == "CreateBlob" }.all { it.second == NetworkPurpose.TRANSFER })
                    val identities = synchronized(transport.identities) { transport.identities.toList() }
                    val second = identities.single { it.itemId == "key-1" }
                    val third = identities.single { it.itemId == "key-2" }
                    assertEquals(second.operationId, third.operationId)
                    assertEquals(TransferStep.Put, second.step)
                    assertEquals(TransferStep.Put, third.step)
                    val final = server.requests.map { it.second }.single { it["operationName"]?.jsonPrimitive?.content == "CreateAssets" }
                    val sent = final.getValue("variables").jsonObject.getValue("items").jsonArray
                    assertEquals(listOf("key-1", "key-2"), sent.map { it.jsonObject.getValue("clientKey").jsonPrimitive.content })
                    assertEquals(listOf("signed-1.jpg", "signed-2.jpg"), sent.map { it.jsonObject.getValue("signedId").jsonPrimitive.content })
                    DBlayer.close()
                } finally { server.hold(null); DBlayer.close() }
            }
        } finally { DBlayer.close(); directory.deleteRecursively() }
    }

    @OptIn(DBlayerTestingApi::class)
    @Test
    fun reopeningAfterSevenDaysExpiresAFailedBlob() = bounded {
        val directory = Files.createTempDirectory("blob-reopen").toFile()
        val scope = CoroutineScope(SupervisorJob())
        try {
            val source = directory.resolve("video").apply { writeText("video") }
            val path = directory.resolve("db.sqlite").path
            val uploads = UploadOperations(createBlob = UploadOperation("CreateBlob"))
            val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"}]"""
            DBlayer.close()
            val transport = MockTransport()
            transport.enqueue("CreateBlob", DbException.Http(422u, "rejected"))
            DBlayer.openTesting(transport, "[]", path = path, uploads = uploads, startMillis = 1_000_000L)
            DBlayer.database.registerOperations(operations)
            val blob = DBlayer.upload(BlobUpload("video", "CreateBlob"), UploadRequest(source.path), scope)
            try { blob.waitUntilUploaded(); fail("the refused upload returned a signed ID") } catch (_: DbException) {}
            val id = blob.id
            blob.close()
            DBlayer.close()
            DBlayer.openTesting(transport, "[]", path = path, uploads = uploads, startMillis = 1_000_000L + 7L * 24 * 60 * 60 * 1000)
            DBlayer.database.registerOperations(operations)
            val restored = DBlayer.blob(id, scope)
            val expired = restored.snapshot.first { it.state == BlobState.EXPIRED }
            assertEquals(BlobState.EXPIRED, expired.state)
            restored.close()
            DBlayer.close()
        } finally { DBlayer.close(); scope.cancel(); directory.deleteRecursively() }
    }

    @Test
    fun anHttpRefusalOfAPhotoKeepsTheFailedRecordUntilDiscard() = bounded {
        val source = Files.createTempFile("photo", ".jpg").toFile().apply { writeText("image") }
        try {
            val transport = MockTransport()
            transport.enqueue("CreateBlob", """{"data":{"createBlob":{"url":"https://storage/file","headers":{},"signedId":"signed","blobId":"blob"}}}""")
            transport.enqueue("CreateMoment", DbException.Http(422u, "rejected"))
            val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt } }"}]"""
            reopen(Config(null, null, 32u), modelsJSON, transport, operations, UploadOperations(createBlob = UploadOperation("CreateBlob")))
            val receipt = AccountScope.current().send(Moment, buildJsonObject { put("text", "group") }, upload = UploadCommand("upload_clip", UploadRequest(source.path)))
            val row = requireNotNull(receipt.optimisticValue)
            assertTrue(receipt.outcome() is OperationOutcome.Rejected)
            val failed = requireNotNull(Moment.find(row.id).current())
            assertEquals(SyncState.Failed, failed.sync.state)
            failed.discard(null)
            assertNull(Moment.find(row.id).current())
            DBlayer.close()
        } finally { DBlayer.close(); source.delete() }
    }

    @Test
    fun discardingAQueuedPhotoOfflineRoutesTheAbortAfterReopen() = bounded {
        val directory = Files.createTempDirectory("photo").toFile()
        val transport = MockTransport()
        transport.enqueue("InitMultipart", """{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}""")
        val held = transport.hold("PresignParts")
        val abort = transport.hold("AbortMultipart", reply = """{"data":{"abortMultipart":{"ok":true}}}""")
        try {
            val source = directory.resolve("photo").apply { writeBytes(ByteArray(11 * 1024 * 1024)) }
            val config = Config(directory.resolve("db.sqlite").path, null, 32u)
            val operations = """[{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"""
            val uploads = UploadOperations(multipart = UploadOperations.Multipart(UploadOperation("InitMultipart"), UploadOperation("PresignParts"), UploadOperation("CompleteMultipart"), UploadOperation("AbortMultipart")))
            val db = reopen(config, modelsJSON, transport, operations, uploads)
            val queued = requireNotNull(AccountScope.current().send(Moment, buildJsonObject { put("text", "group") }, upload = UploadCommand("upload_clip", UploadRequest(source.path))).optimisticValue)
            held.entered()
            db.setOnline(false)
            queued.discard(null)
            assertNull(Moment.find(queued.id).current())
            held.release()
            DBlayer.close()
            val reopened = reopen(config, modelsJSON, transport, operations, uploads)
            reopened.setOnline(false)
            reopened.setOnline(true)
            abort.entered()
            abort.release()
            reopened.resumeUploads(reopened.sessionEpoch())
            reopened.setOnline(false)
            reopened.setOnline(true)
            reopened.resumeUploads(reopened.sessionEpoch())
            val aborts = transport.calls.filter { it.operationName == "AbortMultipart" }
            assertEquals(1, aborts.size)
            assertEquals(NetworkPurpose.TRANSFER, aborts.single().purpose)
            DBlayer.close()
        } finally { held.release(); abort.release(); DBlayer.close(); directory.deleteRecursively() }
    }

    @Test
    fun discardingARefusedFileSendsTheRemainingFileAsSingle() = bounded {
        val directory = Files.createTempDirectory("album-single").toFile()
        try {
            val files = (0..1).map { index -> directory.resolve("$index.jpg").apply { writeText("image-$index") } }
            val transport = MockTransport()
            repeat(2) { index -> transport.enqueue("CreateBlob", createdBlob(index)) }
            transport.enqueue("CreateAsset", """{"data":{"createAsset":{"asset":{"id":"a1","clientKey":"key-1","size":2,"position":1}}}}""")
            val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }"}]"""
            reopen(Config(null, null, 32u), modelsJSON, RefuseFirstFile(transport), operations, UploadOperations(createBlob = UploadOperation("CreateBlob")))
            val items = files.mapIndexed { index, file -> UploadItem(values = buildJsonObject { put("clientKey", "key-$index"); put("size", 2); put("position", index) }, path = file.path) }
            val receipt = AccountScope.current().uploadBatch(Asset, items, UploadBatchRequest(operation = "create_files"))
            val rows = receipt.items.map { it.optimisticValue }
            finishTransfers()
            assertEquals(SyncState.Failed, requireNotNull(Asset.find(rows[0].id).current()).sync.state)
            rows[0].discard("create_files")
            assertEquals(listOf(null, "a1"), confirmedIds(receipt))
            assertEquals(2, transport.calls.count { it.operationName == "CreateBlob" })
            assertTrue(transport.calls.none { it.operationName == "CreateAssets" })
            val call = transport.calls.single { it.operationName == "CreateAsset" }
            assertEquals("key-1", Json.parseToJsonElement(call.variablesJson).jsonObject.getValue("clientKey").jsonPrimitive.content)
            DBlayer.close()
        } finally { DBlayer.close(); directory.deleteRecursively() }
    }

    @Test
    fun retryingARefusedFileKeepsTheOtherAlbumFiles() = bounded {
        val directory = Files.createTempDirectory("album-retry").toFile()
        try {
            val files = (0..2).map { index -> directory.resolve("$index.jpg").apply { writeText("image-$index") } }
            val transport = MockTransport()
            repeat(4) { index -> transport.enqueue("CreateBlob", createdBlob(index)) }
            transport.enqueue("CreateAssets", """{"data":{"createAssets":{"assets":[{"id":"a2","clientKey":"key-2","size":3,"position":2},{"id":"a1","clientKey":"key-1","size":3,"position":1},{"id":"a0","clientKey":"key-0","size":3,"position":0}]}}}""")
            val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }"}]"""
            reopen(Config(null, null, 32u), modelsJSON, RefuseFirstFile(transport), operations, UploadOperations(createBlob = UploadOperation("CreateBlob")))
            val items = files.mapIndexed { index, file -> UploadItem(values = buildJsonObject { put("clientKey", "key-$index"); put("size", 3); put("position", index) }, path = file.path) }
            val receipt = AccountScope.current().uploadBatch(Asset, items, UploadBatchRequest(operation = "create_files"))
            val rows = receipt.items.map { it.optimisticValue }
            finishTransfers()
            assertEquals(SyncState.Failed, requireNotNull(Asset.find(rows[0].id).current()).sync.state)
            assertEquals(receipt.operation, rows[0].retry("create_files").operation)
            assertEquals(listOf("a0", "a1", "a2"), confirmedIds(receipt))
            val call = transport.calls.single { it.operationName == "CreateAssets" }
            val sent = Json.parseToJsonElement(call.variablesJson).jsonObject.getValue("items").jsonArray
            assertEquals(listOf("key-0", "key-1", "key-2"), sent.map { it.jsonObject.getValue("clientKey").jsonPrimitive.content })
            assertEquals(4, transport.calls.count { it.operationName == "CreateBlob" })
            val filenames = transport.calls.filter { it.operationName == "CreateBlob" }.map { Json.parseToJsonElement(it.variablesJson).jsonObject.getValue("filename").jsonPrimitive.content }.sorted()
            assertEquals(listOf("0.jpg", "0.jpg", "1.jpg", "2.jpg"), filenames)
            DBlayer.close()
        } finally { DBlayer.close(); directory.deleteRecursively() }
    }

    @Test
    fun cancellingTheMiddleFileKeepsTheOtherFilesInOrder() = bounded {
        val directory = Files.createTempDirectory("album").toFile()
        val transport = MockTransport()
        val held = transport.hold("CreateBlob", reply = """{"data":{"createBlob":{"url":"https://storage/file","headers":{},"signedId":"signed-0","blobId":"b0"}}}""")
        try {
            val files = (0..2).map { index -> directory.resolve("$index.jpg").apply { writeText("image-$index") } }
            transport.enqueue("CreateBlob", """{"data":{"createBlob":{"url":"https://storage/file","headers":{},"signedId":"signed-2","blobId":"b2"}}}""")
            transport.enqueue("CreateAssets", """{"data":{"createAssets":{"assets":[{"id":"a2","clientKey":"key-2","size":2,"position":1},{"id":"a0","clientKey":"key-0","size":2,"position":0}]}}}""")
            val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }"}]"""
            reopen(Config(null, null, 32u), modelsJSON, transport, operations, UploadOperations(createBlob = UploadOperation("CreateBlob")))
            val items = files.mapIndexed { index, file -> UploadItem(values = buildJsonObject { put("clientKey", "key-$index"); put("size", 3); put("position", index) }, path = file.path) }
            val receipt = AccountScope.current().uploadBatch(Asset, items, UploadBatchRequest(operation = "create_files"))
            val rows = receipt.items.map { it.optimisticValue }
            held.entered()
            assertTrue(rows[1].cancelUpload("create_files"))
            val first = requireNotNull(Asset.find(rows[0].id).current())
            val last = requireNotNull(Asset.find(rows[2].id).current())
            assertEquals(listOf(0L, 1L), listOf(first.position, last.position))
            assertEquals(listOf(2L, 2L), listOf(first.size, last.size))
            held.release()
            assertEquals(listOf("a0", null, "a2"), confirmedIds(receipt))
            val call = transport.calls.single { it.operationName == "CreateAssets" }
            val sent = Json.parseToJsonElement(call.variablesJson).jsonObject.getValue("items").jsonArray
            assertEquals(listOf("key-0", "key-2"), sent.map { it.jsonObject.getValue("clientKey").jsonPrimitive.content })
            assertEquals(2, transport.calls.count { it.operationName == "CreateBlob" })
            val filenames = transport.calls.filter { it.operationName == "CreateBlob" }.map { Json.parseToJsonElement(it.variablesJson).jsonObject.getValue("filename").jsonPrimitive.content }.sorted()
            assertEquals(listOf("0.jpg", "2.jpg"), filenames)
            DBlayer.close()
        } finally { held.release(); DBlayer.close(); directory.deleteRecursively() }
    }

    @OptIn(DBlayerTestingApi::class)
    @Test
    fun aFailedBlobExpiresWhenTheNetworkReturnsAfterSevenDays() = bounded {
        val source = Files.createTempFile("blob-expiry", ".mp4").toFile().apply { writeText("video") }
        val scope = CoroutineScope(SupervisorJob())
        try {
            DBlayer.close()
            val transport = MockTransport()
            transport.enqueue("CreateBlob", DbException.Http(422u, "rejected"))
            val clock = DBlayer.openTesting(transport, "[]", uploads = UploadOperations(createBlob = UploadOperation("CreateBlob")))
            DBlayer.database.registerOperations("""[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"}]""")
            val blob = DBlayer.upload(BlobUpload("video", "CreateBlob"), UploadRequest(source.path), scope)
            try { blob.waitUntilUploaded(); fail("the refused upload returned a signed ID") } catch (_: DbException) {}
            DBlayer.database.setOnline(false)
            clock.advance(7L * 24 * 60 * 60 * 1000)
            DBlayer.database.setOnline(true)
            val expired = blob.snapshot.first { it.state == BlobState.EXPIRED }
            assertEquals(BlobState.EXPIRED, expired.state)
            blob.close()
            DBlayer.close()
        } finally { DBlayer.close(); scope.cancel(); source.delete() }
    }

    @Test
    fun completeFailureThenRetryKeepsTheConfirmedParts() = bounded {
        val directory = Files.createTempDirectory("multipart").toFile()
        val scope = CoroutineScope(SupervisorJob())
        try {
            val source = directory.resolve("video").apply { writeBytes(ByteArray(11 * 1024 * 1024)) }
            val transport = MultipartTransfer()
            transport.mock.enqueue("InitMultipart", """{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}""")
            transport.mock.enqueue("PresignParts", """{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}""")
            transport.mock.enqueue("CompleteMultipart", DbException.Http(422u, "rejected"))
            transport.mock.enqueue("CompleteMultipart", """{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}""")
            val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"""
            val uploads = UploadOperations(multipart = UploadOperations.Multipart(UploadOperation("InitMultipart"), UploadOperation("PresignParts"), UploadOperation("CompleteMultipart"), UploadOperation("AbortMultipart")))
            reopen(Config(null, null, 8u), "[]", transport, operations, uploads)
            val blob = DBlayer.upload(BlobUpload("video", "CreateBlob"), UploadRequest(source.path), scope)
            try { blob.waitUntilUploaded(); fail("the failed complete returned a signed ID") } catch (_: DbException) {}
            blob.retry()
            assertEquals("signed", blob.waitUntilUploaded())
            val names = transport.mock.calls.map { it.operationName }
            assertEquals(1, names.count { it == "InitMultipart" })
            assertEquals(1, names.count { it == "PresignParts" })
            assertEquals(2, names.count { it == "CompleteMultipart" })
            assertEquals(listOf("https://storage/1", "https://storage/2", "https://storage/3"), transport.puts.sorted())
            blob.close()
            DBlayer.close()
        } finally { DBlayer.close(); scope.cancel(); directory.deleteRecursively() }
    }

    @Test
    fun attachedFilesSurviveReopenAndDetachingOneKeepsTheOther() = bounded {
        val directory = Files.createTempDirectory("files").toFile()
        try {
            val config = Config(directory.resolve("files.sqlite").path, null, 32u)
            reopen(config, modelsJSON, null)
            val first = requireNotNull(User.create(name = "first").optimisticValue)
            val second = requireNotNull(User.create(name = "second").optimisticValue)
            val large = ByteArray(300 * 1024) { 9 }
            first.attach(User.avatar, byteArrayOf(1, 2, 3), "small", "image/png")
            second.attach(User.avatar, large, "large", "image/png")
            assertArrayEquals(byteArrayOf(1, 2, 3), first.read(User.avatar))
            assertArrayEquals(large, java.io.File(requireNotNull(second.url(User.avatar))).readBytes())
            first.attach(User.avatar, byteArrayOf(4), "replacement", "image/png")
            assertTrue(first.detach(User.avatar))
            val firstId = first.id
            val secondId = second.id
            DBlayer.close()
            reopen(config, modelsJSON, null)
            val restoredFirst = requireNotNull(User.find(firstId).current())
            val restoredSecond = requireNotNull(User.find(secondId).current())
            assertNull(restoredFirst.read(User.avatar))
            assertArrayEquals(large, restoredSecond.read(User.avatar))
            val info = requireNotNull(restoredSecond.file(User.avatar))
            assertEquals("large", info.name)
            assertEquals(300L * 1024, info.size)
            DBlayer.close()
        } finally { DBlayer.close(); directory.deleteRecursively() }
    }

    @Test
    fun anOfflineDiscardRoutesItsReopenedAbortAsATransfer() = bounded {
        val directory = Files.createTempDirectory("transfer").toFile()
        val scope = CoroutineScope(SupervisorJob())
        val transport = MockTransport()
        transport.enqueue("InitMultipart", """{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}""")
        val held = transport.hold("PresignParts")
        val abort = transport.hold("AbortMultipart", reply = """{"data":{"abortMultipart":{"ok":true}}}""")
        try {
            val source = directory.resolve("video").apply { writeBytes(ByteArray(11 * 1024 * 1024)) }
            val config = Config(directory.resolve("db.sqlite").path, null, 8u)
            val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"""
            val uploads = UploadOperations(multipart = UploadOperations.Multipart(UploadOperation("InitMultipart"), UploadOperation("PresignParts"), UploadOperation("CompleteMultipart"), UploadOperation("AbortMultipart")))
            val db = reopen(config, "[]", transport, operations, uploads)
            val blob = DBlayer.upload(BlobUpload("video", "CreateBlob"), UploadRequest(source.path), scope)
            held.entered()
            val files = directory.resolve("db.sqlite.files")
            assertEquals(1, requireNotNull(files.list()).size)
            db.setOnline(false)
            blob.discard()
            assertTrue(requireNotNull(files.list()).isEmpty())
            assertTrue(source.exists())
            held.release()
            blob.close()
            DBlayer.close()
            val reopened = reopen(config, "[]", transport, operations, uploads)
            reopened.setOnline(false)
            reopened.setOnline(true)
            abort.entered()
            abort.release()
            reopened.resumeUploads(reopened.sessionEpoch())
            reopened.setOnline(false)
            reopened.setOnline(true)
            reopened.resumeUploads(reopened.sessionEpoch())
            val aborts = transport.calls.filter { it.operationName == "AbortMultipart" }
            assertEquals(1, aborts.size)
            assertEquals(NetworkPurpose.TRANSFER, aborts.single().purpose)
            DBlayer.close()
        } finally {
            held.release()
            abort.release()
            DBlayer.close()
            scope.cancel()
            directory.deleteRecursively()
        }
    }
}

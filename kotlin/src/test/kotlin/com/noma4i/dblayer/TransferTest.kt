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

/** A part gets its own presign: each of the three parts of an 11 MiB file asks once and finds its url in the answer. */
private fun enqueuePresigns(mock: MockTransport) {
    repeat(3) {
        mock.enqueue("PresignParts", """{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}""")
    }
}

internal interface Sqlite3Library : Library {
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

/** The item outcomes of a batch: each item has its own receipt. */
internal suspend fun <Model : DBModel> confirmedPairs(receipt: OperationReceipt<Model>): List<Pair<String, String>?> =
    receipt.items.map { item -> (item.receipt.outcome() as? OperationOutcome.Confirmed<Model>)?.confirmedValue?.let { item.item.value to it.id } }

private suspend fun <Model : DBModel> confirmedIds(receipt: OperationReceipt<Model>): List<String?> = confirmedPairs(receipt).map { it?.second }


class TransferTest {
    @Test
    fun multipartBatchAcceptsACorrelationKeyWithPathCharacters() = bounded {
        val directory = Files.createTempDirectory("multipart-key").toFile()
        try {
            val source = directory.resolve("video").apply { writeBytes(ByteArray(11 * 1024 * 1024)) }
            val transport = MultipartTransfer()
            repeat(2) {
                transport.mock.enqueue("InitMultipart", """{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}""")
                enqueuePresigns(transport.mock)
                transport.mock.enqueue("CompleteMultipart", """{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}""")
            }
            transport.mock.enqueue("CreateAssets", """{"data":{"createAssets":{"assets":[{"id":"a0","clientKey":"../../../outside","size":2,"position":0},{"id":"a1","clientKey":"other","size":2,"position":1}]}}}""")
            val operations = """[{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }"}]"""
            reopen(Config(directory.resolve("db.sqlite").path, null, 8u), modelsJSON, transport, operations)
            val items = listOf("../../../outside", "other").mapIndexed { index, key -> UploadItem(values = buildJsonObject { put("clientKey", key); put("size", 2); put("position", index) }, path = source.path) }
            val receipt = AccountScope.current().uploadBatch(Asset, items, UploadBatchRequest(operation = "create_files"))
            assertEquals(listOf("a0", "a1"), confirmedIds(receipt))
            assertEquals(3, transport.identities.count { it.itemId == "../../../outside" })
            val ranges = directory.resolve("db.sqlite.files/transfers").canonicalFile.toPath()
            assertTrue(transport.paths.filter { it.first == "../../../outside" }.all { it.second.startsWith(ranges) })
        } finally {
            closeForCleanup()
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
                    val encrypted = EncryptedConfig("https://api.example.com", SealedServer.SERVER_KEY, SealedServer.SERVER_PIN, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("login_link" to EncryptedSessionOperation("/api/auth/login_link", EncryptedOperationClass.AUTHENTICATED))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel")
                    val transport = HoldSecondFile(server)
                    val credential = TransferCredential()
                    val path = directory.resolve("db-$case.sqlite").path
                    val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }"}]"""
                    closeForCleanup()
                    DBlayer.openTesting(transport, modelsJSON, path = path, encrypted = encrypted, credentials = credential, http = server)
                    DBlayer.database.registerOperations(operations)
                    DBlayer.database.setOnline(true)
                    DBlayer.database.sessionCall("login_link", """{"account":"A"}""")
                    val files = (0..2).map { index -> directory.resolve("$index.jpg").apply { writeText("image-$index") } }
                    val items = files.mapIndexed { index, file -> UploadItem(values = buildJsonObject { put("clientKey", "key-$index"); put("size", 3); put("position", index) }, path = file.path) }
                    val receipt = AccountScope.current().uploadBatch(Asset, items, UploadBatchRequest(operation = "create_files"))
                    val rows = receipt.items.map { it.optimisticValue }
                    awaitWithin("the held request reaches the server") { server.heldRequest() }
                    awaitWithin("the discard of the first file") { rows[0].discard("create_files") }
                    if (lose) { DBlayer.database.setOnline(false); server.loseAnswer("1.jpg") }
                    server.hold(null)
                    if (lose) {
                        awaitWithin("the answer is lost") { server.lostAnswer() }
                        closeForCleanup()
                        DBlayer.openTesting(transport, modelsJSON, path = path, encrypted = encrypted, credentials = credential, http = server)
                        DBlayer.database.registerOperations(operations)
                        DBlayer.database.setOnline(true)
                        eventually("both assets are stored") { Asset.find("a1").current() != null && Asset.find("a2").current() != null }
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
                    val puts = identities.filter { it.step == TransferStep.Put }
                    assertEquals(listOf("key-0", "key-1", "key-2"), puts.map { it.itemId }.sorted())
                    assertEquals(1, puts.map { it.operationId }.toSet().size)
                    val final = server.requests.map { it.second }.single { it["operationName"]?.jsonPrimitive?.content == "CreateAssets" }
                    val sent = final.getValue("variables").jsonObject.getValue("items").jsonArray
                    assertEquals(listOf("key-1", "key-2"), sent.map { it.jsonObject.getValue("clientKey").jsonPrimitive.content })
                    assertEquals(listOf("signed-1.jpg", "signed-2.jpg"), sent.map { it.jsonObject.getValue("signedId").jsonPrimitive.content })
                    closeForCleanup()
                } finally { server.hold(null); closeForCleanup() }
            }
        } finally { closeForCleanup(); directory.deleteRecursively() }
    }

    private val expiryOperations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"}]"""

    private suspend fun refusedUpload(transport: MockTransport, source: java.io.File): Moment {
        transport.enqueue("CreateBlob", DbException.Http(422u, "rejected"))
        val sent = AccountScope.current().send(Moment, buildJsonObject { put("text", "video") }, upload = UploadCommand("upload_clip", UploadRequest(source.path)))
        val row = requireNotNull(sent.optimisticValue)
        eventually("the refused upload failed") { Moment.find(row.id).current()?.sync?.state == SyncState.Failed }
        return row
    }

    private suspend fun isGone(row: Moment): Boolean = DBlayer.database.readRow(DBlayer.database.sessionEpoch(), "Moment", row.id) == null

    @OptIn(DBlayerTestingApi::class)
    @Test
    fun reopeningAfterSevenDaysExpiresAFailedBlob() = bounded {
        val directory = Files.createTempDirectory("blob-reopen").toFile()
        try {
            val source = directory.resolve("video").apply { writeText("video") }
            val path = directory.resolve("db.sqlite").path
            closeForCleanup()
            val transport = MockTransport()
            reopenClocked(modelsJSON, transport, expiryOperations, path = path, startMillis = 1_000_000L)
            val row = refusedUpload(transport, source)
            closeForCleanup()
            val reopened = reopenClocked(modelsJSON, transport, expiryOperations, path = path, startMillis = 1_000_000L + 7L * 24 * 60 * 60 * 1000 + 1)
            eventually("the expired upload is discarded at the open") { reopened.advance(1); isGone(row) }
            closeForCleanup()
        } finally { closeForCleanup(); directory.deleteRecursively() }
    }


    @Test
    fun anHttpRefusalOfAPhotoKeepsTheFailedRecordUntilDiscard() = bounded {
        val source = Files.createTempFile("photo", ".jpg").toFile().apply { writeText("image") }
        try {
            val transport = MockTransport()
            transport.enqueue("CreateBlob", """{"data":{"createBlob":{"url":"https://storage/file","headers":{},"signedId":"signed","blobId":"blob"}}}""")
            transport.enqueue("CreateMoment", DbException.Http(422u, "rejected"))
            val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt } }"}]"""
            reopen(Config(null, null, 32u), modelsJSON, transport, operations)
            val receipt = AccountScope.current().send(Moment, buildJsonObject { put("text", "group") }, upload = UploadCommand("upload_clip", UploadRequest(source.path)))
            val row = requireNotNull(receipt.optimisticValue)
            assertTrue(receipt.outcome() is OperationOutcome.Rejected)
            val failed = requireNotNull(Moment.find(row.id).current())
            assertEquals(SyncState.Failed, failed.sync.state)
            failed.discard(null)
            assertNull(Moment.find(row.id).current())
            closeForCleanup()
        } finally { closeForCleanup(); source.delete() }
    }

    @Test
    fun discardingAQueuedPhotoOfflineRoutesTheAbortAfterReopen() = bounded {
        val directory = Files.createTempDirectory("photo").toFile()
        val transport = MockTransport()
        transport.enqueue("InitMultipart", """{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}""")
        val held = transport.hold("PresignParts", reply = """{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}""")
        val abort = transport.hold("AbortMultipart", reply = """{"data":{"abortMultipart":{"ok":true}}}""")
        transport.enqueue("PresignParts", """{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}""")
        try {
            val source = directory.resolve("photo").apply { writeBytes(ByteArray(11 * 1024 * 1024)) }
            val config = Config(directory.resolve("db.sqlite").path, null, 32u)
            val operations = """[{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"""
            val db = reopen(config, modelsJSON, transport, operations)
            val queued = requireNotNull(AccountScope.current().send(Moment, buildJsonObject { put("text", "group") }, upload = UploadCommand("upload_clip", UploadRequest(source.path))).optimisticValue)
            held.entered()
            db.setOnline(false)
            queued.discard(null)
            assertNull(Moment.find(queued.id).current())
            held.release()
            closeForCleanup()
            val reopened = reopen(config, modelsJSON, transport, operations)
            reopened.setOnline(false)
            reopened.setOnline(true)
            abort.entered()
            abort.release()
            reopened.setOnline(false)
            reopened.setOnline(true)
            assertEquals("the saved pair of the held step is sent once more", 2, transport.calls.count { it.operationName == "PresignParts" })
            val aborts = transport.calls.filter { it.operationName == "AbortMultipart" }
            assertEquals(1, aborts.size)
            assertEquals(NetworkPurpose.TRANSFER, aborts.single().purpose)
            closeForCleanup()
        } finally { held.release(); abort.release(); closeForCleanup(); directory.deleteRecursively() }
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
            reopen(Config(null, null, 32u), modelsJSON, RefuseFirstFile(transport), operations)
            val items = files.mapIndexed { index, file -> UploadItem(values = buildJsonObject { put("clientKey", "key-$index"); put("size", 2); put("position", index) }, path = file.path) }
            val receipt = AccountScope.current().uploadBatch(Asset, items, UploadBatchRequest(operation = "create_files"))
            val rows = receipt.items.map { it.optimisticValue }
            eventually("the refused file failed") { Asset.find(rows[0].id).current()?.sync?.state == SyncState.Failed }
            assertEquals(SyncState.Failed, requireNotNull(Asset.find(rows[0].id).current()).sync.state)
            rows[0].discard("create_files")
            assertEquals(listOf(null, "a1"), confirmedIds(receipt))
            assertEquals(2, transport.calls.count { it.operationName == "CreateBlob" })
            assertTrue(transport.calls.none { it.operationName == "CreateAssets" })
            val call = transport.calls.single { it.operationName == "CreateAsset" }
            assertEquals("key-1", Json.parseToJsonElement(call.variablesJson).jsonObject.getValue("clientKey").jsonPrimitive.content)
            closeForCleanup()
        } finally { closeForCleanup(); directory.deleteRecursively() }
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
            reopen(Config(null, null, 32u), modelsJSON, RefuseFirstFile(transport), operations)
            val items = files.mapIndexed { index, file -> UploadItem(values = buildJsonObject { put("clientKey", "key-$index"); put("size", 3); put("position", index) }, path = file.path) }
            val receipt = AccountScope.current().uploadBatch(Asset, items, UploadBatchRequest(operation = "create_files"))
            val rows = receipt.items.map { it.optimisticValue }
            eventually("the refused file failed") { Asset.find(rows[0].id).current()?.sync?.state == SyncState.Failed }
            assertEquals(SyncState.Failed, requireNotNull(Asset.find(rows[0].id).current()).sync.state)
            val retried = rows[0].retry("create_files")
            assertTrue(receipt.items[0].receipt.outcome() is OperationOutcome.Rejected)
            assertEquals("a0", retried.confirmed().id)
            assertEquals(listOf("a1", "a2"), confirmedIds(receipt).drop(1))
            val call = transport.calls.single { it.operationName == "CreateAssets" }
            val sent = Json.parseToJsonElement(call.variablesJson).jsonObject.getValue("items").jsonArray
            assertEquals(listOf("key-0", "key-1", "key-2"), sent.map { it.jsonObject.getValue("clientKey").jsonPrimitive.content })
            assertEquals(4, transport.calls.count { it.operationName == "CreateBlob" })
            val filenames = transport.calls.filter { it.operationName == "CreateBlob" }.map { Json.parseToJsonElement(it.variablesJson).jsonObject.getValue("filename").jsonPrimitive.content }.sorted()
            assertEquals(listOf("0.jpg", "0.jpg", "1.jpg", "2.jpg"), filenames)
            closeForCleanup()
        } finally { closeForCleanup(); directory.deleteRecursively() }
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
            reopen(Config(null, null, 32u), modelsJSON, transport, operations)
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
            closeForCleanup()
        } finally { held.release(); closeForCleanup(); directory.deleteRecursively() }
    }

    @OptIn(DBlayerTestingApi::class)
    @Test
    fun aFailedBlobExpiresWhenTheNetworkReturnsAfterSevenDays() = bounded {
        val source = Files.createTempFile("blob-expiry", ".mp4").toFile().apply { writeText("video") }
        try {
            closeForCleanup()
            val transport = MockTransport()
            val clock = reopenClocked(modelsJSON, transport, expiryOperations)
            val row = refusedUpload(transport, source)
            DBlayer.database.setOnline(false)
            clock.advance(7L * 24 * 60 * 60 * 1000 + 1)
            DBlayer.database.setOnline(true)
            eventually("the expired upload is discarded when the network returns") { clock.advance(1); isGone(row) }
            closeForCleanup()
        } finally { closeForCleanup(); source.delete() }
    }


    @Test
    fun completeFailureThenRetryKeepsTheConfirmedParts() = bounded {
        val directory = Files.createTempDirectory("multipart").toFile()
        val scope = CoroutineScope(SupervisorJob())
        try {
            val source = directory.resolve("video").apply { writeBytes(ByteArray(11 * 1024 * 1024)) }
            val transport = MultipartTransfer()
            transport.mock.enqueue("InitMultipart", """{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}""")
            enqueuePresigns(transport.mock)
            transport.mock.enqueue("CompleteMultipart", DbException.Http(422u, "rejected"))
            transport.mock.enqueue("CompleteMultipart", """{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}""")
            val operations = """[{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"},{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"""
            transport.mock.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m1","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
            reopen(Config(null, null, 8u), modelsJSON, transport, operations)
            val sent = AccountScope.current().send(Moment, buildJsonObject { put("text", "video") }, upload = UploadCommand("upload_clip", UploadRequest(source.path)))
            val row = requireNotNull(sent.optimisticValue)
            eventually("the refused complete fails the upload") { Moment.find(row.id).current()?.sync?.state == SyncState.Failed }
            val retried = row.retry("upload_clip")
            assertEquals("m1", retried.confirmed().id)
            assertEquals(SyncState.Synced, Moment.find("m1").current()?.sync?.state)
            val names = transport.mock.calls.map { it.operationName }
            assertEquals(1, names.count { it == "InitMultipart" })
            assertEquals(3, names.count { it == "PresignParts" })
            assertEquals(2, names.count { it == "CompleteMultipart" })
            assertEquals(listOf("https://storage/1", "https://storage/2", "https://storage/3"), transport.puts.sorted())
            closeForCleanup()
        } finally { closeForCleanup(); scope.cancel(); directory.deleteRecursively() }
    }

    @Test
    fun attachedFilesSurviveReopenAndDetachingOneKeepsTheOther() = bounded {
        val directory = Files.createTempDirectory("files").toFile()
        try {
            val config = Config(directory.resolve("files.sqlite").path, null, 32u)
            val members = """[{"name":"ContainerMembers","document":"query ContainerMembers(${'$'}containerId: ID!, ${'$'}first: Int, ${'$'}after: Cursor) { node(id: ${'$'}containerId) { ... on Container { membersCount members(first: ${'$'}first, after: ${'$'}after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"}]"""
            val transport = MockTransport()
            transport.enqueue("ContainerMembers", """{"data":{"node":{"membersCount":2,"members":{"edges":[{"cursor":"u1","node":{"id":"u1","name":"first"}},{"cursor":"u2","node":{"id":"u2","name":"second"}}],"pageInfo":{"hasNextPage":false,"endCursor":"u2"}}}}}""")
            reopen(config, modelsJSON, transport, members)
            User.containerMembers("c1").loadAll()
            val first = requireNotNull(User.find("u1").current())
            val second = requireNotNull(User.find("u2").current())
            val large = ByteArray(300 * 1024) { 9 }
            first.attach(User.avatar, byteArrayOf(1, 2, 3), "small", "image/png")
            second.attach(User.avatar, large, "large", "image/png")
            assertArrayEquals(byteArrayOf(1, 2, 3), first.read(User.avatar))
            assertArrayEquals(large, java.io.File(requireNotNull(second.url(User.avatar))).readBytes())
            first.attach(User.avatar, byteArrayOf(4), "replacement", "image/png")
            assertTrue(first.detach(User.avatar))
            val firstId = first.id
            val secondId = second.id
            closeForCleanup()
            reopen(config, modelsJSON, MockTransport(), members)
            val restoredFirst = requireNotNull(User.find(firstId).current())
            val restoredSecond = requireNotNull(User.find(secondId).current())
            assertNull(restoredFirst.read(User.avatar))
            assertArrayEquals(large, restoredSecond.read(User.avatar))
            val info = requireNotNull(restoredSecond.file(User.avatar))
            assertEquals("large", info.name)
            assertEquals(300L * 1024, info.size)
            closeForCleanup()
        } finally { closeForCleanup(); directory.deleteRecursively() }
    }

    @Test
    fun anOfflineDiscardRoutesItsReopenedAbortAsATransfer() = bounded {
        val directory = Files.createTempDirectory("transfer").toFile()
        val scope = CoroutineScope(SupervisorJob())
        val transport = MockTransport()
        transport.enqueue("InitMultipart", """{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}""")
        val held = transport.hold("PresignParts", reply = """{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}""")
        val abort = transport.hold("AbortMultipart", reply = """{"data":{"abortMultipart":{"ok":true}}}""")
        transport.enqueue("PresignParts", """{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}""")
        try {
            val source = directory.resolve("video").apply { writeBytes(ByteArray(11 * 1024 * 1024)) }
            val config = Config(directory.resolve("db.sqlite").path, null, 8u)
            val operations = """[{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"},{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"""
            transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m1","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
            val db = reopen(config, modelsJSON, transport, operations)
            val sent = AccountScope.current().send(Moment, buildJsonObject { put("text", "video") }, upload = UploadCommand("upload_clip", UploadRequest(source.path)))
            val row = requireNotNull(sent.optimisticValue)
            held.entered()
            val copies = directory.resolve("db.sqlite.files/transfers")
            fun copyCount() = copies.list()?.size ?: 0
            assertEquals(1, copyCount())
            db.setOnline(false)
            row.discard("upload_clip")
            assertEquals(0, copyCount())
            assertTrue(source.exists())
            held.release()
            closeForCleanup()
            val reopened = reopen(config, modelsJSON, transport, operations)
            reopened.setOnline(false)
            reopened.setOnline(true)
            abort.entered()
            abort.release()
            reopened.setOnline(false)
            reopened.setOnline(true)
            assertEquals("the saved pair of the held step is sent once more", 2, transport.calls.count { it.operationName == "PresignParts" })
            val aborts = transport.calls.filter { it.operationName == "AbortMultipart" }
            assertEquals(1, aborts.size)
            assertEquals(NetworkPurpose.TRANSFER, aborts.single().purpose)
            closeForCleanup()
        } finally {
            held.release()
            abort.release()
            closeForCleanup()
            scope.cancel()
            directory.deleteRecursively()
        }
    }
}

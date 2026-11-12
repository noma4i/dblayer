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
import kotlinx.coroutines.launch
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.CompletableDeferred
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
import org.junit.Assert.assertNotNull
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

private class MultipartTransfer(val mock: MockTransport = MockTransport()) : ScriptedTransport {
    override suspend fun query(request: TransportRequest): String = mock.query(request)
    override suspend fun mutate(request: TransportRequest): String = mock.mutate(request)
    override suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome = mock.scriptedCable(url, events)
    val puts = java.util.concurrent.CopyOnWriteArrayList<String>()
    val identities = java.util.concurrent.CopyOnWriteArrayList<TransferIdentity>()
    val paths = java.util.concurrent.CopyOnWriteArrayList<Pair<String, java.nio.file.Path>>()
    override suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply {
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

private class HoldSecondFile(private val server: SealedServer, private val mock: MockTransport = MockTransport()) : ScriptedTransport {
    override suspend fun query(request: TransportRequest): String = mock.query(request)
    override suspend fun mutate(request: TransportRequest): String = mock.mutate(request)
    override suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome = mock.scriptedCable(url, events)
    private val count = java.util.concurrent.atomic.AtomicInteger()
    val identities = java.util.Collections.synchronizedList(mutableListOf<TransferIdentity>())
    override suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply {
        identities.add(identity)
        if (count.incrementAndGet() == 1) server.hold("CreateBlob")
        progress.sent(file.length)
        return PutReply(200u, null)
    }
}

private const val REFUSED_BLOB = """{"errors":[{"message":"no storage","extensions":{"code":"FORBIDDEN"}}]}"""

private fun createdBlob(index: Int) = """{"data":{"blobCreate":{"url":"https://storage/file","headers":{},"signedId":"signed-$index","blobId":"b$index"}}}"""

private class RefuseFirstFile(private val mock: MockTransport) : ScriptedTransport {
    override suspend fun query(request: TransportRequest): String = mock.query(request)
    override suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply = mock.scriptedPut(url, headers, file, identity, progress)
    override suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome = mock.scriptedCable(url, events)
    private val refused = java.util.concurrent.atomic.AtomicBoolean(false)
    override suspend fun mutate(request: TransportRequest): String {
        val answer = mock.mutate(request)
        val file = Json.parseToJsonElement(request.variablesJson).jsonObject["filename"]?.jsonPrimitive?.content
        return if (request.operationName == "CreateBlob" && file == "0.jpg" && refused.compareAndSet(false, true)) REFUSED_BLOB else answer
    }
}

/** The item outcomes of a batch: each item has its own receipt. */
internal suspend fun <Model : DBModel> confirmedPairs(receipt: OperationReceipt<Model>): List<Pair<String, String>?> =
    receipt.items.map { item -> try { item.receipt.confirmed().value?.let { item.item.value to it.id } } catch (_: DbException.Rejected) { null } catch (_: DbException.Interrupted) { null } }

private suspend fun <Model : DBModel> confirmedIds(receipt: OperationReceipt<Model>): List<String?> = confirmedPairs(receipt).map { it?.second }


class TransferTest {

    @Test fun savedStepOnlineAtOpen() = bounded { shiftingABatch(listOf("reopen"), onlineAtOpen = true) }
    @Test fun savedStepOnlineAfterRegister() = bounded { shiftingABatch(listOf("reopen"), onlineAtOpen = false) }
    @Test fun orphanAbortOnlineAtOpen() = bounded { abortOfADiscardedPhotoAfterReopen(onlineAtOpen = true) }
    @Test fun orphanAbortOnlineAfterRegister() = bounded { abortOfADiscardedPhotoAfterReopen(onlineAtOpen = false) }
    @Test fun savedStepAfterReaderOpens() = bounded { shiftingABatch(listOf("reopen"), onlineAtOpen = true, holdReplay = true) }

    @OptIn(DBlayerTestingApi::class)
    private suspend fun openStand(transport: HoldSecondFile, path: String, encrypted: EncryptedConfig, credential: TransferCredential, server: SealedServer, operations: String, onlineAtOpen: Boolean) {
        closeForCleanup()
        DBlayer.openTesting(transport, modelsJSON, path = path, encrypted = encrypted, credentials = credential, http = server, observers = if (onlineAtOpen) null else SilentPlatform())
        server.registerDocuments(operations)
        registerStandOperations(DBlayer.database, operations)
        if (!onlineAtOpen) DBlayer.database.setOnline(true)
    }

    private suspend fun shiftingABatch(cases: List<String>, onlineAtOpen: Boolean, holdReplay: Boolean = false) {
        val directory = Files.createTempDirectory("encrypted-album").toFile()
        try {
            for (case in cases) {
                val lose = case != "live"
                val server = SealedServer { path, body ->
                    when {
                        path == "/api/auth/login_link" -> Json.parseToJsonElement("""{"sessionId":"A"}""").jsonObject
                        body["operationName"]?.jsonPrimitive?.content == "CreateBlob" -> {
                            val name = body.getValue("variables").jsonObject.getValue("filename").jsonPrimitive.content
                            Json.parseToJsonElement("""{"data":{"blobCreate":{"url":"https://storage/file","headers":{},"signedId":"signed-$name","blobId":"blob-$name"}}}""").jsonObject
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
                    openStand(transport, path, encrypted, credential, server, operations, onlineAtOpen)
                    DBlayer.database.setOnline(true)
                    DBlayer.database.sessionCall("login_link", """{"account":"A"}""")
                    val files = (0..2).map { index -> directory.resolve("$index.jpg").apply { writeText("image-$index") } }
                    val items = files.mapIndexed { index, file -> UploadItem(values = buildJsonObject { put("clientKey", "key-$index"); put("size", 3); put("position", index) }, path = file.path) }
                    val receipt = AccountScope.current().uploadBatch(Asset, items, UploadBatchRequest(operation = "create_files"))
                    val rows = receipt.items.map { it.optimisticValue }
                    awaitWithin("the held request reaches the server") { server.heldRequest() }
                    awaitWithin("the discard of the first file") { rows[0].discard("create_files") }
                    if (lose) { DBlayer.database.setOnline(false); awaitWithin("the held request is dropped") { server.heldDropped() } }
                    server.hold(null)
                    if (lose) {
                        if (holdReplay) server.holdStored("CreateBlob")
                        openStand(transport, path, encrypted, credential, server, operations, onlineAtOpen)
                        eventually("the lost step is requested again") { server.requests.map { it.second }.count { it["operationName"]?.jsonPrimitive?.content == "CreateBlob" && it.getValue("variables").jsonObject.getValue("filename").jsonPrimitive.content == "1.jpg" } == 2 }
                        if (holdReplay) awaitWithin("the stored answer is held") { server.storedRequest() }
                        awaitAssets(server, transport, holdReplay)
                        assertEquals("key-1", Asset.find("a1").current()?.clientKey)
                        assertEquals("key-2", Asset.find("a2").current()?.clientKey)
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
                } finally { server.releaseStored(); server.hold(null); closeForCleanup() }
            }
        } finally { closeForCleanup(); directory.deleteRecursively() }
    }

    private suspend fun awaitAssets(server: SealedServer, transport: HoldSecondFile, holdReplay: Boolean) = coroutineScope {
        val latestRows = java.util.concurrent.atomic.AtomicReference("no list value")
        val latestOperations = java.util.concurrent.atomic.AtomicReference("no operations value")
        val directRows = java.util.concurrent.atomic.AtomicReference("not read after commit")
        val opened = CompletableDeferred<Unit>()
        val queueEmpty = CompletableDeferred<Unit>()
        var sawOperation = false
        val operations = launch {
            AccountScope.current().operations().collect { value ->
                latestOperations.set(value.toString())
                if (value is OperationQueueItem.Queue) {
                    if (value.items.isNotEmpty()) sawOperation = true
                    if (sawOperation && value.items.isEmpty()) queueEmpty.complete(Unit)
                }
            }
        }
        val direct = launch {
            queueEmpty.await()
            directRows.set(listOf(Asset.find("a1").current(), Asset.find("a2").current()).toString())
        }
        val rows = async {
            Asset.ids(listOf("a1", "a2")).live.flow.first { value ->
                latestRows.set(value.toString())
                opened.complete(Unit)
                value.map { it.id }.toSet() == setOf("a1", "a2")
            }
        }
        try {
            if (holdReplay) {
                awaitWithin("the asset reader opens") { opened.await() }
                server.releaseStored()
            }
            awaitWithin("both assets are stored", {
                "operations=${latestOperations.get()} rows=${latestRows.get()} readAfterCommit=${directRows.get()} requests=${server.requests} parked=${server.parkedCount} heldStored=$holdReplay puts=${transport.identities}"
            }) { rows.await() }
        } finally { rows.cancel(); direct.cancel(); operations.cancel() }
    }

    private val expiryOperations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"}]"""

    private suspend fun refusedUpload(transport: MockTransport, source: java.io.File): Moment {
        transport.enqueue("CreateBlob", DbException.Http(422u, "rejected"))
        val sent = AccountScope.current().send(Moment, buildJsonObject { put("text", "video") }, upload = UploadCommand("upload_clip", UploadRequest(source.path)))
        val row = requireNotNull(sent.optimisticValue)
        Moment.ids(listOf(row.id)).rowsReach("the refused upload failed") { it.firstOrNull()?.sync?.state == SyncState.Failed }
        assertNotNull(requireNotNull(Moment.find(row.id).current()).sync.error)
        return row
    }


    private suspend fun abortOfADiscardedPhotoAfterReopen(onlineAtOpen: Boolean) {
        val directory = Files.createTempDirectory("photo").toFile()
        val transport = MockTransport()
        transport.enqueue("InitMultipart", """{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}""")
        val held = transport.hold("PresignParts", reply = """{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}""")
        val abort = transport.hold("AbortMultipart", reply = """{"data":{"abortMultipart":{"ok":true}}}""")
        transport.enqueue("PresignParts", """{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}""")
        try {
            val source = directory.resolve("photo").apply { writeBytes(ByteArray(11 * 1024 * 1024)) }
            val config = Config(directory.resolve("db.sqlite").path, null)
            val operations = """[{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"""
            val db = openSigned(config, modelsJSON, transport, operations, onlineAtOpen)
            val queued = requireNotNull(AccountScope.current().send(Moment, buildJsonObject { put("text", "group") }, upload = UploadCommand("upload_clip", UploadRequest(source.path))).optimisticValue)
            held.entered()
            db.setOnline(false)
            queued.discard(null)
            assertNull(Moment.find(queued.id).current())
            held.release()
            closeForCleanup()
            openSigned(config, modelsJSON, transport, operations, onlineAtOpen)
            awaitWithin("the saved abort reached the transport after the network returned", { "calls=" + transport.calls.map { it.operationName } }) { abort.entered() }
            abort.release()
            eventually("the saved pair of the held step is sent once more") { transport.calls.count { it.operationName == "PresignParts" } == 2 }
            val aborts = transport.calls.filter { it.operationName == "AbortMultipart" }
            assertEquals(1, aborts.size)
            assertEquals(NetworkPurpose.TRANSFER, aborts.single().purpose)
            closeForCleanup()
        } finally { held.release(); abort.release(); closeForCleanup(); directory.deleteRecursively() }
    }

}

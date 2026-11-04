package com.noma4i.dblayer

import android.app.job.JobScheduler
import com.noma4i.dblayer.models.Asset
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import java.nio.file.Files
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.selects.select
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.robolectric.Robolectric
import org.robolectric.Shadows.shadowOf

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class UploadWorkTest {
    private val notification = UploadNotification("Uploading", "Files are on their way", 1)

    @Test
    fun theApiLevelPicksTheUserInitiatedJobFromApi34AndTheForegroundServiceBelow() {
        assertEquals(UploadMode.ForegroundService, uploadMode(33))
        assertEquals(UploadMode.UserInitiatedJob, uploadMode(34))
        assertEquals(UploadMode.UserInitiatedJob, uploadMode(36))
    }

    @Test
    fun theJobIsUserInitiatedNeedsANetworkAndIsNotPersisted() {
        val context = org.robolectric.RuntimeEnvironment.getApplication()
        assertNull(UploadJobService.schedule(context))
        val job = context.getSystemService(JobScheduler::class.java).allPendingJobs.single()
        assertTrue(job.isUserInitiated)
        assertTrue(job.networkType != android.app.job.JobInfo.NETWORK_TYPE_NONE)
        assertTrue(!job.isPersisted)
        assertNull("a pending job is kept instead of replaced", UploadJobService.schedule(context))
        assertEquals(1, context.getSystemService(JobScheduler::class.java).allPendingJobs.size)
    }

    @Test
    fun onlyUploadRequestsGoThroughTheNetworkOfTheRunningJob() = bounded {
        MockWebServer().use { server ->
            repeat(2) { server.enqueue(MockResponse().setResponseCode(200).setBody("""{"data":{}}""")) }
            server.start()
            val transport = OkHttpTransport(server.url("/graphql").toString())
            val oldOwner = UploadJobToken()
            val owner = UploadJobToken()
            UploadNetwork.started(null, oldOwner)
            UploadNetwork.started(null, owner)
            UploadNetwork.ended(oldOwner)
            try {
                assertEquals("""{"data":{}}""", transport.query(TransportRequest("query Probe { id }", "Probe", "{}", NetworkPurpose.STANDARD)))
                refused { transport.query(TransportRequest("mutation Probe { id }", "Probe", "{}", NetworkPurpose.TRANSFER)) }
                refused { OkHttpEncryptedHttp(server.url("/").toString().trimEnd('/')).post("/api/graphql", "a.b.c.d.e", NetworkPurpose.TRANSFER) }
            } finally {
                UploadNetwork.ended(owner)
            }
            assertEquals("""{"data":{}}""", transport.mutate(TransportRequest("mutation Probe { id }", "Probe", "{}", NetworkPurpose.TRANSFER)))
        }
    }

    private suspend fun refused(block: suspend () -> Unit) {
        val failure = runCatching { block() }.exceptionOrNull()
        assertTrue("expected a network failure, got $failure", failure is DbException.Network)
    }

    @Test
    fun aRefusedBackgroundStartIsKeptOnTheAcceptedBatchOnly() = bounded {
        val directory = Files.createTempDirectory("accepted").toFile()
        try {
            val source = java.io.File(directory, "image.png").apply { writeText("image") }
            val database = reopen(Config(null, null, 32u), modelsJSON, MockTransport(), operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"}]""", uploads = UploadOperations(createBlob = UploadOperation("CreateBlob")))
            database.setOnline(false)
            var starts = 0
            database.setUploadAcceptedSink(UploadAcceptedStart(org.robolectric.RuntimeEnvironment.getApplication(), database, UploadScheduler { _, _ -> if (starts++ == 0) "the app was not visible" else null }))
            fun batch(keys: IntRange) = keys.map { UploadItem(values = buildJsonObject { put("clientKey", "key-$it"); put("size", 2) }, path = source.path) }
            val rows = AccountScope.current().uploadBatch(Asset, batch(0..1), UploadBatchRequest(operation = "create_files"))
            val other = AccountScope.current().uploadBatch(Asset, batch(8..9), UploadBatchRequest(operation = "create_files"))
            eventually("every member of the refused batch shows the reason") {
                rows.items.all { Asset.find(it.optimisticValue.id).current()?.uploads?.values?.singleOrNull()?.error?.message?.contains("the app was not visible") == true }
            }
            assertNull(Asset.find(other.items.first().optimisticValue.id).current()!!.uploads.values.single().error)
        } finally {
            DBlayer.close()
            directory.deleteRecursively()
        }
    }

    @Test
    fun aLateBackgroundRefusalDoesNotChangeTheNextSession() = bounded {
        val directory = Files.createTempDirectory("accepted-epoch").toFile()
        try {
            val source = java.io.File(directory, "image.png").apply { writeText("image") }
            val transport = MockTransport()
            transport.enqueue("UserProfile", """{"data":{"me":{"__typename":"User","id":"B","name":"B"}}}""")
            val database = reopen(Config(null, null, 32u), modelsJSON, transport, operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"},{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]""", uploads = UploadOperations(createBlob = UploadOperation("CreateBlob")))
            database.setOnline(false)
            val accepted = mutableListOf<WireUploadAccepted>()
            database.setUploadAcceptedSink(object : UploadAcceptedSink { override fun accepted(event: WireUploadAccepted) { accepted.add(event) } })
            fun batch(prefix: String) = (0..1).map { UploadItem(values = buildJsonObject { put("clientKey", "$prefix-$it"); put("size", 2) }, path = source.path) }
            val firstEpoch = database.sessionEpoch()
            val owner = requireNotNull(database.roundBegin(firstEpoch))
            assertEquals(WireRoundStep.DONE, database.roundNext(firstEpoch, owner))
            AccountScope.current().uploadBatch(Asset, batch("old"), UploadBatchRequest(operation = "create_files"))
            val old = accepted.single()
            DBlayer.sessionReset()
            database.setOnline(true)
            User.profile.load(AccountScope.current())
            database.setOnline(false)
            val rows = AccountScope.current().uploadBatch(Asset, batch("new"), UploadBatchRequest(operation = "create_files"))
            val current = accepted.last()
            val stale = runCatching { database.backgroundRefused(old.epoch, current.items, "old refusal") }.exceptionOrNull()
            assertTrue("the old epoch must be rejected", stale is DbException.Interrupted)
            val newOwner = requireNotNull(database.roundBegin(database.sessionEpoch()))
            val late = requireNotNull(database.roundEnd(database.sessionEpoch(), owner))
            assertEquals(listOf(old.epoch, current.epoch), late.map { it.epoch })
            assertEquals(WireRoundStep.DONE, database.roundNext(database.sessionEpoch(), newOwner))
            database.roundEnd(database.sessionEpoch(), newOwner)
            val deliver = UploadAcceptedStart(org.robolectric.RuntimeEnvironment.getApplication(), database, UploadScheduler { _, items -> if (items == old.items) "old refusal" else null })
            late.forEach(deliver::accepted)
            assertTrue(rows.all { Asset.find(it.id).current()?.uploads?.values?.singleOrNull()?.error == null })
        } finally {
            DBlayer.close()
            directory.deleteRecursively()
        }
    }

    @Test
    fun aNewSessionSchedulesUploadsAfterTheOldRoundWasRunning() = bounded {
        val directory = Files.createTempDirectory("round-epoch").toFile()
        try {
            val source = java.io.File(directory, "image.png").apply { writeText("image") }
            val transport = MockTransport()
            transport.enqueue("UserProfile", """{"data":{"me":{"__typename":"User","id":"B","name":"B"}}}""")
            val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"},{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"""
            val database = reopen(Config(null, null, 32u), modelsJSON, transport, operations, UploadOperations(createBlob = UploadOperation("CreateBlob")))
            database.setOnline(false)
            val starts = java.util.concurrent.atomic.AtomicInteger()
            database.setUploadAcceptedSink(UploadAcceptedStart(org.robolectric.RuntimeEnvironment.getApplication(), database, UploadScheduler { _, _ -> starts.incrementAndGet(); null }))
            fun batch(prefix: String) = (0..1).map { UploadItem(values = buildJsonObject { put("clientKey", "$prefix-$it"); put("size", 2) }, path = source.path) }
            AccountScope.current().uploadBatch(Asset, batch("old"), UploadBatchRequest(operation = "create_files"))
            requireNotNull(database.roundBegin(database.sessionEpoch()))
            DBlayer.sessionReset()
            database.setOnline(true)
            User.profile.load(AccountScope.current())
            database.setOnline(false)
            AccountScope.current().uploadBatch(Asset, batch("new"), UploadBatchRequest(operation = "create_files"))
            assertEquals("the new session starts its own job", 2, starts.get())
        } finally {
            DBlayer.close()
            directory.deleteRecursively()
        }
    }

    @Test
    fun aNewSessionSchedulesUploadsWhenTheOldEndingRoundLostItsSession() = bounded {
        val directory = Files.createTempDirectory("ending-epoch").toFile()
        try {
            val source = java.io.File(directory, "image.png").apply { writeText("image") }
            val transport = MockTransport()
            transport.enqueue("UserProfile", """{"data":{"me":{"__typename":"User","id":"B","name":"B"}}}""")
            val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"},{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"""
            val database = reopen(Config(null, null, 32u), modelsJSON, transport, operations, UploadOperations(createBlob = UploadOperation("CreateBlob")))
            database.setOnline(false)
            val starts = java.util.concurrent.atomic.AtomicInteger()
            database.setUploadAcceptedSink(UploadAcceptedStart(org.robolectric.RuntimeEnvironment.getApplication(), database, UploadScheduler { _, _ -> starts.incrementAndGet(); null }))
            fun batch(prefix: String) = (0..1).map { UploadItem(values = buildJsonObject { put("clientKey", "$prefix-$it"); put("size", 2) }, path = source.path) }
            AccountScope.current().uploadBatch(Asset, batch("old"), UploadBatchRequest(operation = "create_files"))
            val owner = requireNotNull(database.roundBegin(database.sessionEpoch()))
            assertEquals(WireRoundStep.DONE, database.roundNext(database.sessionEpoch(), owner))
            DBlayer.sessionReset()
            assertTrue(runCatching { database.roundEnd(database.sessionEpoch(), owner) }.exceptionOrNull() is DbException.NoSession)
            database.setOnline(true)
            User.profile.load(AccountScope.current())
            database.setOnline(false)
            AccountScope.current().uploadBatch(Asset, batch("new"), UploadBatchRequest(operation = "create_files"))
            assertEquals(2, starts.get())
            val next = requireNotNull(database.roundBegin(database.sessionEpoch()))
            assertEquals(WireRoundStep.DONE, database.roundNext(database.sessionEpoch(), next))
            database.roundEnd(database.sessionEpoch(), next)
        } finally {
            DBlayer.close()
            directory.deleteRecursively()
        }
    }

    @Test
    fun aSchedulerRefusalReturningAfterResetDoesNotChangeTheNewUpload() = bounded {
        val directory = Files.createTempDirectory("late-refusal").toFile()
        val release = java.util.concurrent.CountDownLatch(1)
        try {
            val source = java.io.File(directory, "image.png").apply { writeText("image") }
            val transport = MockTransport()
            transport.enqueue("UserProfile", """{"data":{"me":{"__typename":"User","id":"B","name":"B"}}}""")
            val operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"},{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"""
            val database = reopen(Config(null, null, 32u), modelsJSON, transport, operations, UploadOperations(createBlob = UploadOperation("CreateBlob")))
            database.setOnline(false)
            val entered = CompletableDeferred<Unit>()
            val starts = java.util.concurrent.atomic.AtomicInteger()
            val timedOut = java.util.concurrent.atomic.AtomicBoolean()
            database.setUploadAcceptedSink(UploadAcceptedStart(org.robolectric.RuntimeEnvironment.getApplication(), database, UploadScheduler { _, _ ->
                if (starts.incrementAndGet() == 1) {
                    entered.complete(Unit)
                    if (!release.await(10, java.util.concurrent.TimeUnit.SECONDS)) timedOut.set(true)
                    "old refusal"
                } else null
            }))
            fun batch(prefix: String) = (0..1).map { UploadItem(values = buildJsonObject { put("clientKey", "$prefix-$it"); put("size", 2) }, path = source.path) }
            val old = async(Dispatchers.Default) { runCatching { AccountScope.current().uploadBatch(Asset, batch("old"), UploadBatchRequest(operation = "create_files")) } }
            select<Unit> {
                entered.onAwait { }
                old.onAwait { throw AssertionError("the upload returned before the scheduler started: $it") }
            }
            DBlayer.sessionReset()
            database.setOnline(true)
            User.profile.load(AccountScope.current())
            database.setOnline(false)
            val rows = AccountScope.current().uploadBatch(Asset, batch("new"), UploadBatchRequest(operation = "create_files"))
            release.countDown()
            old.await()
            assertTrue("the held scheduler was released by the test", !timedOut.get())
            assertEquals(2, starts.get())
            assertTrue(rows.all { Asset.find(it.id).current()?.uploads?.values?.singleOrNull()?.error == null })
        } finally {
            release.countDown()
            DBlayer.close()
            directory.deleteRecursively()
        }
    }

    @Test
    fun theUploadServiceShowsItsNotificationBeforeOpenFinishedAndStopsWhenNothingIsUnfinished() = bounded {
        DBlayer.close()
        val context = org.robolectric.RuntimeEnvironment.getApplication()
        UploadNotifications.store(context, notification)
        val controller = Robolectric.buildService(UploadService::class.java).create()
        controller.startCommand(0, 1)
        assertNotNull("the foreground notification is shown before open ran", shadowOf(controller.get()).lastForegroundNotification)
        assertTrue(!shadowOf(controller.get()).isStoppedBySelf)
        DBlayer.open(context, transport = MockTransport(), modelsJson = modelsJSON, uploadNotification = notification)
        eventually("the service stops by itself when no upload is unfinished") { shadowOf(controller.get()).isStoppedBySelf }
        DBlayer.close()
    }
}

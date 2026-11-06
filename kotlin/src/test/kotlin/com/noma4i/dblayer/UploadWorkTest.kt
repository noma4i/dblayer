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
        assertNull(UploadJobService.schedule(context, 1uL, 1uL))
        val job = context.getSystemService(JobScheduler::class.java).allPendingJobs.single()
        assertTrue(job.isUserInitiated)
        assertTrue(job.networkType != android.app.job.JobInfo.NETWORK_TYPE_NONE)
        assertTrue(!job.isPersisted)
        assertNull("a pending job is kept instead of replaced", UploadJobService.schedule(context, 2uL, 1uL))
        assertEquals(1, context.getSystemService(JobScheduler::class.java).allPendingJobs.size)
    }

    @Test
    fun onlyUploadRequestsGoThroughTheNetworkOfTheRunningJob() = bounded {
        val old = UploadJobToken()
        val current = UploadJobToken()
        UploadNetwork.started(null, old)
        UploadNetwork.started(null, current)
        try {
            UploadNetwork.ended(old)
            MockWebServer().use { server ->
                repeat(2) { server.enqueue(MockResponse().setResponseCode(200).setBody("""{"data":{}}""")) }
                server.start()
                val transport = OkHttpTransport(server.url("/graphql").toString())
                assertEquals("""{"data":{}}""", transport.query(TransportRequest("query Probe { id }", "Probe", "{}", NetworkPurpose.STANDARD)))
                refused { transport.query(TransportRequest("mutation Probe { id }", "Probe", "{}", NetworkPurpose.TRANSFER)) }
                refused { OkHttpEncryptedHttp(server.url("/").toString().trimEnd('/')).post("/api/graphql", "a.b.c.d.e", NetworkPurpose.TRANSFER) }
                UploadNetwork.ended(current)
                assertEquals("""{"data":{}}""", transport.mutate(TransportRequest("mutation Probe { id }", "Probe", "{}", NetworkPurpose.TRANSFER)))
            }
        } finally {
            UploadNetwork.ended(current)
        }
    }

    private suspend fun refused(block: suspend () -> Unit) {
        val failure = runCatching { block() }.exceptionOrNull()
        assertTrue("expected a network failure, got $failure", failure is DbException.Network)
    }

    @Test
    fun aRefusedBackgroundStartIsKeptOnTheItemsOfTheRound() = bounded {
        val directory = Files.createTempDirectory("accepted").toFile()
        try {
            val source = java.io.File(directory, "image.png").apply { writeText("image") }
            val database = reopen(Config(null, null, 32u), modelsJSON, MockTransport(), operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"}]""", uploads = UploadOperations(createBlob = UploadOperation("CreateBlob")))
            database.setOnline(false)
            database.setRoundJobs(UploadRounds(org.robolectric.RuntimeEnvironment.getApplication(), database, UploadScheduler { _, _, _ -> "the app was not visible" }))
            fun batch(keys: IntRange) = keys.map { UploadItem(values = buildJsonObject { put("clientKey", "key-$it"); put("size", 2) }, path = source.path) }
            val rows = AccountScope.current().uploadBatch(Asset, batch(0..1), UploadBatchRequest(operation = "create_files"))
            eventually("every member of the refused batch shows the reason") {
                rows.items.all { Asset.find(it.optimisticValue.id).current()?.uploads?.values?.singleOrNull()?.error?.message?.contains("the app was not visible") == true }
            }
        } finally {
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

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
        val round = RoundJob(7uL, 3u, 9uL)
        assertNull(UploadJobService.schedule(context, round))
        val job = context.getSystemService(JobScheduler::class.java).allPendingJobs.single()
        assertTrue(job.isUserInitiated)
        assertTrue(job.networkType != android.app.job.JobInfo.NETWORK_TYPE_NONE)
        assertTrue(!job.isPersisted)
        assertEquals("the system job carries the round value of the core", round, roundJobOf(job.extras))
    }

    @Test
    fun onlyUploadRequestsGoThroughTheNetworkOfTheRunningJob() = bounded {
        UploadNetwork.started(null)
        try {
            MockWebServer().use { server ->
                repeat(2) { server.enqueue(MockResponse().setResponseCode(200).setBody("""{"data":{}}""")) }
                server.start()
                val transport = OkHttpTransport()
                val graphql = probeRequest(server.url("/graphql").toString(), "{}")
                assertEquals(RawOutcome.Response(200u, null, """{"data":{}}"""), transport.send(graphql))
                refused { transport.send(graphql.copy(purpose = NetworkPurpose.TRANSFER)) }
                refused { OkHttpTransport().send(probeRequest(server.url("/api/graphql").toString(), "a.b.c.d.e").copy(purpose = NetworkPurpose.TRANSFER)) }
                UploadNetwork.ended()
                assertEquals(RawOutcome.Response(200u, null, """{"data":{}}"""), transport.send(graphql.copy(purpose = NetworkPurpose.TRANSFER)))
            }
        } finally {
            UploadNetwork.ended()
        }
    }

    private suspend fun refused(block: suspend () -> RawOutcome) {
        val outcome = block()
        assertTrue("expected a failed outcome, got $outcome", outcome.rawFailure() != null)
    }

    @Test
    fun aRefusedBackgroundStartIsKeptOnTheItemsOfTheRound() = bounded {
        val directory = Files.createTempDirectory("accepted").toFile()
        try {
            val source = java.io.File(directory, "image.png").apply { writeText("image") }
            val database = reopen(Config(null, null), modelsJSON, MockTransport(), operations = """[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"}]""")
            database.setOnline(false)
            database.setRoundJobs(UploadRounds(org.robolectric.RuntimeEnvironment.getApplication(), database, UploadScheduler { _, _ -> Refusal(RoundRefusal.JOB_REFUSED, "") }))
            fun batch(keys: IntRange) = keys.map { UploadItem(values = buildJsonObject { put("clientKey", "key-$it"); put("size", 2) }, path = source.path) }
            val rows = AccountScope.current().uploadBatch(Asset, batch(0..1), UploadBatchRequest(operation = "create_files"))
            eventually("every member of the refused batch shows the reason") {
                rows.items.all { Asset.find(it.optimisticValue.id).current()?.uploads?.values?.singleOrNull()?.error?.message?.contains("while the app was not visible") == true }
            }
        } finally {
            closeForCleanup()
            directory.deleteRecursively()
        }
    }

    @Test
    fun theUploadServiceShowsItsNotificationBeforeOpenFinishedAndStopsWhenNothingIsUnfinished() = bounded {
        closeForCleanup()
        val context = org.robolectric.RuntimeEnvironment.getApplication()
        UploadNotifications.store(context, notification)
        val controller = Robolectric.buildService(UploadService::class.java).create()
        controller.withIntent(android.content.Intent(context, UploadService::class.java).putExtra("request", 1L).putExtra("round", 1L).putExtra("epoch", 1L)).startCommand(0, 1)
        assertNotNull("the foreground notification is shown before open ran", shadowOf(controller.get()).lastForegroundNotification)
        assertTrue(!shadowOf(controller.get()).isStoppedBySelf)
        try {
            DBlayer.open(context, endpoint = SCRIPTED_ENDPOINT, transport = MockTransport(), modelsJson = modelsJSON, operations = UPLOAD_STEP_OPERATIONS.map { (name, text) -> object : GraphQLOperation { override val operationName = name; override val document = text } }, uploadNotification = notification)
            eventuallyOs("the service stops by itself when no upload is unfinished") { shadowOf(controller.get()).isStoppedBySelf }
        } finally {
            closeForCleanup()
        }
    }
}

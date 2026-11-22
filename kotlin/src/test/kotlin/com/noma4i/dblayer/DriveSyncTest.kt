package com.noma4i.dblayer

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.MutableStateFlow
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okio.Buffer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class DriveSyncTest {
    @Test
    fun commitsTheDriveCursorOnlyAfterTheJournalIsAccepted() = bounded(timeoutMillis = 8_000) {
        val directory = Files.createTempDirectory("drive-sync").toFile()
        try {
            MockWebServer().use { server ->
                val domain = "a".repeat(64)
                val record = DriveJournalEntry("b".repeat(64), "opaque body".toByteArray())
                val journal = File(directory, "provider-journal")
                DriveJournal.prepare(null, journal, listOf(record))
                val grant = MutableStateFlow<DriveAuthorization>(DriveAuthorization.Available("account", "test-bearer"))
                val port = DriveSync(grant, directory, "local", DriveChanges(baseUrl = server.url("/drive/v3/")))
                val frames = mutableListOf<CloudFetched>()
                var refusing = true
                val sink = object : CloudSignals(NoHandle) {
                    override fun account(account: CloudAccount) = Unit
                    override suspend fun fetched(fetched: CloudFetched) {
                        frames.add(fetched)
                        if (refusing) throw IOException("store refused the journal")
                    }
                }
                val fingerprint = "9af211329b2fc82e5efe906062c730082819b23fe8394bc435e0b1bf0458eb54"
                val first = CloudWireScope(1uL, 1uL, domain, fingerprint)
                val second = first.copy(generation = 2uL)
                val snapshot = """{"files":[{"id":"journal","appProperties":{"domain":"$domain","replica":"other"}}]}"""
                fun enqueueSnapshot() {
                    server.enqueue(MockResponse().setBody("""{"startPageToken":"before"}"""))
                    server.enqueue(MockResponse().setBody(snapshot))
                    server.enqueue(MockResponse().setBody(Buffer().write(journal.readBytes())))
                }
                try {
                    port.observe(sink)
                    assertTrue(runCatching { port.start(first.copy(providerFingerprint = "0".repeat(64)), null, sink) }.isFailure)
                    assertEquals(0, server.requestCount)
                    enqueueSnapshot()
                    assertTrue(runCatching { port.start(first, null, sink) }.isFailure)
                    assertTrue(frames.none { it.progress != null || it.initialComplete })
                    assertEquals(3, server.requestCount)
                    repeat(3) { requireNotNull(server.takeRequest(1, TimeUnit.SECONDS)) }

                    refusing = false
                    frames.clear()
                    enqueueSnapshot()
                    server.enqueue(MockResponse().setBody("""{"newStartPageToken":"after","changes":[]}"""))
                    port.start(second, null, sink)
                    assertEquals(record.key, frames.first().records.single().key)
                    assertEquals("after", frames.last().progress?.decodeToString())
                    assertTrue(frames.last().initialComplete)
                    val requests = List(4) { requireNotNull(server.takeRequest(1, TimeUnit.SECONDS)) }
                    assertEquals("before", requests.last().requestUrl?.queryParameter("pageToken"))
                } finally {
                    port.stop(2uL)
                    port.unobserve()
                }
                assertEquals(listOf(journal.name), directory.list().orEmpty().toList())
            }
        } finally {
            directory.deleteRecursively()
        }
    }
}

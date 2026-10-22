package com.noma4i.dblayer

import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test

private fun blob(key: String, size: Int = 8) = CloudBlob(key, ByteArray(size) { (it * 37 + key.length + 200).toByte() })

class DriveCloudTest {
    private val drive = FakeDrive()
    private val server = MockWebServer()
    private lateinit var cloud: DriveCloud

    @Before
    fun start() {
        server.dispatcher = drive
        server.start()
        cloud = DriveCloud(DriveApi(OkHttpClient(), { "token" }, server.url("/")), { "token" })
    }

    @After
    fun stop() = server.shutdown()

    @Test
    fun aPushWritesOneFilePerBlobNamedByItsKeyIntoTheAppDataFolder() = bounded {
        cloud.push(listOf(blob("k1"), blob("k2")))
        assertEquals(listOf("k1", "k2"), drive.names)
        val create = server.takeRequest()
        assertEquals("Bearer token", create.getHeader("Authorization"))
        assertTrue(create.body.readUtf8().contains("\"parents\":[\"appDataFolder\"]"))
    }

    @Test
    fun blobsComeBackWithTheSameKeysAndBytes() = bounded {
        val sent = listOf(blob("k1"), blob("k2", 0), blob("k3", 5000))
        cloud.push(sent)
        val page = cloud.pull(null, 10u)
        assertEquals(sent.map { it.key }, page.blobs.map { it.key })
        assertTrue(sent.zip(page.blobs).all { (a, b) -> a.bytes.contentEquals(b.bytes) })
    }

    @Test
    fun theFirstPullReadsEveryFileThenTheCursorFollowsTheChangeLog() = bounded {
        cloud.push(listOf(blob("a")))
        val first = cloud.pull(null, 10u)
        assertEquals(listOf("a"), first.blobs.map { it.key })
        assertEquals(false, first.more)
        val idle = cloud.pull(first.cursor, 10u)
        assertEquals(emptyList<CloudBlob>(), idle.blobs)
        cloud.push(listOf(blob("b")))
        val later = cloud.pull(idle.cursor, 10u)
        assertEquals(listOf("b"), later.blobs.map { it.key })
    }

    @Test
    fun pagesReportMoreUntilTheListIsDone() = bounded {
        cloud.push(listOf(blob("v1"), blob("v2"), blob("v3")))
        val one = cloud.pull(null, 2u)
        assertEquals(2, one.blobs.size)
        assertEquals(true, one.more)
        val two = cloud.pull(one.cursor, 2u)
        assertEquals(1, two.blobs.size)
        assertEquals(false, two.more)
    }

    @Test
    fun httpFailuresBecomeCloudErrors() = bounded {
        val table = listOf(
            MockResponse().setResponseCode(401) to CloudException.NeedsAuthorization::class,
            MockResponse().setResponseCode(429).setHeader("Retry-After", "2") to CloudException.Throttled::class,
            MockResponse().setResponseCode(503) to CloudException.Unavailable::class,
            MockResponse().setResponseCode(403).setBody("""{"error":{"errors":[{"reason":"storageQuotaExceeded"}]}}""") to CloudException.QuotaExceeded::class,
        )
        for ((reply, expected) in table) {
            drive.failNext = reply
            val error = runCatching { cloud.pull(null, 10u) }.exceptionOrNull()
            assertEquals(expected, error?.let { it::class })
        }
    }

    @Test
    fun aDownServerIsOffline() = bounded {
        server.shutdown()
        val error = runCatching { cloud.pull(null, 10u) }.exceptionOrNull()
        assertEquals(CloudException.Offline::class, error?.let { it::class })
    }

    @Test
    fun aMissingConsentIsReportedByTheAccount() = bounded {
        val waiting = DriveCloud(DriveApi(OkHttpClient(), { throw CloudException.NeedsAuthorization("consent") }, server.url("/")), { throw CloudException.NeedsAuthorization("consent") })
        assertEquals(CloudAccount.NEEDS_CONSENT, waiting.account())
        assertEquals(CloudAccount.AVAILABLE, cloud.account())
    }
}

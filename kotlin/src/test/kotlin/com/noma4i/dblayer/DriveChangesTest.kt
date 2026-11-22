package com.noma4i.dblayer

import java.util.concurrent.TimeUnit
import java.nio.file.Files
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class DriveChangesTest {
    @Test
    fun readsOpaqueCursorInTheAppDataSpace() = bounded(timeoutMillis = 5_000) {
            MockWebServer().use { server ->
                server.enqueue(MockResponse().setBody("""{"startPageToken":"A+B /&"}"""))
                server.enqueue(MockResponse().setBody("""{"nextPageToken":"next","changes":[{"fileId":"f1","file":{"appProperties":{"domain":"example"}}}]}"""))
                server.enqueue(MockResponse().setBody("""{"newStartPageToken":"complete","changes":[{"fileId":"f2","removed":true}]}"""))
                server.enqueue(MockResponse().setResponseCode(503).setBody("""{"newStartPageToken":"forged"}"""))
                val drive = DriveChanges(baseUrl = server.url("/drive/v3/"))
                val token = "test-bearer"
                val cursor = drive.startPageToken(token)
                assertEquals("A+B /&", cursor)
                assertEquals("/drive/v3/changes/startPageToken", server.takeRequest(1, TimeUnit.SECONDS)?.requestUrl?.encodedPath)

                val first = drive.page(token, cursor)
                val request = requireNotNull(server.takeRequest(1, TimeUnit.SECONDS))
                assertEquals("Bearer $token", request.getHeader("Authorization"))
                assertEquals("appDataFolder", request.requestUrl?.queryParameter("spaces"))
                assertEquals(cursor, request.requestUrl?.queryParameter("pageToken"))
                assertEquals("next", first.nextPageToken)
                assertNull(first.newStartPageToken)
                assertEquals("example", first.changes.single().appProperties["domain"])

                val last = drive.page(token, requireNotNull(first.nextPageToken))
                assertNull(last.nextPageToken)
                assertEquals("complete", last.newStartPageToken)
                assertTrue(last.changes.single().removed)
                assertTrue(runCatching { drive.page(token, requireNotNull(last.newStartPageToken)) }.isFailure)
            }
    }

    @Test
    fun streamsOneDomainJournalAndRemovesAnInterruptedDownload() = bounded(timeoutMillis = 5_000) {
        MockWebServer().use { server ->
            val domain = "a".repeat(64)
            server.enqueue(MockResponse().setBody("""{"files":[{"id":"foreign","appProperties":{"domain":"other"}},{"id":"file with /","appProperties":{"replica":"r1","domain":"$domain"}}],"nextPageToken":"more"}"""))
            server.enqueue(MockResponse().setBody("journal"))
            server.enqueue(MockResponse().setBody("x".repeat(16_384)).setSocketPolicy(SocketPolicy.DISCONNECT_DURING_RESPONSE_BODY))
            val drive = DriveChanges(baseUrl = server.url("/drive/v3/"))
            val page = drive.files("test-bearer", domain, "opaque+/& cursor")
            assertEquals("more", page.nextPageToken)
            assertEquals("r1", page.files.single().replica)
            val listed = requireNotNull(server.takeRequest(1, TimeUnit.SECONDS)).requestUrl
            assertEquals("appDataFolder", listed?.queryParameter("spaces"))
            assertEquals("opaque+/& cursor", listed?.queryParameter("pageToken"))
            assertTrue(listed?.queryParameter("q")?.contains(domain) == true)

            val target = Files.createTempFile("drive-journal", ".tmp").toFile()
            try {
                target.writeText("old, longer journal")
                drive.download("test-bearer", page.files.single().id, target)
                assertEquals("journal", target.readText())
                val request = requireNotNull(server.takeRequest(1, TimeUnit.SECONDS))
                assertEquals("/drive/v3/files/file%20with%20%2F", request.requestUrl?.encodedPath)
                assertEquals("media", request.requestUrl?.queryParameter("alt"))
                assertTrue(runCatching { drive.download("test-bearer", page.files.single().id, target) }.isFailure)
                assertTrue(!target.exists())
            } finally {
                target.delete()
            }
        }
    }

    @Test
    fun reservesAndUploadsOnlyTheSelectedJournal() = bounded(timeoutMillis = 5_000) {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setBody("""{"ids":["reserved"]}"""))
            server.enqueue(MockResponse().addHeader("Location", server.url("/upload/session")))
            server.enqueue(MockResponse().setBody("""{"id":"reserved"}"""))
            server.enqueue(MockResponse().addHeader("Location", server.url("/upload/replacement")))
            server.enqueue(MockResponse().setBody("""{"id":"unexpected"}"""))
            val drive = DriveChanges(baseUrl = server.url("/drive/v3/"))
            val token = "test-bearer"
            val domain = "a".repeat(64)
            val journal = Files.createTempFile("drive-journal", ".tmp").toFile()
            try {
                journal.writeText("journal bytes")
                val id = drive.reserveId(token)
                assertEquals("reserved", id)
                val reservation = requireNotNull(server.takeRequest(1, TimeUnit.SECONDS)).requestUrl
                assertEquals("/drive/v3/files/generateIds", reservation?.encodedPath)
                assertEquals("appDataFolder", reservation?.queryParameter("space"))

                drive.upload(token, domain, "replica", id, true, journal)
                val create = requireNotNull(server.takeRequest(1, TimeUnit.SECONDS))
                assertEquals("POST", create.method)
                assertEquals("/upload/drive/v3/files", create.requestUrl?.encodedPath)
                assertEquals("resumable", create.requestUrl?.queryParameter("uploadType"))
                val metadata = create.body.readUtf8()
                assertTrue(metadata.contains("appDataFolder"))
                assertTrue(metadata.contains(domain))
                assertTrue(metadata.contains(id))
                val transferred = requireNotNull(server.takeRequest(1, TimeUnit.SECONDS))
                assertEquals("PUT", transferred.method)
                assertEquals("journal bytes", transferred.body.readUtf8())

                assertTrue(runCatching { drive.upload(token, domain, "replica", id, false, journal) }.isFailure)
                val replacement = requireNotNull(server.takeRequest(1, TimeUnit.SECONDS))
                assertEquals("PATCH", replacement.method)
                assertEquals("/upload/drive/v3/files/reserved", replacement.requestUrl?.encodedPath)
                assertEquals("{}", replacement.body.readUtf8())
                assertEquals("journal bytes", journal.readText())

                MockWebServer().use { other ->
                    server.enqueue(MockResponse().addHeader("Location", other.url("/upload/session")))
                    assertTrue(runCatching { drive.upload(token, domain, "replica", id, false, journal) }.isFailure)
                    assertEquals(0, other.requestCount)
                }
            } finally {
                journal.delete()
            }
        }
    }
}

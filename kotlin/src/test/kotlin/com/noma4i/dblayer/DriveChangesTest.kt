package com.noma4i.dblayer

import java.util.concurrent.TimeUnit
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
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
}

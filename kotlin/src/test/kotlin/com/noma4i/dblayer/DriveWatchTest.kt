package com.noma4i.dblayer

import java.util.Base64
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.toList
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class DriveWatchTest {
    @Test
    fun usesTheRelaySecretForEventsAndTheDriveTokenForTheChannel() = bounded(timeoutMillis = 5_000) {
        MockWebServer().use { server ->
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse = when (request.requestUrl?.encodedPath) {
                    "/events" -> MockResponse().setBody("event: ready\ndata: {}\n\nevent: changed\ndata: {}\n\n")
                    "/drive/v3/changes/watch" -> {
                        val body = Json.parseToJsonElement(request.body.clone().readUtf8()).jsonObject
                        val id = body.getValue("id").jsonPrimitive.content
                        MockResponse().setBody("""{"id":"$id","resourceId":"resource","expiration":"2000000000000"}""")
                    }
                    "/drive/v3/channels/stop" -> MockResponse().setResponseCode(204)
                    else -> MockResponse().setResponseCode(404)
                }
            }
            val relay = DriveWatch(server.url("/"))
            assertEquals(32, Base64.getUrlDecoder().decode(relay.secret).size)
            assertTrue(!relay.secret.contains('='))
            assertEquals(listOf(DriveWatchEvent.READY, DriveWatchEvent.CHANGED), relay.events().toList())
            assertEquals("Bearer ${relay.secret}", server.takeRequest(1, TimeUnit.SECONDS)?.getHeader("Authorization"))

            val drive = DriveChanges(baseUrl = server.url("/drive/v3/"))
            val webhook = "https://relay.invalid/drive/notifications"
            val channel = drive.watch("test-oauth", "before+/&", webhook, relay.secret)
            val registration = requireNotNull(server.takeRequest(1, TimeUnit.SECONDS))
            assertEquals("Bearer test-oauth", registration.getHeader("Authorization"))
            assertEquals("before+/&", registration.requestUrl?.queryParameter("pageToken"))
            assertEquals("appDataFolder", registration.requestUrl?.queryParameter("spaces"))
            val body = Json.parseToJsonElement(registration.body.readUtf8()).jsonObject
            assertEquals(webhook, body.getValue("address").jsonPrimitive.content)
            assertEquals(relay.secret, body.getValue("token").jsonPrimitive.content)
            drive.stop("test-oauth", channel)
            val stop = requireNotNull(server.takeRequest(1, TimeUnit.SECONDS))
            assertEquals("/drive/v3/channels/stop", stop.requestUrl?.encodedPath)
            assertTrue(stop.body.readUtf8().contains("resource"))
        }
    }
}

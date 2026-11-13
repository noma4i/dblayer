package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35], shadows = [PulsingConnectivityManager::class])
class OpenHeadersTest {
    @Test
    fun openReadsTheHeadersAgainForEveryRequest() = bounded {
        closeForCleanup()
        MockWebServer().use { server ->
            repeat(2) { server.enqueue(MockResponse().setResponseCode(400).setBody("busy")) }
            server.start()
            val token = java.util.concurrent.atomic.AtomicReference("first")
            val origin = server.url("/").toString().trimEnd('/')
            val encrypted = EncryptedConfig(origin, SealedServer.SERVER_KEY, SealedServer.SERVER_PIN, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("login_link" to EncryptedSessionOperation("/api/auth/login_link", EncryptedOperationClass.AUTHENTICATED))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel")
            val credential = object : CredentialStore {
                @Volatile private var value: String? = null
                override fun get(): String? = value
                override fun set(credential: String) { value = credential }
                override fun clear() { value = null }
            }
            DBlayer.open(org.robolectric.RuntimeEnvironment.getApplication(), headers = { mapOf("X-Token" to token.get()) }, encrypted = encrypted, credentials = credential, modelsJson = EMPTY_SPECS, uploadNotification = UploadNotification("Uploading", "Files are on their way", 1))

            osNetwork(online = true)

            val refused = ArrayList<Throwable>()
            suspend fun call() { try { DBlayer.database.sessionCall("login_link", """{"account":"A"}""") } catch (error: DbException) { refused.add(error) } }
            call()
            token.set("second")
            call()
            assertEquals("both calls are refused by the server: $refused", 2, refused.size)

            val tokens = generateSequence { server.takeRequest(1, java.util.concurrent.TimeUnit.SECONDS) }.map { it.getHeader("X-Token") }.toList()
            assertTrue("tokens sent: $tokens", tokens.size >= 2 && tokens.first() == "first" && tokens.last() == "second")
            closeForCleanup()
        }
    }
}

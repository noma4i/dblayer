package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class OpenHeadersTest {
    @Test
    fun openReadsTheHeadersAgainForEveryRequest() = bounded {
        DBlayer.close()
        for (name in listOf("manager", "sink")) {
            Reachability::class.java.getDeclaredField(name).apply { isAccessible = true }.set(null, null)
        }
        MockWebServer().use { server ->
            repeat(2) { server.enqueue(MockResponse().setResponseCode(400).setBody("busy")) }
            server.start()
            val operation = object : GraphQLOperation {
                override val operationName = "HeaderProbe"
                override val document = "query HeaderProbe { __typename }"
            }
            val token = java.util.concurrent.atomic.AtomicReference("first")
            DBlayer.open(org.robolectric.RuntimeEnvironment.getApplication(), endpoint = server.url("/").toString(), headers = { mapOf("X-Token" to token.get()) }, modelsJson = modelsJSON, operations = listOf(operation), uploadNotification = UploadNotification("Uploading", "Files are on their way", 1))

            val context = org.robolectric.RuntimeEnvironment.getApplication()
            val manager = context.getSystemService(android.content.Context.CONNECTIVITY_SERVICE) as android.net.ConnectivityManager
            val validated = org.robolectric.shadows.ShadowNetworkCapabilities.newInstance()
            org.robolectric.Shadows.shadowOf(validated).addCapability(android.net.NetworkCapabilities.NET_CAPABILITY_VALIDATED)
            org.robolectric.Shadows.shadowOf(manager).networkCallbacks.single().onCapabilitiesChanged(manager.activeNetwork!!, validated)

            runCatching { DBlayer.database.run("HeaderProbe", "{}") }
            token.set("second")
            runCatching { DBlayer.database.run("HeaderProbe", "{}") }

            assertEquals(listOf("first", "second"), List(2) { server.takeRequest(5, java.util.concurrent.TimeUnit.SECONDS)?.getHeader("X-Token") })
            DBlayer.close()
        }
    }
}

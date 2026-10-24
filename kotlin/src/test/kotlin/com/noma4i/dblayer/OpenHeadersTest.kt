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
        MockWebServer().use { server ->
            repeat(2) { server.enqueue(MockResponse().setResponseCode(400).setBody("busy")) }
            server.start()
            val operation = object : GraphQLOperation {
                override val operationName = "HeaderProbe"
                override val document = "query HeaderProbe { __typename }"
            }
            val token = java.util.concurrent.atomic.AtomicReference("first")
            DBlayer.open(org.robolectric.RuntimeEnvironment.getApplication(), endpoint = server.url("/").toString(), headers = { mapOf("X-Token" to token.get()) }, modelsJson = modelsJSON, operations = listOf(operation))

            runCatching { DBlayer.database.run("HeaderProbe", "{}") }
            token.set("second")
            runCatching { DBlayer.database.run("HeaderProbe", "{}") }

            assertEquals(listOf("first", "second"), List(2) { server.takeRequest().getHeader("X-Token") })
            DBlayer.close()
        }
    }
}

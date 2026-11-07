package com.noma4i.dblayer

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.ResponseBody.Companion.asResponseBody
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import okio.Buffer
import okio.ForwardingSource
import okio.buffer
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class TransportThreadTest {
    private val server = MockWebServer()
    private val executor = Executors.newSingleThreadExecutor { Thread(it, "caller-main") }
    private val caller = executor.asCoroutineDispatcher()
    private val readers = ConcurrentLinkedQueue<String>()

    private val probe = Interceptor { chain ->
        val response = chain.proceed(chain.request())
        val body = response.body
        val source = object : ForwardingSource(body.source()) {
            override fun read(sink: Buffer, byteCount: Long): Long {
                readers.add(Thread.currentThread().name)
                return super.read(sink, byteCount)
            }
        }.buffer()
        response.newBuilder().body(source.asResponseBody(body.contentType(), body.contentLength())).build()
    }
    private val client = OkHttpClient.Builder().addInterceptor(probe).build()

    @Before fun start() = server.start()

    @After fun stop() {
        server.shutdown()
        caller.close()
    }

    private fun assertBodyReadOffTheCaller() {
        assertTrue("the response body was never read", readers.isNotEmpty())
        assertFalse("the response body was read on ${readers.toSet()}", readers.any { it.startsWith("caller-main") })
    }

    @Test
    fun theHttpTransportReadsTheResponseBodyOffTheCallerThread() = bounded {
      for (mutation in listOf(false, true)) {
        server.enqueue(MockResponse().setResponseCode(200).setBody("""{"data":{"id":"7"}}"""))
        val transport = OkHttpTransport(client = client)

        val document = if (mutation) "mutation Probe { probe { id } }" else "query Probe { probe { id } }"
        val request = HttpRequest(Author.SESSION, 0uL, 1u, server.url("/graphql").toString(), "POST", mapOf("Content-Type" to "application/json"), """{"query":"$document","operationName":"Probe","variables":{"id":"7"}}""", 10_000uL, 30_000uL, NetworkPurpose.STANDARD)
        val text = (withContext(caller) { transport.send(request) } as RawOutcome.Response).body

        assertEquals("""{"data":{"id":"7"}}""", text)
        assertBodyReadOffTheCaller()
        val sent = server.takeRequest()
        assertEquals("POST", sent.method)
        val body = parseJson(sent.body.readUtf8()) as kotlinx.serialization.json.JsonObject
        assertEquals(kotlinx.serialization.json.JsonPrimitive(document), body["query"])
        assertEquals(parseJson("""{"id":"7"}"""), body["variables"])
      }
    }

    private class IdleProgress : ByteProgress(NoHandle) {
        override fun sent(bytes: ULong) {}
        override fun isCancelled() = false
    }

    private fun putOf(transport: OkHttpTransport) = suspend {
        transport.put(PutTarget(server.url("/upload").toString(), emptyMap(), 10_000uL, 30_000uL), PutFile(java.io.File.createTempFile("thread", ".bin").apply { writeBytes(byteArrayOf(1, 2)); deleteOnExit() }.path, 0u, 2u), TransferIdentity(1L, "1.0.1", TransferStep.Put), IdleProgress())
    }

    @Test
    fun aLostConnectionReachesTheCoreAsARawFailure() = bounded {
        for (operation in listOf("query", "mutation", "put")) {
            server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START))
            val transport = OkHttpTransport(client = OkHttpClient.Builder().retryOnConnectionFailure(false).build())
            val outcome = if (operation == "put") putOf(transport)() else transport.send(probeRequest(server.url("/graphql").toString(), "{}"))
            assertFalse(operation, outcome.rawFailure()?.message.equals("Canceled", ignoreCase = true))
        }
    }

    /** xfer_prop_put_cancel_pushes on the platform: the cancel of the coroutine cancels the OkHttp call, the flag of the progress is never read. */
    @Test
    fun cancellingThePutCoroutineCancelsTheCall() = bounded {
        val canceled = kotlinx.coroutines.CompletableDeferred<Unit>()
        val observed = OkHttpClient.Builder().retryOnConnectionFailure(false).eventListener(object : okhttp3.EventListener() {
            override fun canceled(call: okhttp3.Call) { canceled.complete(Unit) }
        }).build()
        server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE))
        val put = kotlinx.coroutines.async(kotlinx.coroutines.Dispatchers.IO) { putOf(OkHttpTransport(client = observed))() }
        kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.IO) { server.takeRequest(10, java.util.concurrent.TimeUnit.SECONDS) }
        put.cancel()
        canceled.await()
        assertTrue(put.isCancelled)
    }
}

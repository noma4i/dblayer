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
        val transport = OkHttpTransport(server.url("/graphql").toString(), client = client)

        val request = TransportRequest(if (mutation) "mutation Probe { probe { id } }" else "query Probe { probe { id } }", "Probe", """{"id":"7"}""", NetworkPurpose.STANDARD)
        val text = withContext(caller) { if (mutation) transport.mutate(request) else transport.query(request) }

        assertEquals("""{"data":{"id":"7"}}""", text)
        assertBodyReadOffTheCaller()
        val sent = server.takeRequest()
        assertEquals("POST", sent.method)
        val body = parseJson(sent.body.readUtf8()) as kotlinx.serialization.json.JsonObject
        assertEquals(kotlinx.serialization.json.JsonPrimitive(request.document), body["query"])
        assertEquals(parseJson(request.variablesJson), body["variables"])
      }
    }

    private class CancelledProgress(private val cancelled: Boolean) : ByteProgress(NoHandle) {
        override fun sent(bytes: ULong) {}
        override fun isCancelled() = cancelled
    }

    @Test
    fun lostConnectionsAndCancelledUploadsReachTheCoreAsTypedFailures() = bounded {
        val cases = listOf(
            Triple("query", false, DbException.Network::class.java),
            Triple("mutation", false, DbException.Interrupted::class.java),
            Triple("put", false, DbException.Network::class.java),
            Triple("put", true, DbException.Interrupted::class.java),
        )
        for ((operation, cancelled, expected) in cases) {
            server.enqueue(MockResponse().setSocketPolicy(if (operation == "put" && cancelled) SocketPolicy.NO_RESPONSE else SocketPolicy.DISCONNECT_AT_START))
            val transport = OkHttpTransport(server.url("/graphql").toString(), client = OkHttpClient.Builder().retryOnConnectionFailure(false).build())
            val request = TransportRequest("query Probe { probe { id } }", "Probe", "{}", NetworkPurpose.STANDARD)
            val error = runCatching {
                when (operation) {
                    "query" -> transport.query(request)
                    "mutation" -> transport.mutate(request)
                    else -> transport.put(server.url("/upload").toString(), emptyMap(), PutFile(java.io.File.createTempFile("thread", ".bin").apply { writeBytes(byteArrayOf(1, 2)); deleteOnExit() }.path, 0u, 2u), TransferIdentity(1L, "1.0.1", TransferStep.Put), CancelledProgress(cancelled))
                }
            }.exceptionOrNull()
            assertEquals("$operation cancelled=$cancelled", expected, error?.javaClass)
        }
    }
}

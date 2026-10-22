package com.noma4i.dblayer

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.ResponseBody.Companion.asResponseBody
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
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
        server.enqueue(MockResponse().setResponseCode(200).setBody("""{"data":{"id":"7"}}"""))
        val transport = OkHttpTransport(server.url("/graphql").toString(), client = client)

        val text = withContext(caller) { transport.query(TransportRequest("query { probe { id } }", "Probe", "{}")) }

        assertEquals("""{"data":{"id":"7"}}""", text)
        assertBodyReadOffTheCaller()
    }
}

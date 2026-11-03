package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import java.lang.ref.PhantomReference
import java.lang.ref.ReferenceQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import org.junit.Assert.assertEquals
import org.junit.Test

class SubscriptionTokenTest {
    private val count = 8

    @Test
    fun cancelHandsBackExactlyTheTokensTheTransportIssued() = bounded {
        val mock = MockTransport()
        repeat(count) { mock.enqueue("documentBySlug", """{"data":{"documentBySlug":{"id":"d1","slug":"a"}}}""") }
        val issued = CopyOnWriteArrayList<String>()
        val revoked = CopyOnWriteArrayList<String>()
        val unknown = CopyOnWriteArrayList<String>()
        val sequence = AtomicInteger()
        val queue = ReferenceQueue<Any>()
        val watched = CopyOnWriteArrayList<PhantomReference<Any>>()
        val transport = object : Transport by mock {
            override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink) {
                val token = "token-${sequence.getAndIncrement()}-${request.operationName}-${"x".repeat(40)}"
                issued.add(token)
                watched.add(PhantomReference<Any>(sink, queue))
                sink.opened(token)
                sink.ready()
            }
            override fun unsubscribe(token: String) {
                if (token !in issued) unknown.add(token)
                revoked.add(token)
            }
        }
        val database = openDatabase(Config(null, null, 8u), modelsJSON, transport)
        database.registerOperations("""[{"name":"documentBySlug","document":"query documentBySlug(${'$'}slug: String!, ${'$'}source: String!) { documentBySlug(slug: ${'$'}slug, source: ${'$'}source) { id slug } }","rootPath":"documentBySlug"},{"name":"documentUpdated","document":"subscription documentUpdated(${'$'}slug: String!, ${'$'}source: String!) { documentUpdated(slug: ${'$'}slug, source: ${'$'}source) { id slug } }","rootPath":"documentUpdated"}]""")
        val listener = object : ValueSink { override fun delivered(delivery: StreamDelivery) {} }
        val lives = (0 until count).map { database.subscribe(database.sessionEpoch(), StreamRequest.Query("""{"model":"Document","relation":"bySlug","params":{"slug":"s$it","source":"web"}}"""), listener) }
        eventually("every subscription reaches the transport") { issued.size == count }
        var collected = 0
        eventually("the Kotlin sink wrappers are collected") { System.gc(); while (queue.poll() != null) collected++; collected == count }
        lives.forEach { database.cancelStream(it) }
        eventually("every cancel reaches the transport") { revoked.size == count }
        assertEquals(emptyList<String>(), unknown.toList())
        assertEquals(emptyList<String>(), mock.unknownTokens.toList())
        assertEquals(issued.toSet(), revoked.toSet())
    }
}

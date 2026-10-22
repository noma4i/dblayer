package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import java.util.concurrent.CopyOnWriteArrayList
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

private class CopyingLoader(private val source: ClassLoader) : ClassLoader(source) {
    override fun loadClass(name: String, resolve: Boolean): Class<*> {
        if (!name.startsWith("com.noma4i.dblayer.") || name.startsWith("com.noma4i.dblayer.models.")) return super.loadClass(name, resolve)
        synchronized(this) {
            findLoadedClass(name)?.let { return it }
            val bytes = source.getResourceAsStream(name.replace('.', '/') + ".class")?.use { it.readBytes() } ?: return super.loadClass(name, resolve)
            return defineClass(name, bytes, 0, bytes.size)
        }
    }
}

class SecondLoaderTest {
    @Test
    fun aSecondLoaderIsRefusedAndTheFirstStillSubscribesAndCancels() = bounded {
        uniffiEnsureInitialized()
        val second = CopyingLoader(SecondLoaderTest::class.java.classLoader)
        val refusal = runCatching { second.loadClass("com.noma4i.dblayer.DblayerKt").getMethod("uniffiEnsureInitialized").invoke(null) }.exceptionOrNull()
        val cause = generateSequence(refusal) { it.cause }.last()
        assertTrue(cause is IllegalStateException)
        assertEquals("DBlayer is already loaded by another class loader in this process; load it once", cause.message)
        val mock = MockTransport()
        val issued = CopyOnWriteArrayList<String>()
        val revoked = CopyOnWriteArrayList<String>()
        val transport = object : Transport by mock {
            override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink): String {
                val token = "token-${request.operationName}-${"x".repeat(40)}"
                issued.add(token)
                sink.ready()
                return token
            }
            override fun unsubscribe(token: String) { revoked.add(token) }
        }
        val database = openDatabase(Config(null, null, 8u), modelsJSON, transport)
        database.registerOperations("""[{"name":"documentBySlug","document":"query documentBySlug(${'$'}slug: String!, ${'$'}source: String!) { documentBySlug(slug: ${'$'}slug, source: ${'$'}source) { id slug } }","rootPath":"documentBySlug"},{"name":"documentUpdated","document":"subscription documentUpdated(${'$'}slug: String!, ${'$'}source: String!) { documentUpdated(slug: ${'$'}slug, source: ${'$'}source) { id slug } }","rootPath":"documentUpdated"}]""")
        val live = database.liveQuery("""{"model":"Document","relation":"bySlug","params":{"slug":"a","source":"web"}}""", object : QueryListener { override fun changed(change: LiveChange) {} })
        eventually("subscribe reaches the transport") { issued.size == 1 }
        live.cancel()
        eventually("cancel reaches the transport") { revoked.size == 1 }
        assertEquals(issued.toList(), revoked.toList())
    }
}

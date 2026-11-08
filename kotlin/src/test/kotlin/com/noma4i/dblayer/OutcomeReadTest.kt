package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

class OutcomeReadTest {
    @Test
    fun parallelOutcomeReadsOfOneReceiptAllGetTheServerId() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        openSigned(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")
        val sent = Moment.create(text = "hello", media = null, account = AccountScope.current())
        val ids = coroutineScope { (1..8).map { async { withTimeout(5000) { sent.confirmed().id } } }.awaitAll() }
        assertEquals(List(8) { "m77" }, ids)
    }
}

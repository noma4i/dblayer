package com.noma4i.dblayer

import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertTrue
import org.junit.Test

class SwapSnapshotTest {
    @Test
    fun aLiveReadWhileTheCreateReplyLandsNeverSeesAnEmptySet() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateRecord", """{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c","authorId":"u1","text":"hi","createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateRecord","document":"mutation CreateRecord(${'$'}containerId: ID!, ${'$'}text: String!, ${'$'}signedId: String, ${'$'}attachedUserId: ID) { createRecord(containerId: ${'$'}containerId, text: ${'$'}text, signedId: ${'$'}signedId, attachedUserId: ${'$'}attachedUserId) { id containerId authorId text createdAt } }"}]""")
        val query = """{"model":"Record"}"""
        val held = DBlayer.database.liveQuery(query, object : QueryListener {
            override fun changed(change: LiveChange) = Unit
        })

        val local = Record.create(containerId = "c", authorId = "u1", text = "hi", attachedUserId = null, signedId = null)
        val counts = mutableListOf<Int>()
        coroutineScope {
            val landing = async { withTimeout(5000) { local.settled(null) } }
            do {
                val live = DBlayer.database.liveQuery(query, object : QueryListener {
                    override fun changed(change: LiveChange) = Unit
                })
                val seen = live.current().rows.size
                live.cancel()
                if (seen == 1 || counts.isNotEmpty()) counts.add(seen)
            } while (!landing.isCompleted)
            landing.await()
        }
        held.cancel()
        assertTrue("rows seen by fresh live reads from the temporary row until the reply landed: $counts", counts.isNotEmpty() && counts.all { it == 1 })
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.cancelAndJoin
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ListChangeTest {
    @Test fun aSlowConsumerReceivesABoundedReplacementWithEveryCurrentRow() = bounded {
        reopen(Config(null, null, 64u), modelsJSON, null)
        val query = Query("Moment", Moment).order(Moment.createdAt.desc)
        val attached = kotlinx.coroutines.CompletableDeferred<Unit>()
        val release = kotlinx.coroutines.CompletableDeferred<Unit>()
        val completed = kotlinx.coroutines.CompletableDeferred<Pair<Boolean, List<Moment>>>()
        val collection = launch {
            val rows = mutableListOf<Moment>()
            var sawReplacement = false
            query.changes.collect { change ->
                if (!attached.isCompleted) { attached.complete(Unit); release.await() }
                if (change.kind == ListChangeKind.Replaced) {
                    rows.clear()
                    rows.addAll(change.inserted.map { it.row })
                    sawReplacement = sawReplacement || change.inserted.size >= 64
                }
                else change.inserted.forEach { rows.add(it.index, it.row) }
                if (change.size == 192) completed.complete(sawReplacement to rows.toList())
            }
        }
        attached.await()
        repeat(192) { Moment.create(text = "row$it") }
        release.complete(Unit)
        val (sawReplacement, rows) = completed.await()
        collection.cancelAndJoin()
        assertTrue("overflow never delivered a replacement snapshot", sawReplacement)
        assertEquals(query.live.flow.first(), rows)
    }
    @Test fun changesWork() {
        var decoded = 0
        val decoder = object : ModelDecoder<Moment> by Moment {
            override fun decode(row: WireRow): Moment { decoded++; return Moment.decode(row) }
        }
        for (run in 1..5) {
            val initial = ListChange.decode(LiveChange(1L, true, 2000u, (0 until 2000).map { wire("$it") }, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), null, null, inserted = (0 until 2000).map { it.toUInt() }), decoder)
            assertEquals(2000, initial.inserted.size)
            val before = decoded
            val delta = LiveChange(2L, false, 2001u, listOf(wire("new")), listOf(2000u), emptyList(), listOf(Placement("new", 2000u)), emptyList(), emptyList(), null, null, inserted = listOf(0u))
            val next = ListChange.decode(delta, decoder)
            assertEquals(1, next.inserted.size)
            assertEquals(2000, next.inserted.single().index)
            assertEquals(1, decoded - before)
            println("delta-work kotlin run=$run ffi=${delta.rows.size} decoded=${decoded - before} consumer=${next.inserted.size}")
        }
    }

    @Test fun changesDeliverOnlyChangedRowsAndRestart() = bounded {
        reopen(Config(null, null, 64u), modelsJSON, null)
        val query = Query("Moment", Moment).order(Moment.createdAt.desc)
        repeat(2000) { Moment.create(text = "row$it") }
        val inbox = Channel<ListChange<Moment>>(Channel.UNLIMITED)
        val collection = launch { query.changes.collect { inbox.send(it) } }
        val initial = inbox.receive()
        assertEquals(2000, initial.inserted.size)
        var row: Moment? = null
        for (run in 1..5) {
            row = Moment.create(text = "new$run")
            val next = inbox.receive()
            assertEquals(listOf(row.id), next.inserted.map { it.row.id })
            assertEquals(emptyList<Any>(), next.updated)
            assertEquals(2000 + run, next.size)
            assertTrue(next.revision > initial.revision)
            println("changes-work kotlin run=$run consumer=${next.inserted.size}")
        }
        row!!.update(text = "edited")
        assertEquals("edited", inbox.receive().updated.single().row.text)
        row.destroy()
        assertEquals(listOf(row.id), inbox.receive().removed)
        collection.cancelAndJoin()
        assertEquals(2004, query.changes.first().inserted.size)
        eventually("changes subscription released") { DBlayer.subscriptionCensus().subscriptions == 0uL }
    }

    @Test fun changesKeepAllBatchesAndCancel() = bounded {
        val transport = MockTransport()
        val nodes = (0 until 2501).joinToString(",") { """{"__typename":"Moment","id":"$it","uuid":"$it","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}""" }
        transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[$nodes],"pageInfo":{"hasNextPage":false}}}}""")
        reopen(Config(null, null, 64u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed { feed { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]""")
        val query = Query("Moment", Moment)
        val inbox = Channel<ListChange<Moment>>(Channel.UNLIMITED)
        val collection = launch { query.changes.collect { inbox.send(it) } }
        assertEquals(0, inbox.receive().inserted.size)
        Moment.feed.load()
        assertEquals((0 until 2501).map { "$it" }.toSet(), inbox.receive().inserted.map { it.row.id }.toSet())
        collection.cancelAndJoin()
        eventually("changes subscription released") { DBlayer.subscriptionCensus().subscriptions == 0uL }
    }
    private fun wire(id: String) = WireRow(id, 1L, 1uL, Moment.layout,
        listOf(Cell.Text(id), Cell.Null, Cell.Text(id), Cell.Bool(false), Cell.Keyed(emptyList()), Cell.Null, Cell.Text("x")),
        WireSync(WireSyncState.SYNCED, emptyList(), emptyList(), null, 0u, 0L), null)

    @Test fun snapshotWork() {
        for (run in 1..5) {
            val feed = ListFeed(Moment)
            feed.apply(LiveChange(1L, true, 2000u, (0 until 2000).map { wire("$it") }, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), null, null))
            val before = feed.decodedRows
            val delta = LiveChange(2L, false, 2001u, listOf(wire("new")), listOf(2000u), emptyList(), listOf(Placement("new", 2000u)), emptyList(), emptyList(), null, null)
            val snapshot = feed.apply(delta)
            println("snapshot-work kotlin run=$run ffi=${delta.rows.size} decoded=${feed.decodedRows - before} consumer=${snapshot.rows.size} known-scan=${snapshot.rows.size}")
            assertEquals(2001, snapshot.rows.size)
            assertEquals(1, feed.decodedRows - before)
        }
    }
}

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
    private enum class Surface { ListChange, ListFeed }

    @Test fun aDeltaOfOneRowDecodesOneRowAtEveryListSize() {
        for (surface in Surface.entries) for (size in listOf(20, 200, 2000)) {
            val label = "$surface $size"
            val rows = (0 until size).map { wire("$it") }
            val delta = LiveChange(2L, false, (size + 1).toUInt(), listOf(wire("new")), listOf(size.toUInt()), emptyList(), listOf(Placement("new", size.toUInt())), emptyList(), emptyList(), null, null, inserted = listOf(0u))
            when (surface) {
                Surface.ListChange -> {
                    var decoded = 0
                    val decoder = object : ModelDecoder<Moment> by Moment {
                        override fun decode(row: WireRow): Moment { decoded++; return Moment.decode(row) }
                    }
                    val initial = ListChange.decode(LiveChange(1L, true, size.toUInt(), rows, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), null, null, inserted = (0 until size).map { it.toUInt() }), decoder)
                    assertEquals(label, ListChangeKind.Replaced, initial.kind)
                    assertEquals(label, size, initial.inserted.size)
                    val before = decoded
                    val next = ListChange.decode(delta, decoder)
                    assertEquals(label, ListChangeKind.Delta, next.kind)
                    assertEquals(label, 1, next.inserted.size)
                    assertEquals(label, size, next.inserted.single().index)
                    assertEquals(label, 1, decoded - before)
                }
                Surface.ListFeed -> {
                    val feed = ListFeed(Moment)
                    feed.apply(LiveChange(1L, true, size.toUInt(), rows, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), null, null))
                    val before = feed.decodedRows
                    val snapshot = feed.apply(delta)
                    assertEquals(label, size + 1, snapshot.rows.size)
                    assertEquals(label, 1, feed.decodedRows - before)
                }
            }
        }
    }
    @Test fun aChangeStreamDeliversOneInsertionAndARestartedStreamStartsWithTheWholeList() = bounded {
        reopen(Config(null, null, 64u), modelsJSON, null)
        val query = Query("Moment", Moment).order(Moment.createdAt.desc)
        repeat(20) { Moment.create(text = "row$it") }
        val inbox = Channel<ListChange<Moment>>(Channel.UNLIMITED)
        val collection = launch { query.changes.collect { inbox.send(it) } }
        val initial = inbox.receive()
        assertEquals(20, initial.inserted.size)
        val row = Moment.create(text = "new")
        val next = inbox.receive()
        assertEquals(listOf(row.id), next.inserted.map { it.row.id })
        assertEquals(21, next.size)
        assertTrue(next.revision > initial.revision)
        collection.cancelAndJoin()
        val restarted = query.changes.first()
        assertEquals(ListChangeKind.Replaced, restarted.kind)
        assertEquals(21, restarted.inserted.size)
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
        WireSync(WireSyncState.SYNCED, emptyList(), emptyList(), null, 0u, 0L), emptyList())

}

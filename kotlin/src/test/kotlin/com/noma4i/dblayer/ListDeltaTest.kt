package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import java.util.concurrent.atomic.AtomicInteger
import org.junit.Test

class ListDeltaTest {
    private val byText get() = Query("Moment", Moment).order(Order("text", "asc"))

    private suspend fun open() {
        reopen(Config(null, null, 64u), modelsJSON, null)
    }

    private fun watching(query: Query<Moment>, feed: ListFeed<Moment>): Flow<List<Moment>> {
        val json = query.json
        return LiveSource<List<Moment>>(emptyList()) { emit ->
            val subscription = DBlayer.database.liveQuery(json, ChangeListener { emit(feed.apply(it).rows) })
            val cancel: () -> Unit = { subscription.cancel() }
            cancel
        }.flow
    }

    @Test
    fun inv_observe_15_the_feed_rebuilds_the_list_that_a_fresh_query_returns() = bounded {
        open()
        val feed = ListFeed(Moment)
        val changes = watching(byText, feed)
        var seed = 0x2545F4914F6CDD1DL
        fun below(bound: Int): Int {
            seed = seed xor (seed shl 13); seed = seed xor (seed ushr 7); seed = seed xor (seed shl 17)
            return java.lang.Long.remainderUnsigned(seed, bound.toLong()).toInt()
        }
        val rows = ArrayList<Moment>()
        for (step in 0 until 40) {
            when (if (rows.isEmpty()) 0 else below(3)) {
                0 -> rows.add(Moment.create(text = "n%02d-%03d".format(below(40), step)))
                1 -> { val at = below(rows.size); rows[at] = rows[at].update(text = GraphQLNullable.Value("n%02d-%03d".format(below(40), step))) }
                else -> rows.removeAt(below(rows.size)).destroy()
            }
            val fresh = byText.live.flow.first().map { it.id to it.text }
            val seen = withTimeout(20000) { changes.first { list -> list.map { it.id to it.text } == fresh } }
            assertEquals("step $step", fresh, seen.map { it.id to it.text })
        }
    }

    private fun moment(id: String, text: String) = Moment(id = id, uuid = id, text = text, likedByMe = false, media = kotlinx.serialization.json.JsonObject(emptyMap()), createdAt = "x")

    private fun wire(row: Moment, cells: List<Cell>? = null) = WireRow(
        row.id, 1L, 1uL, Moment.layout,
        cells ?: listOf(Cell.Text(row.uuid), Cell.Null, Cell.Text(row.text ?: ""), Cell.Bool(false), Cell.Keyed(emptyList()), Cell.Null, Cell.Text(row.createdAt)),
        WireSync(WireSyncState.SYNCED, emptyList(), emptyList(), null, 0u, 0L), emptyList(),
    )

    private fun change(
        size: Int, full: Boolean = false, rows: List<Moment> = emptyList(), indexes: List<UInt> = emptyList(), removed: List<String> = emptyList(),
        placed: List<Placement> = emptyList(), replaced: List<Replacement> = emptyList(), renamed: List<Replacement> = emptyList(),
    ) = LiveChange(1L, full, size.toUInt(), rows.map { wire(it) }, indexes, removed, placed, replaced, renamed, null, null)

    @Test
    fun inv_observe_15_the_feed_applies_each_kind_of_delta_in_order() {
        val feed = ListFeed(Moment)
        val first = feed.apply(change(size = 4, full = true, rows = listOf(moment("a", "1"), moment("b", "2"), moment("c", "3"), moment("d", "4"))))
        assertEquals(listOf("a", "b", "c", "d"), first.rows.map { it.id })
        val updated = feed.apply(change(size = 4, rows = listOf(moment("b", "2x")), indexes = listOf(1u)))
        assertEquals(false, updated.reordered)
        assertEquals(listOf("1", "2x", "3", "4"), updated.rows.map { it.text })
        val moved = feed.apply(change(size = 4, placed = listOf(Placement("d", 0u))))
        assertEquals(true, moved.reordered)
        assertEquals(listOf("d", "a", "b", "c"), moved.rows.map { it.id })
        val mixed = feed.apply(change(size = 4, rows = listOf(moment("e", "5"), moment("r", "6")), indexes = listOf(1u, 2u), removed = listOf("a"), placed = listOf(Placement("e", 1u), Placement("c", 3u)), replaced = listOf(Replacement("b", "r"))))
        assertEquals(listOf("d", "e", "r", "c"), mixed.rows.map { it.id })
    }

    @Test
    fun inv_observe_15_a_delivery_that_does_not_fit_the_rows_is_refused_and_the_feed_counts_it() {
        val feed = ListFeed(Moment)
        assertEquals(false, feed.apply(change(size = 2, full = true, rows = listOf(moment("a", "1"), moment("b", "2")))).desynced)
        assertEquals(true, feed.apply(change(size = 3, placed = listOf(Placement("zz", 0u)))).desynced)
        assertEquals(true, feed.apply(change(size = 2, rows = listOf(moment("a", "1")), indexes = listOf(5u))).desynced)
        assertEquals(true, feed.apply(change(size = 9, rows = listOf(moment("a", "1")), indexes = listOf(0u))).desynced)
        assertEquals(true, feed.apply(change(size = 2, removed = listOf("a"), placed = listOf(Placement("zz", 0u)))).desynced)
        assertEquals(listOf("a", "b"), feed.rows.map { it.id })
        assertEquals(4, feed.desyncs)
    }

    @Test
    fun inv_observe_15_a_delivery_that_does_not_fit_opens_the_subscription_again_for_the_whole_list() {
        val listeners = java.util.concurrent.LinkedBlockingQueue<QueryListener>()
        val values = java.util.concurrent.LinkedBlockingQueue<List<String>>()
        val session = ListSession<Moment, List<String>>(Moment, { listener -> listeners.put(listener); val cancel: () -> Unit = {}; cancel }, { values.put(it) }, { applied, _ -> applied.rows.map { it.id } })
        session.start()
        val first = listeners.poll(5, java.util.concurrent.TimeUnit.SECONDS)!!
        first.changed(change(size = 2, full = true, rows = listOf(moment("a", "1"), moment("b", "2"))))
        assertEquals(listOf("a", "b"), values.poll(5, java.util.concurrent.TimeUnit.SECONDS))
        first.changed(change(size = 3, placed = listOf(Placement("zz", 0u))))
        val second = listeners.poll(5, java.util.concurrent.TimeUnit.SECONDS)!!
        second.changed(change(size = 3, full = true, rows = listOf(moment("a", "1"), moment("b", "2"), moment("c", "3"))))
        assertEquals(listOf("a", "b", "c"), values.poll(5, java.util.concurrent.TimeUnit.SECONDS))
        assertEquals(1, session.desyncs)
        assertEquals(1, session.reopened)
    }

    private fun poll(queue: java.util.concurrent.LinkedBlockingQueue<*>) = queue.poll(10, java.util.concurrent.TimeUnit.SECONDS)

    private class Rig(opens: (Int) -> Unit = {}) {
        val listeners = java.util.concurrent.LinkedBlockingQueue<QueryListener>()
        val values = java.util.concurrent.LinkedBlockingQueue<Snapshot<Moment>>()
        private val count = AtomicInteger()
        val session = ListSession<Moment, Snapshot<Moment>>(
            Moment,
            { listener -> opens(count.getAndIncrement()); listeners.put(listener); val cancel: () -> Unit = {}; cancel },
            { values.put(it) },
            { applied, _ -> Snapshot(applied.rows, emptyMap(), applied.reordered, error = applied.failure) },
        )
    }

    @Test
    fun inv_observe_15_a_row_that_does_not_decode_keeps_its_slot_and_never_reopens_the_subscription() {
        val rig = Rig()
        rig.session.start()
        val listener = poll(rig.listeners) as QueryListener
        listener.changed(LiveChange(1L, true, 3u, listOf(wire(moment("a", "1")), wire(moment("b", "2"), emptyList()), wire(moment("c", "3"))), emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), null, null))
        assertEquals(listOf("a", "c"), (poll(rig.values) as Snapshot<*>).rows.map { (it as Moment).id })
        repeat(3) {
            listener.changed(change(size = 3, rows = listOf(moment("c", "3")), indexes = listOf(2u)))
            assertEquals(listOf("a", "c"), (poll(rig.values) as Snapshot<*>).rows.map { (it as Moment).id })
        }
        assertEquals(0, rig.session.reopened)
        assertEquals(0, rig.session.desyncs)
    }

    @Test
    fun inv_observe_18_a_delivery_that_inserts_the_real_row_names_the_temporary_id_it_replaced() {
        val feed = ListFeed(Moment)
        assertEquals(false, feed.apply(change(size = 1, full = true, rows = listOf(moment("a", "1")))).desynced)
        val named = change(size = 2, rows = listOf(moment("m77", "2")), indexes = listOf(1u), placed = listOf(Placement("m77", 1u)), renamed = listOf(Replacement("tmp-1", "m77")))
        assertEquals(false, feed.apply(named).desynced)
        assertEquals(mapOf("m77" to "tmp-1"), named.replacedIds())
    }

    @Test
    fun inv_observe_18_a_snapshot_names_the_replaced_id_when_the_temporary_row_was_never_delivered() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]""")
        val gate = java.util.concurrent.CountDownLatch(1)
        val deliveries = AtomicInteger()
        val values = java.util.concurrent.LinkedBlockingQueue<Snapshot<Moment>>()
        val json = Query("Moment", Moment).order(Order("createdAt", "desc")).json
        val session = ListSession<Moment, Snapshot<Moment>>(
            Moment,
            { listener ->
                val subscription = DBlayer.database.liveQuery(json, ChangeListener { change ->
                    val held = deliveries.getAndIncrement() == 0
                    listener.changed(change)
                    if (held) gate.await(30, java.util.concurrent.TimeUnit.SECONDS)
                })
                val cancel: () -> Unit = { subscription.cancel() }
                cancel
            },
            { values.put(it) },
            { applied, change -> Snapshot(applied.rows, change.replacedIds(), applied.reordered) },
        )
        session.start()
        assertEquals(true, (poll(values) as Snapshot<*>).rows.isEmpty())
        val local = Moment.create(text = "hello", media = null)
        withTimeout(5000) { local.settled(null) }
        gate.countDown()
        var found: Snapshot<*>? = null
        repeat(5) { if (found == null) (poll(values) as Snapshot<*>?)?.let { if (it.replaced.containsKey("m77")) found = it } }
        assertEquals(local.id, found?.replaced?.get("m77"))
        session.cancel()
    }

    private data class DesyncScenario(val name: String, val rounds: Int, val goodDeltaFirst: Boolean, val reopenFails: Boolean, val stalls: Boolean, val reopened: Int?)

    @Test
    fun inv_observe_15_a_desynced_subscription_opens_again_until_it_stalls_and_says_why() {
        val scenarios = listOf(
            DesyncScenario("keeps desyncing", 3, false, false, true, 2),
            DesyncScenario("a good delta resets the count", 3, true, false, false, 3),
            DesyncScenario("the reopen cannot open", 1, false, true, true, null),
        )
        for (scenario in scenarios) {
            val rig = if (scenario.reopenFails) Rig { number -> if (number > 0) throw java.io.IOException("offline") } else Rig()
            rig.session.start()
            var current = poll(rig.listeners) as QueryListener
            repeat(scenario.rounds) { round ->
                current.changed(change(size = 1, full = true, rows = listOf(moment("a", "1"))))
                assertEquals(scenario.name, listOf("a"), (poll(rig.values) as Snapshot<*>).rows.map { (it as Moment).id })
                if (scenario.goodDeltaFirst) {
                    current.changed(change(size = 1, rows = listOf(moment("a", "1")), indexes = listOf(0u)))
                    assertEquals(scenario.name, listOf("a"), (poll(rig.values) as Snapshot<*>).rows.map { (it as Moment).id })
                }
                current.changed(change(size = 2, placed = listOf(Placement("zz", 0u))))
                if (scenario.stalls && round == scenario.rounds - 1) {
                    val stopped = poll(rig.values) as Snapshot<*>
                    assertEquals(scenario.name, true, stopped.error != null)
                    assertEquals(scenario.name, listOf("a"), stopped.rows.map { (it as Moment).id })
                } else {
                    current = poll(rig.listeners) as QueryListener
                }
            }
            assertEquals(scenario.name, scenario.stalls, rig.session.isStalled)
            scenario.reopened?.let { assertEquals(scenario.name, it, rig.session.reopened) }
        }
    }
}

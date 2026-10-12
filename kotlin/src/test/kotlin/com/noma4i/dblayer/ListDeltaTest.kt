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
    private val byText get() = Query("Moment", Moment.serializer()).order(Order("text", "asc"))

    private fun open() {
        DBlayer.database = Database.open(Config(null, null, 64u), modelsJSON, null)
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
    fun inv_observe_17_an_update_of_one_row_decodes_one_row_at_every_list_size() = bounded {
        val decoded = listOf(20, 200, 2000).map { count ->
            open()
            val created = (0 until count).map { Moment.create(text = "t%05d".format(it)) }
            val feed = ListFeed(Moment.serializer())
            val states = java.util.concurrent.LinkedBlockingQueue<List<Moment>>()
            val subscription = DBlayer.database.liveQuery(byText.json, ChangeListener { states.put(feed.apply(it).rows) })
            assertEquals(count, states.poll(20, java.util.concurrent.TimeUnit.SECONDS)!!.size)
            val before = feed.decodedRows
            val edited = "t%05dx".format(count / 2)
            created[count / 2].update(text = edited)
            assertEquals(edited, states.poll(20, java.util.concurrent.TimeUnit.SECONDS)!![count / 2].text)
            subscription.cancel()
            feed.decodedRows - before
        }
        assertEquals(listOf(1, 1, 1), decoded)
    }

    @Test
    fun inv_observe_15_the_feed_rebuilds_the_list_that_a_fresh_query_returns() = bounded {
        open()
        val feed = ListFeed(Moment.serializer())
        val changes = watching(byText, feed)
        var seed = 0x2545F4914F6CDD1DL
        fun below(bound: Int): Int {
            seed = seed xor (seed shl 13); seed = seed xor (seed ushr 7); seed = seed xor (seed shl 17)
            return java.lang.Long.remainderUnsigned(seed, bound.toLong()).toInt()
        }
        val rows = ArrayList<Moment>()
        for (step in 0 until 120) {
            when (if (rows.isEmpty()) 0 else below(3)) {
                0 -> rows.add(Moment.create(text = "n%02d-%03d".format(below(40), step)))
                1 -> { val at = below(rows.size); rows[at] = rows[at].update(text = "n%02d-%03d".format(below(40), step)) }
                else -> rows.removeAt(below(rows.size)).destroy()
            }
            val fresh = byText.live.flow.first().map { it.id to it.text }
            val seen = withTimeout(20000) { changes.first { list -> list.map { it.id to it.text } == fresh } }
            assertEquals("step $step", fresh, seen.map { it.id to it.text })
        }
    }

    private fun moment(id: String, text: String) = Moment(id = id, uuid = id, text = text, likedByMe = false, media = kotlinx.serialization.json.JsonObject(emptyMap()), createdAt = "x")

    private fun json(row: Moment) = dbJson.encodeToString(Moment.serializer(), row)

    private fun change(
        size: Int, full: Boolean = false, rows: List<Moment> = emptyList(), indexes: List<UInt> = emptyList(), removed: List<String> = emptyList(),
        placed: List<Placement> = emptyList(), replaced: List<Replacement> = emptyList(),
    ) = LiveChange(1L, full, size.toUInt(), rows.map(::json), indexes, removed, placed, replaced, null, null)

    @Test
    fun inv_observe_15_the_feed_applies_each_kind_of_delta_in_order() {
        val feed = ListFeed(Moment.serializer())
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
        val feed = ListFeed(Moment.serializer())
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
        val session = ListSession<Moment, List<String>>(Moment.serializer(), { listener -> listeners.put(listener); val cancel: () -> Unit = {}; cancel }, { values.put(it) }, { applied, _ -> applied.rows.map { it.id } })
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
            Moment.serializer(),
            { listener -> opens(count.getAndIncrement()); listeners.put(listener); val cancel: () -> Unit = {}; cancel },
            { values.put(it) },
            { applied, _ -> Snapshot(applied.rows, emptyMap(), applied.reordered, applied.failure) },
        )
    }

    @Test
    fun inv_observe_15_a_row_that_does_not_decode_keeps_its_slot_and_never_reopens_the_subscription() {
        val rig = Rig()
        rig.session.start()
        val listener = poll(rig.listeners) as QueryListener
        listener.changed(LiveChange(1L, true, 3u, listOf(json(moment("a", "1")), """{"id":"b"}""", json(moment("c", "3"))), emptyList(), emptyList(), emptyList(), emptyList(), null, null))
        assertEquals(listOf("a", "c"), (poll(rig.values) as Snapshot<*>).rows.map { (it as Moment).id })
        repeat(3) {
            listener.changed(change(size = 3, rows = listOf(moment("c", "3")), indexes = listOf(2u)))
            assertEquals(listOf("a", "c"), (poll(rig.values) as Snapshot<*>).rows.map { (it as Moment).id })
        }
        assertEquals(0, rig.session.reopened)
        assertEquals(0, rig.session.desyncs)
    }

    @Test
    fun inv_observe_15_a_subscription_that_keeps_desyncing_stops_after_two_reopens_and_says_why() {
        val rig = Rig()
        rig.session.start()
        var current = poll(rig.listeners) as QueryListener
        repeat(2) {
            current.changed(change(size = 1, full = true, rows = listOf(moment("a", "1"))))
            poll(rig.values)
            current.changed(change(size = 2, placed = listOf(Placement("zz", 0u))))
            current = poll(rig.listeners) as QueryListener
        }
        current.changed(change(size = 1, full = true, rows = listOf(moment("a", "1"))))
        poll(rig.values)
        current.changed(change(size = 2, placed = listOf(Placement("zz", 0u))))
        val stopped = poll(rig.values) as Snapshot<*>
        assertEquals(true, stopped.failure != null)
        assertEquals(listOf("a"), stopped.rows.map { (it as Moment).id })
        assertEquals(2, rig.session.reopened)
        assertEquals(true, rig.session.isStalled)
    }

    @Test
    fun inv_observe_15_a_reopen_that_cannot_open_the_subscription_reports_the_failure() {
        val rig = Rig { number -> if (number > 0) throw java.io.IOException("offline") }
        rig.session.start()
        val first = poll(rig.listeners) as QueryListener
        first.changed(change(size = 1, full = true, rows = listOf(moment("a", "1"))))
        poll(rig.values)
        first.changed(change(size = 2, placed = listOf(Placement("zz", 0u))))
        val stopped = poll(rig.values) as Snapshot<*>
        assertEquals(true, stopped.failure != null)
        assertEquals(true, rig.session.isStalled)
    }
}

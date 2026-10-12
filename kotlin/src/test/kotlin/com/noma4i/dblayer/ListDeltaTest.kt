package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
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
    fun inv_observe_17_an_update_of_one_row_decodes_one_row_at_every_list_size() = runBlocking {
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
    fun inv_observe_15_the_feed_rebuilds_the_list_that_a_fresh_query_returns() = runBlocking {
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
        full: Boolean = false, rows: List<Moment> = emptyList(), indexes: List<UInt> = emptyList(), removed: List<String> = emptyList(),
        placed: List<Placement> = emptyList(), replaced: List<Replacement> = emptyList(),
    ) = LiveChange(1L, full, rows.map(::json), indexes, removed, placed, replaced, null, null)

    @Test
    fun inv_observe_15_the_feed_applies_each_kind_of_delta_in_order() {
        val feed = ListFeed(Moment.serializer())
        val first = feed.apply(change(full = true, rows = listOf(moment("a", "1"), moment("b", "2"), moment("c", "3"), moment("d", "4"))))
        assertEquals(listOf("a", "b", "c", "d"), first.rows.map { it.id })
        val updated = feed.apply(change(rows = listOf(moment("b", "2x")), indexes = listOf(1u)))
        assertEquals(false, updated.reordered)
        assertEquals(listOf("1", "2x", "3", "4"), updated.rows.map { it.text })
        val moved = feed.apply(change(placed = listOf(Placement("d", 0u))))
        assertEquals(true, moved.reordered)
        assertEquals(listOf("d", "a", "b", "c"), moved.rows.map { it.id })
        val mixed = feed.apply(change(rows = listOf(moment("e", "5"), moment("r", "6")), indexes = listOf(1u, 2u), removed = listOf("a"), placed = listOf(Placement("e", 1u), Placement("c", 3u)), replaced = listOf(Replacement("b", "r"))))
        assertEquals(listOf("d", "e", "r", "c"), mixed.rows.map { it.id })
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import kotlinx.coroutines.cancel
import org.junit.Test

class ListEditTest {
    private fun moment(id: String, text: String = id) = Moment(id = id, uuid = id, text = text, likedByMe = false, media = kotlinx.serialization.json.JsonObject(emptyMap()), createdAt = "x")

    private fun wire(row: Moment, cells: List<Cell>? = null) = WireRow(
        row.id, 1L, 1uL, Moment.layout,
        cells ?: listOf(Cell.Text(row.uuid), Cell.Null, Cell.Text(row.text ?: ""), Cell.Bool(false), Cell.Keyed(emptyList()), Cell.Null, Cell.Text(row.createdAt)),
        WireSync(WireSyncState.SYNCED, emptyList(), emptyList(), null, 0u, 0L), null,
    )

    private fun change(
        size: Int, full: Boolean = false, rows: List<Moment> = emptyList(), indexes: List<UInt> = emptyList(), removed: List<String> = emptyList(),
        placed: List<Placement> = emptyList(), replaced: List<Replacement> = emptyList(),
    ) = LiveChange(1L, full, size.toUInt(), rows.map { wire(it) }, indexes, removed, placed, replaced, replaced, null, null)

    private fun snapshot(feed: ListFeed<Moment>, change: LiveChange): Snapshot<Moment> {
        val applied = feed.apply(change)
        return Snapshot(applied.rows, change.replacedIds(), applied.reordered, applied.edits, error = applied.failure)
    }

    private fun texts(rows: List<Moment>) = rows.map { "${it.id}:${it.text}" }

    @Test
    fun one_update_one_append_and_one_remove_are_one_edit_each_at_any_list_size() {
        for (size in listOf(20, 200, 2000)) {
            val feed = ListFeed(Moment)
            feed.apply(change(size, full = true, rows = (0 until size).map { moment("m$it") }))
            val middle = size / 2

            val update = feed.apply(change(size, rows = listOf(moment("m$middle", "new")), indexes = listOf(middle.toUInt())))
            val append = feed.apply(change(size + 1, rows = listOf(moment("x")), indexes = listOf(size.toUInt()), placed = listOf(Placement("x", size.toUInt()))))
            val remove = feed.apply(change(size, removed = listOf("m1")))

            assertEquals("update at $size", 1, update.edits?.size)
            assertEquals("append at $size", 1, append.edits?.size)
            assertEquals("remove at $size", 1, remove.edits?.size)
            assertEquals("new", (update.edits!![0] as ListEdit.Rewrite).row.text)
        }
    }

    @Test
    fun several_deliveries_merged_replay_to_the_final_rows_whatever_they_did_to_one_id() {
        val steps = listOf(
            "update then move then remove" to listOf(
                change(13, rows = listOf(moment("a", "a1")), indexes = listOf(0u)),
                change(13, rows = listOf(moment("a", "a2")), indexes = listOf(12u), placed = listOf(Placement("a", 12u))),
                change(12, removed = listOf("a")),
            ),
            "the same id updated three times" to listOf(
                change(13, rows = listOf(moment("b", "b1")), indexes = listOf(1u)),
                change(13, rows = listOf(moment("b", "b2")), indexes = listOf(1u)),
                change(13, rows = listOf(moment("b", "b3")), indexes = listOf(1u)),
            ),
            "temporary row becomes real then reorders" to listOf(
                change(14, rows = listOf(moment("t1")), indexes = listOf(13u), placed = listOf(Placement("t1", 13u))),
                change(14, rows = listOf(moment("r1")), indexes = listOf(13u), replaced = listOf(Replacement("t1", "r1"))),
                change(14, rows = listOf(moment("r1", "r1x")), indexes = listOf(0u), placed = listOf(Placement("r1", 0u))),
            ),
        )
        for ((name, changes) in steps) {
            val feed = ListFeed(Moment)
            val ids = listOf("a", "b", "c") + (0 until 10).map { "f$it" }
            val base = feed.apply(change(ids.size, full = true, rows = ids.map { moment(it) })).rows
            val deliveries = changes.map { snapshot(feed, it) }
            val merged = deliveries.drop(1).fold(deliveries[0]) { older, newer -> Snapshot.merging(older, newer) }
            val mirror = base.toMutableList()
            mirror.replay(requireNotNull(merged.edits) { "$name has edits" })

            assertEquals(name, texts(feed.rows), texts(mirror))
            assertEquals(name, texts(merged.rows), texts(mirror))
        }
    }

    @Test
    fun several_rows_removed_by_one_delivery_replay_to_the_rows_that_stay() {
        val feed = ListFeed(Moment)
        val base = feed.apply(change(6, full = true, rows = listOf("a", "b", "c", "d", "e", "f").map { moment(it) })).rows

        val removed = snapshot(feed, change(4, removed = listOf("b", "d")))
        val mirror = base.toMutableList()
        mirror.replay(requireNotNull(removed.edits))

        assertEquals(listOf("a", "c", "e", "f"), mirror.map { it.id })
    }

    @Test
    fun a_whole_list_and_too_many_edits_are_delivered_without_edits() {
        val feed = ListFeed(Moment)
        val whole = snapshot(feed, change(2, full = true, rows = listOf(moment("a"), moment("b"))))
        val one = snapshot(feed, change(2, rows = listOf(moment("a", "1")), indexes = listOf(0u)))
        val two = snapshot(feed, change(2, rows = listOf(moment("b", "2")), indexes = listOf(1u)))
        val three = snapshot(feed, change(2, rows = listOf(moment("a", "3")), indexes = listOf(0u)))

        assertNull(whole.edits)
        assertNull(Snapshot.merging(whole, one).edits)
        assertEquals(2, Snapshot.merging(one, two).edits?.size)
        assertNull(Snapshot.merging(Snapshot.merging(one, two), three).edits)
    }

    @Test
    fun a_row_that_does_not_decode_turns_the_edits_off_until_the_reader_takes_the_whole_list() {
        val feed = ListFeed(Moment)
        val broken = LiveChange(1L, true, 3u, listOf(wire(moment("a")), wire(moment("b"), emptyList()), wire(moment("c"))), emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), null, null)
        feed.apply(broken)

        val update = feed.apply(change(3, rows = listOf(moment("c", "c1")), indexes = listOf(2u)))

        assertNull(update.edits)
        assertEquals(listOf("a", "c"), update.rows.map { it.id })
    }

    @Test
    fun the_list_hands_out_the_edits_of_every_delivery_the_reader_missed() = bounded {
        reopen(Config(null, null, 64u), modelsJSON, null)
        val created = (0 until 5).map { Moment.create(text = "t$it") }
        val scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.Dispatchers.Default)
        val list = LiveList(Query("Moment", Moment).order(Order("text", "asc")), scope)
        suspend fun waitFor(what: String, done: (List<Moment>) -> Boolean) = eventually(what, 20_000) { done(list.rows.value) }
        waitFor("the whole list") { it.size == 5 }
        assertNull(list.takeUpdate().edits)
        val mirror = list.rows.value.toMutableList()

        created[0].update(text = GraphQLNullable.Value("z0"))
        created[3].update(text = GraphQLNullable.Value("t3x"))
        waitFor("both updates") { rows -> rows.any { it.text == "z0" } && rows.any { it.text == "t3x" } }
        val update = list.takeUpdate()
        mirror.replay(update.edits!!)

        assertEquals(texts(list.rows.value), texts(mirror))
        assertEquals(texts(update.rows), texts(mirror))
        assertEquals(emptyList<ListEdit<Moment>>(), list.takeUpdate().edits)
        scope.cancel()
    }

    @Test
    fun a_full_delivery_copies_the_list_once_and_a_delta_copies_it_once_at_every_size() {
        val copies = listOf(100, 1000).map { size ->
            val feed = ListFeed(Moment)
            val all = (0 until size).map { moment("m%05d".format(it)) }
            feed.apply(change(size, full = true, rows = all))
            val afterFull = feed.copiedRows
            feed.apply(change(size, rows = listOf(moment("m00050", "edited")), indexes = listOf(50u)))
            Pair(afterFull, feed.copiedRows - afterFull)
        }
        assertEquals(listOf(Pair(100, 100), Pair(1000, 1000)), copies)
    }
}

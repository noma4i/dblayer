package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.TimeUnit

class ListUpdatesTest {
    private fun snapshot(revision: Int, size: Int) =
        Snapshot(List(size) { revision }, emptyMap(), edits = listOf(ListEdit.Rewrite(0, revision)), seq = revision.toULong())

    @Test
    fun a_consumer_that_races_the_publisher_gets_every_edit_once_and_rows_of_one_revision() {
        val revisions = 3000
        val updates = ListUpdates<Int>()
        val covered = HashSet<Int>()
        val problems = java.util.concurrent.ConcurrentLinkedQueue<String>()
        val publisher = Thread { for (revision in 1..revisions) updates.publish(snapshot(revision, revisions)) }

        fun consume() {
            val update = updates.take()
            if (update.rows.any { it != update.revision.toInt() }) problems += "rows of revision ${update.revision} mix revisions"
            val seen = update.edits?.map { (it as ListEdit.Rewrite).row } ?: (1..update.revision.toInt()).toList()
            for (revision in seen) if (!covered.add(revision)) problems += "revision $revision taken twice"
        }

        publisher.start()
        while (publisher.isAlive) consume()
        publisher.join(10_000)
        consume()

        assertEquals(emptyList<String>(), problems.toList().take(3))
        assertEquals((1..revisions).toSet(), covered)
        assertEquals(emptyList<ListEdit<Int>>(), updates.take().edits)
        assertTrue(publisher.join(1).let { !publisher.isAlive })
    }
}

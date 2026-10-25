package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class ListUpdatesTest {
    private val size = 4

    private fun rows(revision: Int) = List(size) { if (it < revision) it + 1 else 0 }

    private fun snapshot(revision: Int, edits: List<ListEdit<Int>> = listOf(ListEdit.Rewrite(revision - 1, revision))) =
        Snapshot(rows(revision), emptyMap(), edits = edits, seq = revision.toULong())

    private fun held(edits: List<ListEdit<Int>>, entered: CountDownLatch, gate: CountDownLatch) = object : AbstractList<ListEdit<Int>>() {
        override val size get() = edits.size.also { entered.countDown(); gate.await(5, TimeUnit.SECONDS) }
        override fun get(index: Int) = edits[index]
    }

    @Test
    fun the_edits_taken_replay_to_the_rows_of_the_same_revision_when_a_take_overlaps_a_publication() {
        val updates = ListUpdates<Int>()
        updates.publish(snapshot(1))
        val first = updates.take()
        assertNull("before the first take the rows are replaced whole", first.edits)
        val mirror = first.rows.toMutableList()

        updates.publish(snapshot(2))
        updates.publish(snapshot(3))
        val missed = updates.take()
        mirror.replay(missed.edits!!)
        assertEquals(missed.rows, mirror)
        assertEquals(3uL, missed.revision)

        val entered = CountDownLatch(1)
        val gate = CountDownLatch(1)
        val publisher = Thread { updates.publish(snapshot(4, held(listOf(ListEdit.Rewrite(3, 4)), entered, gate))) }
        try {
            publisher.start()
            assertTrue("the publication is composing its edits", entered.await(5, TimeUnit.SECONDS))
            val overlapped = updates.take()
            assertEquals(emptyList<ListEdit<Int>>(), overlapped.edits)
            assertEquals(3uL, overlapped.revision)
        } finally {
            gate.countDown()
            publisher.join(5000)
            assertFalse("the publisher ended", publisher.isAlive)
        }

        val last = updates.take()
        mirror.replay(last.edits!!)
        assertEquals(last.rows, mirror)
        assertEquals(4uL, last.revision)
        assertEquals(emptyList<ListEdit<Int>>(), updates.take().edits)
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import org.junit.Assert.assertEquals
import org.junit.Test

/** A projection mirrors the deliveries of the core: it applies each one as it comes and keeps only the edits of the last one. */
class ProjectionRecoveryTest {
    private val state = ReadState(ReadPhase.LOADED, null, null, false, true, null)

    @Test
    fun aDeltaIsAppliedAsItComesAndKeepsOnlyItsOwnEdits() {
        val first = ListProjection<Moment>().apply(ReadValue.Snapshot(1, emptyList(), emptyList(), state), Moment)!!
        assertEquals(emptyList<ListEdit<Moment>>(), first.lastEdits)
        val second = first.apply(ReadValue.Delta(1, 300, emptyList(), state), Moment)!!
        assertEquals(300L, second.revision)
        assertEquals(emptyList<ListEdit<Moment>>(), second.lastEdits)
    }

    @Test
    fun theSnapshotEditsComeFromTheCore() {
        val applied = ListProjection<Moment>().apply(ReadValue.Snapshot(1, emptyList(), listOf(RowEdit.Remove(0u, "m1"), RowEdit.Move(1u, 0u)), state), Moment)!!
        assertEquals(listOf<ListEdit<Moment>>(ListEdit.Remove(0, "m1"), ListEdit.Move(1, 0)), applied.lastEdits)
    }
}

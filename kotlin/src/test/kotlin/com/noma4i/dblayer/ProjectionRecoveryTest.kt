package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

/** A projection mirrors the deliveries of the core: it applies each one as it comes and keeps only the edits of the last one. */
class ProjectionRecoveryTest {
    private val state = ReadState(ReadPhase.LOADED, null, null, false, true, null)

    @Test
    fun aDeltaIsAppliedAsItComesAndKeepsOnlyItsOwnEdits() {
        val first = ListProjection<Moment>().apply(ReadValue.Snapshot(1, emptyList(), state), Moment)!!
        assertNull(first.lastEdits)
        val second = first.apply(ReadValue.Delta(1, 300, emptyList(), state), Moment)!!
        assertEquals(300L, second.revision)
        assertEquals(emptyList<ListEdit<Moment>>(), second.lastEdits)
    }
}

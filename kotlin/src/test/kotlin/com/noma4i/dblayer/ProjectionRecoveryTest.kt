package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.Assert.assertThrows

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

/** bridge_prop_projection_revision (P-BR-06), bridge_prop_numeric_domain (P-BR-07). */
class ProjectionDomainTest {
    private val state = ReadState(ReadPhase.LOADED, null, null, false, true, null)

    @Test
    fun bridge_prop_projection_revision_theDeltaKeepsItsBaseAndTheStateKeepsItsRevision() {
        var projection = ListProjection<Moment>().apply(ListDelivery.Snapshot(1, emptyList(), emptyList(), state))
        projection = projection.apply(ListDelivery.Delta(1, 300, emptyList(), state))
        assertEquals(1L to 300L, projection.base to projection.revision)
        projection = projection.apply(ListDelivery.State(301, state))
        assertEquals(1L to 301L, projection.base to projection.revision)
    }

    @Test
    fun bridge_prop_projection_revision_theCountIsOneValueOfOneDelivery() {
        val projection = CountProjection(null, null, null, null).apply(ReadValue.Count(7, ULong.MAX_VALUE, state))
        assertEquals(Triple(ULong.MAX_VALUE, 7L, state), Triple(projection.count, projection.revision, projection.state))
    }

    @Test
    fun bridge_prop_numeric_domain_aQueryWindowBelowZeroIsInvalidInsteadOfWrapped() {
        val query = Query("Moment", Moment)
        for (bad in listOf(query.limit(-1), query.offset(-1))) {
            assertThrows(DbException.Invalid::class.java) { bad.spec() }
        }
        assertEquals(0u, query.limit(0).spec().limit)
    }
}

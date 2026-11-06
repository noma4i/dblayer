package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import org.junit.Assert.assertEquals
import org.junit.Test

/** C09: a reader learns of a gap from the base of a delta, not from the positions of the deliveries. A position counts entries of the core journal, and the core replaces dropped list values with one snapshot. */
class ProjectionRecoveryTest {
    private val state = ReadState(ReadPhase.LOADED, null, null, false, true, null)

    private fun snapshot(revision: Long): ReadValue = ReadValue.Snapshot(revision, emptyList(), state)

    private fun delta(base: Long, revision: Long): ReadValue =
        ReadValue.Delta(base, revision, 0u, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), state)

    private fun ListProjection<Moment>.read(value: ReadValue): Pair<ListProjection<Moment>, Applied> = apply(value, Moment).let { it.projection to it.applied }

    @Test
    fun aDeltaOffTheShownRevisionAsksOnceForASnapshotAndTheSnapshotClosesTheGap() {
        var projection = ListProjection<Moment>()
        fun feed(value: ReadValue): Applied = projection.read(value).let { projection = it.first; it.second }
        assertEquals(Applied.Changed, feed(snapshot(1)))
        assertEquals(Applied.Changed, feed(delta(1, 2)))
        assertEquals("a delta off the shown revision must ask for a snapshot", Applied.Resync, feed(delta(3, 4)))
        assertEquals("one request per gap: the next delta waits for the snapshot", Applied.Ignored, feed(delta(4, 5)))
        assertEquals(Applied.Changed, feed(snapshot(5)))
        assertEquals(Applied.Changed, feed(delta(5, 6)))
    }

    @Test
    fun aJumpOfThePositionWithTheShownBaseAppliesTheDelta() {
        var projection = ListProjection<Moment>()
        projection = projection.read(snapshot(1)).first
        assertEquals("a jump of the revision with the shown base is no gap", Applied.Changed, projection.read(delta(1, 300)).second)
    }
}

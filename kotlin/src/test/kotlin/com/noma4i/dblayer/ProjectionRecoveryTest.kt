package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import org.junit.Assert.assertEquals
import org.junit.Test

/** C09: a reader learns of a gap from the base of a delta, not from the positions of the deliveries. A position counts entries of the core journal, and the core replaces dropped list values with one snapshot. */
class ProjectionRecoveryTest {
    private val state = StreamState(LivePhase.LOADED, true, 0, false, false, false, false, true, null, null, null, null)

    private fun snapshot(seq: ULong, revision: Long) = StreamDelivery(1uL, 1uL, seq, StreamValue.Snapshot(revision, emptyList(), state))

    private fun delta(seq: ULong, base: Long, revision: Long) =
        StreamDelivery(1uL, 1uL, seq, StreamValue.Delta(base, revision, 0u, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), state))

    private fun ListProjection<Moment>.read(delivery: StreamDelivery): Pair<ListProjection<Moment>, Applied> = apply(delivery, Moment).let { it.projection to it.applied }

    @Test
    fun aDeltaOffTheShownRevisionAsksOnceForASnapshotAndTheSnapshotClosesTheGap() {
        var projection = ListProjection<Moment>()
        fun feed(delivery: StreamDelivery): Applied = projection.read(delivery).let { projection = it.first; it.second }
        assertEquals(Applied.Changed, feed(snapshot(1uL, 1)))
        assertEquals(Applied.Changed, feed(delta(2uL, 1, 2)))
        assertEquals("a delta off the shown revision must ask for a snapshot", Applied.Resync, feed(delta(3uL, 3, 4)))
        assertEquals("one request per gap: the next delta waits for the snapshot", Applied.Ignored, feed(delta(4uL, 4, 5)))
        assertEquals(Applied.Changed, feed(snapshot(5uL, 5)))
        assertEquals(Applied.Changed, feed(delta(6uL, 5, 6)))
    }

    @Test
    fun aJumpOfThePositionWithTheShownBaseAppliesTheDelta() {
        var projection = ListProjection<Moment>()
        projection = projection.read(snapshot(1uL, 1)).first
        assertEquals("a position counts journal entries: a jump with the shown base is no gap", Applied.Changed, projection.read(delta(300uL, 1, 2)).second)
    }
}

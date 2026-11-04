package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import org.junit.Assert.assertEquals
import org.junit.Test

/** C09: a reader that fell behind reads a jump in the sequence of the deliveries. A jump means that deliveries were dropped, also the snapshot that was asked for. */
class ProjectionRecoveryTest {
    private val state = StreamState(LivePhase.LOADED, true, 0, false, false, false, false, true, null, null, null, null)

    private fun snapshot(seq: ULong, revision: Long) = StreamDelivery(1uL, 1uL, seq, StreamValue.Snapshot(revision, emptyList(), state))

    private fun delta(seq: ULong, base: Long, revision: Long) =
        StreamDelivery(1uL, 1uL, seq, StreamValue.Delta(base, revision, 0u, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), state))

    private fun ListProjection<Moment>.read(delivery: StreamDelivery): Pair<ListProjection<Moment>, Applied> = apply(delivery, Moment).let { it.projection to it.applied }

    @Test
    fun aLostRecoverySnapshotAsksAgainAndTheNextSnapshotClosesTheGap() {
        var projection = ListProjection<Moment>()
        fun feed(delivery: StreamDelivery): Applied = projection.read(delivery).let { projection = it.first; it.second }
        assertEquals(Applied.Changed, feed(snapshot(1uL, 1)))
        assertEquals(Applied.Changed, feed(delta(2uL, 1, 2)))
        assertEquals("a delta behind a gap must ask for a snapshot", Applied.Resync, feed(delta(4uL, 3, 4)))
        assertEquals("the recovery snapshot was dropped: the projection must ask again", Applied.Resync, feed(delta(7uL, 6, 7)))
        assertEquals("one request per gap: a delta without a jump waits for the snapshot", Applied.Ignored, feed(delta(8uL, 7, 8)))
        assertEquals(Applied.Changed, feed(snapshot(9uL, 9)))
        assertEquals(Applied.Changed, feed(delta(10uL, 9, 10)))
    }

    @Test
    fun aJumpAtTheFirstDeliveryReadAsksForASnapshotAlthoughTheBaseMatches() {
        var projection = ListProjection<Moment>()
        projection = projection.read(snapshot(1uL, 1)).first
        assertEquals("deliveries before this one were dropped: the rows may be stale", Applied.Resync, projection.read(delta(300uL, 1, 2)).second)
    }
}

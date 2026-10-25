package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

class FirstSnapshotTest {
    private fun wire(id: String, layout: String = Moment.layout) = WireRow(id, 1L, 1uL, layout,
        listOf(Cell.Text(id), Cell.Null, Cell.Text(id), Cell.Bool(false), Cell.Keyed(emptyList()), Cell.Null, Cell.Text("x")),
        WireSync(WireSyncState.SYNCED, emptyList(), emptyList(), null, 0u, 0L), emptyList())

    @Test
    fun aSnapshotKeepsTheTypedFailureWithItsRowsAndPhase() {
        var older: Snapshot<Moment>? = null
        for (failure in listOf<DbException>(DbException.Session(SessionCode.UNAUTHENTICATED, "session_unknown"), DbException.Http(503u, "busy"))) {
            val change = LiveChange(1L, true, 1u, listOf(wire("1")), emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), null, failure, inserted = listOf(0u), phase = LivePhase.FAILED, errorKind = "session")
            val snapshot = Snapshot.of(ListFeed(Moment).apply(change), change)
            assertEquals(listOf("1"), snapshot.rows.map { it.id })
            assertEquals(LoadPhase.Failed, snapshot.phase)
            assertEquals("session", snapshot.errorKind)
            when (failure) {
                is DbException.Session -> { val kept = snapshot.error as DbException.Session; assertEquals(failure.code, kept.code); assertEquals(failure.detail, kept.detail) }
                is DbException.Http -> assertEquals(failure.status, (snapshot.error as DbException.Http).status)
                else -> throw AssertionError("unexpected $failure")
            }
            older?.let { assertEquals(snapshot.error, Snapshot.merging(it, snapshot).error) }
            older = snapshot
        }
        val decoded = LiveChange(1L, true, 1u, listOf(wire("2", "other")), emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), null, null, inserted = listOf(0u))
        assertEquals(true, Snapshot.of(ListFeed(Moment).apply(decoded), decoded).error is DbException.Invalid)
    }

    @Test
    fun aListThatCannotAttachShowsTheFailureInItsSnapshot() = bounded {
        reopen(Config(null, null, 8u), modelsJSON, null)
        DBlayer.close()
        val scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob())
        try {
            val list = LiveList(Moment.recent, scope)

            val failed = withTimeout(5000) { list.snapshot.first { it.phase == LoadPhase.Failed } }

            assertEquals("subscription", failed.errorKind)
            assertEquals(true, failed.error is IllegalStateException)
        } finally {
            scope.cancel()
        }
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import java.io.File
import java.nio.file.Files
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

class FirstSnapshotTest {
    private val operations = """[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"""

    @Test
    fun theFirstValueOfAWindowLoadedByAnEarlierRunHoldsItsRowsAndTheLoadedPhase() = bounded {
        val directory = Files.createTempDirectory("dblayer-first-snapshot").toFile()
        try {
            val config = Config(File(directory, "db.sqlite").path, null, 32u)
            val earlier = MockTransport()
            earlier.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
            reopen(config, modelsJSON, earlier)
            DBlayer.database.registerOperations(operations)
            Moment.feed.load()

            reopen(config, modelsJSON, MockTransport())
            DBlayer.database.registerOperations(operations)
            val first = withTimeout(5000) { Moment.feed.snapshots.flow.first() }

            assertEquals(LoadPhase.Loaded, first.phase)
            assertEquals(listOf("a"), first.rows.map { it.uuid })
            assertEquals(true, first.loadedAt != null)
        } finally {
            directory.deleteRecursively()
        }
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
            assertEquals(true, failed.error != null)
        } finally {
            scope.cancel()
        }
    }
}

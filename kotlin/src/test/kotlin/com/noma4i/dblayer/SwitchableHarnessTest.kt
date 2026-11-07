package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/** The transport of the app harness: a target that the tests set and unset. An unset target throws the error the app harness throws. */
class SwitchableTransport(@Volatile var target: MockTransport?, private val unset: () -> DbException) : ScriptedTransport {
    private fun resolve(): MockTransport = target ?: throw unset()

    override suspend fun query(request: TransportRequest): String = resolve().query(request)
    override suspend fun mutate(request: TransportRequest): String = resolve().mutate(request)
    override suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome = resolve().scriptedCable(url, events)
    override suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply = resolve().scriptedPut(url, headers, file, identity, progress)
}

@OptIn(DBlayerTestingApi::class)
class SwitchableHarnessTest {
    private fun profile(name: String) = """{"data":{"me":{"__typename":"User","id":"1","name":"$name"}}}"""

    private val operations = """[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"""

    /** C01, C02, R-1, R-3: the target unset (Android throws Interrupted, iOS Invalid), the database closes, the next test opens a new one on the next target. The close sends nothing to either transport and the first load of the next test succeeds with its own account. */
    @Test
    fun aCloseWithTheTargetUnsetLeavesTheNextDatabaseClean() = bounded {
        for (unset in listOf<() -> DbException>({ DbException.Interrupted("transport unavailable during reset") }, { DbException.Invalid("No test transport") })) {
            closeForCleanup()
            try {
                val first = MockTransport(Unanswered.NetworkError)
                first.enqueue("UserProfile", profile("A"))
                val switchable = SwitchableTransport(first, unset)
                reopen(Config(null, null), modelsJSON, switchable, operations)
                val entity = heldUser()
                entity.until("the held entity shows A") { it.rows.firstOrNull()?.name == "A" }
                val before = first.calls.size

                switchable.target = null
                closeForCleanup()
                val second = MockTransport(Unanswered.NetworkError)
                second.enqueue("UserProfile", profile("B"))
                assertEquals("work of the close reached the old transport", before, first.calls.size)
                assertTrue("work of the close reached the next transport", second.calls.isEmpty())
                switchable.target = second
                reopen(Config(null, null), modelsJSON, switchable, operations)

                val next = heldUser()
                next.loaded()
                assertEquals("B", next.value?.name)
                assertEquals(1, second.calls.size)
            } finally {
                closeForCleanup()
            }
        }
    }

    /** C04, R-2, R-4: close, then open again without a transport (the iOS reopen), then with one (the Android direct open). The last open reads its own transport. */
    @Test
    fun reopeningAfterACloseGivesACleanDatabaseWithAndWithoutATransport() = bounded {
        closeForCleanup()
        try {
            val first = MockTransport(Unanswered.NetworkError)
            first.enqueue("UserProfile", profile("A"))
            reopen(Config(null, null), modelsJSON, first, operations)
            val entity = heldUser()
            entity.until("the held entity shows A") { it.rows.firstOrNull()?.name == "A" }
            closeForCleanup()
            val closed = first.calls.size

            DBlayer.openTesting(null, modelsJSON)
            closeForCleanup()

            val second = MockTransport(Unanswered.NetworkError)
            second.enqueue("UserProfile", profile("B"))
            reopen(Config(null, null), modelsJSON, second, operations)
            val reopened = heldUser()
            reopened.until("the reopened entity shows B") { it.rows.firstOrNull()?.name == "B" }
            assertEquals("the first transport saw: ${first.calls.map { it.operationName }}", 1, first.calls.count { it.operationName == "UserProfile" })
            assertEquals("work after the close reached the first transport: ${first.calls.map { it.operationName }}", closed, first.calls.size)
        } finally {
            closeForCleanup()
        }
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/** The transport of the app harness: a target that the tests set and unset. An unset target throws the error the app harness throws. */
class SwitchableTransport(@Volatile var target: MockTransport?, private val unset: () -> DbException) : Transport {
    private fun resolve(): MockTransport = target ?: throw unset()

    override suspend fun openCable(lease: CableLease) = resolve().openCable(lease)
    override suspend fun cableSend(token: String, text: String) = resolve().cableSend(token, text)
    override suspend fun cableNext(token: String): EncryptedSocketEvent? = target?.cableNext(token)
    override suspend fun cableClose(token: String) { target?.cableClose(token) }
    override suspend fun query(request: TransportRequest): String = resolve().query(request)
    override suspend fun mutate(request: TransportRequest): String = resolve().mutate(request)
    override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink) = resolve().subscribe(request, sink)
    override fun unsubscribe(token: String) { target?.unsubscribe(token) }
    override suspend fun put(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply = resolve().put(url, headers, file, identity, progress)
}

@OptIn(DBlayerTestingApi::class)
class SwitchableHarnessTest {
    private fun profile(name: String) = """{"data":{"me":{"__typename":"User","id":"1","name":"$name"}}}"""

    private val operations = """[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"""

    /** C01, C02, R-1, R-3: the target unset (Android throws Interrupted, iOS Invalid), the database closes, the next test opens a new one on the next target. The close sends nothing to either transport and the first load of the next test succeeds with its own account. */
    @Test
    fun aCloseWithTheTargetUnsetLeavesTheNextDatabaseClean() = bounded {
        for (unset in listOf<() -> DbException>({ DbException.Interrupted("transport unavailable during reset") }, { DbException.Invalid("No test transport") })) {
            DBlayer.close()
            try {
                val first = MockTransport(Unanswered.NetworkError)
                first.enqueue("UserProfile", profile("A"))
                val switchable = SwitchableTransport(first, unset)
                DBlayer.openTesting(switchable, modelsJSON)
                DBlayer.database.registerOperations(operations)
                val entity = heldUser()
                eventually("the held entity shows A") { entity.value?.name == "A" }
                val before = first.calls.size

                switchable.target = null
                DBlayer.close()
                val second = MockTransport(Unanswered.NetworkError)
                second.enqueue("UserProfile", profile("B"))
                assertEquals("work of the close reached the old transport", before, first.calls.size)
                assertTrue("work of the close reached the next transport", second.calls.isEmpty())
                switchable.target = second
                DBlayer.openTesting(switchable, modelsJSON)
                DBlayer.database.registerOperations(operations)

                val next = heldUser()
                next.loaded()
                assertEquals("B", next.value?.name)
                assertEquals(1, second.calls.size)
            } finally {
                DBlayer.close()
            }
        }
    }

    /** C04, R-2, R-4: close, then open again without a transport (the iOS reopen), then with one (the Android direct open). The last open reads its own transport. */
    @Test
    fun reopeningAfterACloseGivesACleanDatabaseWithAndWithoutATransport() = bounded {
        DBlayer.close()
        try {
            val first = MockTransport(Unanswered.NetworkError)
            first.enqueue("UserProfile", profile("A"))
            DBlayer.openTesting(first, modelsJSON)
            DBlayer.database.registerOperations(operations)
            val entity = heldUser()
            eventually("the held entity shows A") { entity.value?.name == "A" }
            DBlayer.close()

            DBlayer.openTesting(null, modelsJSON)
            DBlayer.close()

            val second = MockTransport(Unanswered.NetworkError)
            second.enqueue("UserProfile", profile("B"))
            DBlayer.openTesting(second, modelsJSON)
            DBlayer.database.registerOperations(operations)
            val reopened = heldUser()
            eventually("the reopened entity shows B") { reopened.value?.name == "B" }
            assertEquals(1, first.calls.size)
        } finally {
            DBlayer.close()
        }
    }
}

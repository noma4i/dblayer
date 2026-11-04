package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(DBlayerTestingApi::class)
class DestroyRereadTest {
    @Test
    fun aMissingDestroyedRowCausesOneRereadOfTheOpenList() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            DBlayer.close()
            val transport = MockTransport()
            for ((id, uuid) in listOf("1" to "a", "2" to "b")) {
                transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"$id","uuid":"$uuid","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
            }
            transport.enqueue("DeleteMoment", """{"errors":[{"message":"gone","extensions":{"code":"NOT_FOUND"}}]}""")
            DBlayer.openTesting(transport, modelsJSON, listening = false)
            DBlayer.database.registerOperations("""[{"name":"MomentsFeed","document":"query MomentsFeed(${'$'}first: Int, ${'$'}after: String) { feed(first: ${'$'}first, after: ${'$'}after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"DeleteMoment","document":"mutation DeleteMoment(${'$'}id: ID!) { deleteMoment(id: ${'$'}id) { id } }"}]""")
            val held = Projected(AccountScope.current().stream(Moment.feed), Moment, scope)
            held.loaded()
            assertEquals("1", held.value?.id)
            val receipt = Moment.destroy("1")
            assertTrue(receipt.outcome() is OperationOutcome.Confirmed)
            awaitWithin("the reread is shown") { held.projection.first { projection -> projection.rows.map { it.id } == listOf("2") } }
            assertEquals(2, transport.calls.count { it.operationName == "MomentsFeed" })
            held.close()
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }
}

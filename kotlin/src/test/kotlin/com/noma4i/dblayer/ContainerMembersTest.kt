package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ContainerMembersTest {
    private val document = "query ContainerMembers(\$containerId: ID!, \$first: Int, \$after: Cursor) { node(id: \$containerId) { ... on Container { membersCount members(first: \$first, after: \$after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"

    private fun page(id: String, hasNext: Boolean): String =
        """{"data":{"node":{"membersCount":53,"members":{"edges":[{"cursor":"$id","node":{"id":"$id","name":"Member"}}],"pageInfo":{"hasNextPage":$hasNext,"endCursor":"$id"}}}}}"""

    @Test
    fun containerMembersPassesArgumentsAndDecodesTypedSnapshots() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 64u), modelsJSON, transport)
        val def = buildJsonObject { put("name", JsonPrimitive("ContainerMembers")); put("document", JsonPrimitive(document)) }
        DBlayer.database.registerOperations(JsonArray(listOf(def)).toString())
        transport.enqueue("ContainerMembers", page("a0", true))
        transport.enqueue("ContainerMembers", page("a1", false))

        val first = User.containerMembers("c1")
        first.load()
        val counted: Snapshot<User> = withTimeout(5000) { first.snapshots.flow.first { it.total == 53L } }
        val total: Long? = counted.total
        assertEquals(53L, total)
        assertEquals(listOf("a0"), counted.rows.map { it.id })
        assertEquals("Member", counted.rows.single().name)
        assertTrue(counted.hasMore)
        assertEquals(LoadMoreResult.Loaded(false), first.loadMore())
        val calls = transport.calls.filter { it.operationName == "ContainerMembers" }
        assertEquals(
            listOf("""{"containerId":"c1","first":30}""", """{"containerId":"c1","first":30,"after":"a0"}""").map { Json.parseToJsonElement(it) },
            calls.map { Json.parseToJsonElement(it.variablesJson) },
        )
    }

    @Test
    fun loadAllReadsEveryPageAndForceRestartsTheList() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 64u), modelsJSON, transport)
        val def = buildJsonObject { put("name", JsonPrimitive("ContainerMembers")); put("document", JsonPrimitive(document)) }
        DBlayer.database.registerOperations(JsonArray(listOf(def)).toString())
        transport.enqueue("ContainerMembers", page("a0", true))
        transport.enqueue("ContainerMembers", page("a1", false))
        transport.enqueue("ContainerMembers", page("a2", false))

        val members = User.containerMembers("c1")
        members.loadAll()
        members.loadAll(force = true)
        val calls = transport.calls.filter { it.operationName == "ContainerMembers" }
        assertEquals(
            listOf("""{"containerId":"c1","first":30}""", """{"containerId":"c1","first":30,"after":"a0"}""", """{"containerId":"c1","first":30}""").map { Json.parseToJsonElement(it) },
            calls.map { Json.parseToJsonElement(it.variablesJson) },
        )
    }

    @Test
    fun membersSnapshotPlatforms() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 64u), modelsJSON, transport)
        val def = buildJsonObject { put("name", JsonPrimitive("ContainerMembers")); put("document", JsonPrimitive(document)) }
        DBlayer.database.registerOperations(JsonArray(listOf(def)).toString())
        transport.enqueue("ContainerMembers", page("a0", true))
        transport.enqueue("ContainerMembers", page("a1", false))
        val query = User.completeMembers("c1")
        val scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.Dispatchers.Default)
        try {
            val members = LiveList(query, scope)
            eventually("complete members") { members.snapshot.value.complete }
            assertEquals(LoadPhase.Loaded, members.snapshot.value.phase)
            assertEquals(listOf("a0", "a1"), members.snapshot.value.rows.map { it.id })
            assertEquals(null, members.snapshot.value.error)
            assertEquals(members.rows.value.map { it.id }, members.snapshot.value.rows.map { it.id })
            transport.enqueue("ContainerMembers", """{"errors":[{"message":"refused"}]}""")
            try { query.refresh(); org.junit.Assert.fail("a refused refresh must throw") } catch (_: DbException) {}
            eventually("members error") { members.snapshot.value.error != null }
            assertEquals(LoadPhase.Loaded, members.snapshot.value.phase)
            assertEquals(false, members.snapshot.value.complete)
            assertEquals(listOf("a0", "a1"), members.snapshot.value.rows.map { it.id })
            assertTrue(members.snapshot.value.error != null)
        } finally { scope.coroutineContext[kotlinx.coroutines.Job]?.cancel() }
    }
}

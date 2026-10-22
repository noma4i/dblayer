package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ContainerMembersTest {
    private val document = "query ContainerMembers(\$containerId: ID!, \$first: Int, \$after: Cursor) { node(id: \$containerId) { ... on Container { members(first: \$first, after: \$after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"

    private fun page(prefix: String, range: IntRange, hasNext: Boolean): String {
        val edges = range.joinToString(",") { """{"cursor":"$prefix$it","node":{"id":"$prefix$it","name":"x"}}""" }
        return """{"data":{"node":{"members":{"edges":[$edges],"pageInfo":{"hasNextPage":$hasNext,"endCursor":"$prefix${range.last}"}}}}}"""
    }

    @Test
    fun containerMembersPageThroughTheNodeAndKeepEveryContainerApart() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 64u), modelsJSON, transport)
        val def = buildJsonObject { put("name", JsonPrimitive("ContainerMembers")); put("document", JsonPrimitive(document)) }
        DBlayer.database.registerOperations(JsonArray(listOf(def)).toString())
        transport.enqueue("ContainerMembers", page("a", 0..29, true))
        transport.enqueue("ContainerMembers", page("b", 0..1, false))
        transport.enqueue("ContainerMembers", page("a", 30..44, false))

        val first = User.containerMembers("c1")
        first.load()
        User.containerMembers("c2").load()
        assertEquals(false, first.loadMore())
        val calls = transport.calls.filter { it.operationName == "ContainerMembers" }
        assertEquals(3, calls.size)
        assertTrue(calls[2].variablesJson.contains(""""after":"a29"""") && calls[2].variablesJson.contains(""""containerId":"c1""""))
        val second = User.containerMembers("c2")
        first.warm()
        second.warm()
        assertEquals((0..44).map { "a$it" }, first.cached()!!.value.map { it.id })
        assertEquals(listOf("b0", "b1"), second.cached()!!.value.map { it.id })
        first.load()
        assertEquals(3, transport.calls.count { it.operationName == "ContainerMembers" })
    }
}

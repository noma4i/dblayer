package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ChatMembersTest {
    private val document = "query ChatMembers(\$chatId: ID!, \$first: Int, \$after: Cursor) { node(id: \$chatId) { ... on Chat { members(first: \$first, after: \$after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"

    private fun page(prefix: String, range: IntRange, hasNext: Boolean): String {
        val edges = range.joinToString(",") { """{"cursor":"$prefix$it","node":{"id":"$prefix$it","name":"x"}}""" }
        return """{"data":{"node":{"members":{"edges":[$edges],"pageInfo":{"hasNextPage":$hasNext,"endCursor":"$prefix${range.last}"}}}}}"""
    }

    @Test
    fun chatMembersPageThroughTheNodeAndKeepEveryChatApart() = bounded {
        val transport = MockTransport()
        DBlayer.database = Database.open(Config(null, null, 64u), modelsJSON, transport)
        val def = buildJsonObject { put("name", JsonPrimitive("ChatMembers")); put("document", JsonPrimitive(document)) }
        DBlayer.database.registerOperations(JsonArray(listOf(def)).toString())
        transport.enqueue("ChatMembers", page("a", 0..29, true))
        transport.enqueue("ChatMembers", page("b", 0..1, false))
        transport.enqueue("ChatMembers", page("a", 30..44, false))

        val first = User.chatMembers("c1")
        first.load()
        User.chatMembers("c2").load()
        assertEquals(false, first.loadMore())
        val calls = transport.calls.filter { it.operationName == "ChatMembers" }
        assertEquals(3, calls.size)
        assertTrue(calls[2].variablesJson.contains(""""after":"a29"""") && calls[2].variablesJson.contains(""""chatId":"c1""""))
        val second = User.chatMembers("c2")
        first.warm()
        second.warm()
        assertEquals((0..44).map { "a$it" }, first.cached()!!.value.map { it.id })
        assertEquals(listOf("b0", "b1"), second.cached()!!.value.map { it.id })
        first.load()
        assertEquals(3, transport.calls.count { it.operationName == "ChatMembers" })
    }
}

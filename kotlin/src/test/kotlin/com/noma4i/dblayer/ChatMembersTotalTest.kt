package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertEquals
import org.junit.Test

class ChatMembersTotalTest {
    private val document = "query ChatMembers(\$chatId: ID!, \$first: Int, \$after: Cursor) { node(id: \$chatId) { ... on Chat { membersCount members(first: \$first, after: \$after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"

    private fun page(range: IntRange, hasNext: Boolean, count: Int?): String {
        val edges = range.joinToString(",") { """{"cursor":"u$it","node":{"id":"u$it","name":"x"}}""" }
        val counted = count?.let { """"membersCount":$it,""" } ?: ""
        return """{"data":{"node":{$counted"members":{"edges":[$edges],"pageInfo":{"hasNextPage":$hasNext,"endCursor":"u${range.last}"}}}}}"""
    }

    @Test
    fun chatMembersTotalIsTheMemberCountOfTheChat() = bounded {
        val transport = MockTransport()
        DBlayer.database = Database.open(Config(null, null, 64u), modelsJSON, transport)
        val def = buildJsonObject { put("name", JsonPrimitive("ChatMembers")); put("document", JsonPrimitive(document)) }
        DBlayer.database.registerOperations(JsonArray(listOf(def)).toString())
        transport.enqueue("ChatMembers", page(0..29, true, 53))
        transport.enqueue("ChatMembers", page(30..44, true, null))

        val members = User.chatMembers("c1")
        members.load()
        assertEquals(53L, withTimeout(5000) { members.state.flow.first { it.total != null } }.total)
        assertEquals(true, members.loadMore())
        assertEquals(53L, withTimeout(5000) { members.state.flow.first { it.total != null } }.total)
    }
}

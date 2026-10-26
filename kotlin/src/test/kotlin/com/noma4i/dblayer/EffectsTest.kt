package com.noma4i.dblayer

import com.noma4i.dblayer.models.Member
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Test

class EffectsTest {
    @Test
    fun blockingJoinsTheBlockedListAtOnceAndARefusalTakesBackOnlyThatMember() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"GroupRoster","document":"query GroupRoster(${'$'}groupId: ID!) { node(id: ${'$'}groupId) { members { id name } } }"},{"name":"MemberBlock","document":"mutation MemberBlock(${'$'}input: MemberBlockInput!) { memberBlock(input: ${'$'}input) { ok } }"}]""")
        transport.enqueue("GroupRoster", """{"data":{"node":{"members":[{"__typename":"Member","id":"m1","name":"a"},{"__typename":"Member","id":"m2","name":"b"}]}}}""")
        Member.roster("g1").load()
        val first = Member.find("m1").current()!!
        val second = Member.find("m2").current()!!
        Member.blocked.warm()

        transport.enqueue("MemberBlock", """{"errors":[{"message":"not allowed","extensions":{"code":"FORBIDDEN"}}]}""", 800)
        transport.enqueue("MemberBlock", """{"data":{"memberBlock":{"ok":true}}}""", 800)
        first.block()
        second.block()
        eventually("both members join the blocked list before any answer") { Member.blocked.cached()?.value?.map { it.id } == listOf("m2", "m1") }

        eventually("the refused block is taken back") { Member.blocked.cached()?.value?.map { it.id } == listOf("m2") }
        second.settled(null)
        assertEquals(listOf("m2"), Member.blocked.cached()!!.value.map { it.id })
    }
}

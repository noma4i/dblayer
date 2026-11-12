package com.noma4i.dblayer

import com.noma4i.dblayer.models.Club
import com.noma4i.dblayer.models.Letter
import com.noma4i.dblayer.models.Person
import com.noma4i.dblayer.models.modelsJSON
import com.sun.jna.Native
import com.sun.jna.ptr.PointerByReference
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

private const val CLUB_CREATED = """{"data":{"createClub":{"__typename":"Club","id":"g1","name":"friends"}}}"""
private const val LETTER_SENT = """{"data":{"sendLetter":{"__typename":"Letter","id":"l1","chatId":"c1","text":"hello","at":1}}}"""
private val CLUB_DEFS = """[{"name":"CreateClub","document":"mutation CreateClub(${'$'}name: String!) { createClub(name: ${'$'}name) { id name } }"},{"name":"LeaveClub","document":"mutation LeaveClub(${'$'}clubId: ID!) { leaveClub(clubId: ${'$'}clubId) { id name } }"},{"name":"AddClubMember","document":"mutation AddClubMember(${'$'}clubId: ID!, ${'$'}userIds: [ID!]!) { addClubMember(clubId: ${'$'}clubId, userIds: ${'$'}userIds) { id name } }"},{"name":"RemoveClubMember","document":"mutation RemoveClubMember(${'$'}clubId: ID!, ${'$'}userIds: [ID!]!) { removeClubMember(clubId: ${'$'}clubId, userIds: ${'$'}userIds) { id name } }"},{"name":"ClubMembers","document":"query ClubMembers(${'$'}clubId: ID!, ${'$'}order: String!, ${'$'}first: Int, ${'$'}after: Cursor) { node(id: ${'$'}clubId) { ... on Club { members(first: ${'$'}first, after: ${'$'}after, order: ${'$'}order) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"},{"name":"SendLetter","document":"mutation SendLetter(${'$'}chatId: ID!, ${'$'}text: String!, ${'$'}at: Int!) { sendLetter(chatId: ${'$'}chatId, text: ${'$'}text, at: ${'$'}at) { id chatId text at } }"},{"name":"ForwardLetter","document":"mutation ForwardLetter(${'$'}letterId: ID!, ${'$'}chatId: ID!) { forwardLetter(letterId: ${'$'}letterId, chatId: ${'$'}chatId) { letter { id chatId text at } } }"}]"""

private fun clubAnswer(root: String) = """{"data":{"$root":{"__typename":"Club","id":"g1","name":"friends"}}}"""

private fun membersReply(ids: List<String>): String {
    val edges = ids.joinToString(",") { """{"cursor":"$it","node":{"id":"$it","name":"$it"}}""" }
    return """{"data":{"node":{"members":{"edges":[$edges],"pageInfo":{"hasNextPage":false,"endCursor":"${ids.lastOrNull() ?: ""}"}}}}}"""
}

private class MemberEventsTransport(override val base: MockTransport = MockTransport()) : ScriptedTransport, WrapsMock {
    override suspend fun query(request: TransportRequest): String = base.query(request)
    override suspend fun mutate(request: TransportRequest): String = base.mutate(request)
    override suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply = base.scriptedPut(url, headers, file, identity, progress)
    override suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome = base.scriptedCable(url, events)
}

private fun clubModels(keep: Boolean = false, complete: Boolean = false, keepLetter: Boolean = false, events: Boolean = false, destroy: Boolean = false): String {
    if (!keep && !complete && !keepLetter && !events && !destroy) return modelsJSON
    return JsonArray(Json.parseToJsonElement(modelsArrayJSON).jsonArray.map { model ->
        val fields = model.jsonObject
        if (((keep || destroy) && fields["key"] == JsonPrimitive("Club")) || (keepLetter && fields["key"] == JsonPrimitive("Letter"))) {
            var actions = fields.getValue("actions").jsonArray.map { action ->
                val properties = action.jsonObject
                if ((keep || keepLetter) && properties["name"] == JsonPrimitive("create")) JsonObject(properties + ("on_failure" to JsonPrimitive("keep"))) else action
            }
            if (destroy && fields["key"] == JsonPrimitive("Club")) actions = actions + Json.parseToJsonElement("""{"name":"destroy","root_shape":"destroy","mutation":"DeleteClub","variables":{"id":"id"}}""")
            JsonObject(fields + ("actions" to JsonArray(actions)))
        } else if ((complete || events) && fields["key"] == JsonPrimitive("Person")) {
            val relations = fields.getValue("relations").jsonArray.map { relation ->
                val properties = relation.jsonObject
                if (properties["name"] != JsonPrimitive("club_members")) relation else {
                    var result = properties
                    if (complete) {
                        val remote = result.getValue("remote").jsonObject
                        val shape = remote.getValue("shape").jsonObject
                        result = JsonObject(result + ("remote" to JsonObject(remote + ("shape" to JsonObject(shape + ("coverage" to JsonPrimitive("complete")))))))
                    }
                    if (events) {
                        val updates = Json.parseToJsonElement("""[{"subscription":"MemberChanged","set":{},"removes":false}]""")
                        result = JsonObject(result + ("updates" to updates))
                    }
                    result
                }
            }
            JsonObject(fields + ("relations" to JsonArray(relations)))
        } else model
    }).toString().let { specs(it) }.let { if (events) schema("MemberChanged", it) else it }
}

class ClubTest {

    private suspend fun confirmedClub(): Club {
        val receipt = withTimeout(5000) { Club.create(name = "friends") }
        withTimeout(5000) { receipt.confirmed() }
        return Query("Club", Club).read().first()
    }

    private suspend fun removeMemberConfirmed(club: Club) {
        val receipt = withTimeout(5000) { club.removeMember(userIds = listOf("u4")) }
        withTimeout(5000) { receipt.confirmed() }
    }

    @Test
    fun aMemberEventKeepsTheOpenJoinInTheList() = bounded {
        val transport = MemberEventsTransport()
        transport.base.enqueue("CreateClub", CLUB_CREATED)
        transport.base.enqueue("ClubMembers", membersReply(listOf("u1", "u2", "u3", "u4")))
        openClubSession(transport, clubModels(events = true), """{"name":"MemberChanged","document":"subscription MemberChanged { memberChanged { id name } }"}""")
        val club = confirmedClub()
        val joined = Person.clubMembers(club.id, "JOINED")
        joined.load()
        transport.base.enqueue("RemoveClubMember", clubAnswer("removeClubMember"))
        transport.base.enqueue("ClubMembers", membersReply(listOf("u1", "u2", "u3")))
        removeMemberConfirmed(club)
        assertTrue(joined.read().none { it.id == "u4" })
        val scope = CoroutineScope(SupervisorJob())
        val list = ListReader(joined, scope)
        try {
            list.waitForInitialLoad()
            eventually("member subscription") { transport.base.calls.any { it.operationName == "MemberChanged" } }
            val hold = transport.base.hold("AddClubMember", clubAnswer("addClubMember"))
            try {
                val adding = async { club.addMember(userIds = listOf("u4")) }
                withTimeout(5000) { hold.entered() }
                transport.base.emit("MemberChanged", """{"data":{"memberChanged":{"id":"u1","name":"updated"}}}""")
                val visible = withTimeout(5000) { list.rows.first { rows -> rows.any { it.id == "u1" && it.name == "updated" } } }
                assertTrue(visible.any { it.id == "u4" })
                val receipt = withTimeout(5000) { adding.await() }
                transport.base.enqueue("ClubMembers", membersReply(listOf("u1", "u2", "u3", "u4")))
                hold.release()
                withTimeout(5000) { receipt.confirmed() }
                Unit
            } finally {
                hold.release()
            }
        } finally {
            scope.cancel()
        }
    }

    private suspend fun openClubSession(transport: Transport, models: String = modelsJSON, subscription: String? = null) {
        val defs = if (subscription == null) CLUB_DEFS else CLUB_DEFS.dropLast(1) + "," + subscription + "]"
        openSigned(Config(null, null), models, transport, defs)
    }

    private suspend fun club(id: String) = Query("Club", Club).readRow(id)!!

}

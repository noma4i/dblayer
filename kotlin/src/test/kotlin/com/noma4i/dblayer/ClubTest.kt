package com.noma4i.dblayer

import com.noma4i.dblayer.models.Club
import com.noma4i.dblayer.models.Letter
import com.noma4i.dblayer.models.Person
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.async
import kotlinx.coroutines.withTimeout
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

private val FORBIDDEN = DbException.Http(403.toUShort(), "forbidden")

private fun MockTransport.enqueueOutcome(operation: String, reply: String, refusal: Boolean, delayMillis: Long) {
    if (refusal) enqueue(operation, FORBIDDEN, delayMillis) else enqueue(operation, reply, delayMillis)
}

private fun forwardAnswer(id: String, chat: String) = """{"data":{"forwardLetter":{"letter":{"__typename":"Letter","id":"$id","chatId":"$chat","text":"hello","at":99}}}}"""

class ClubTest {
    private suspend fun openClubSession(transport: MockTransport) {
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations(CLUB_DEFS)
    }

    private suspend fun club(id: String) = Query("Club", Club).readRow(id)!!

    /** K04 (B4): a create the server answers returns the confirmed row, needs the network, and queues nothing offline. */
    @Test
    fun aServerCreateReturnsTheConfirmedRowAndNeedsTheNetwork() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateClub", CLUB_CREATED, 300)
        openClubSession(transport)
        val created = Club.create(name = "friends")
        assertTrue("the call returned before the server answered", created.id == "g1" && created.name == "friends" && created.sync.state == SyncState.Synced)
        assertEquals(1, transport.calls.count { it.operationName == "CreateClub" })

        DBlayer.database.setOnline(false)
        try { Club.create(name = "offline"); fail("a server create offline did not fail") } catch (error: DbException.Network) {}
        assertEquals(1, transport.calls.count { it.operationName == "CreateClub" })
    }

    /** K13 (B6, C05): leave and the member actions change the real lists before the answer. The answer keeps the change, a refusal restores the ids and the order of the lists. */
    @Test
    fun aClubActionChangesItsListsBeforeTheAnswerAndARefusalRestoresThem() = bounded {
        for (refusal in listOf(false, true)) {
            val case = if (refusal) "refused" else "answered"
            val transport = MockTransport()
            transport.enqueue("CreateClub", CLUB_CREATED)
            transport.enqueue("ClubMembers", membersReply(listOf("u1", "u2", "u3")))
            transport.enqueue("ClubMembers", membersReply(listOf("u2", "u1", "u3")))
            openClubSession(transport)
            val created = Club.create(name = "friends")
            val joined = Person.clubMembers(created.id, "JOINED")
            val owners = Person.clubMembers(created.id, "OWNER_FIRST")
            joined.load()
            owners.load()
            suspend fun lists() = listOf(joined.read().map { it.id }, owners.read().map { it.id })
            assertEquals("$case: the lists before the action", listOf(listOf("u1", "u2", "u3"), listOf("u2", "u1", "u3")), lists())

            transport.enqueueOutcome("RemoveClubMember", clubAnswer("removeClubMember"), refusal, 2000)
            val started = System.nanoTime()
            val removing = async { runCatching { created.removeMember(userIds = listOf("u2")) } }
            eventually("remove member changed both lists") { lists() == listOf(listOf("u1", "u3"), listOf("u1", "u3")) }
            assertTrue("$case: the lists changed only after the answer", (System.nanoTime() - started) < 1_500_000_000L)
            assertEquals("$case: the outcome of remove member", refusal, removing.await().isFailure)
            val afterRemove = if (refusal) listOf(listOf("u1", "u2", "u3"), listOf("u2", "u1", "u3")) else listOf(listOf("u1", "u3"), listOf("u1", "u3"))
            eventually("the lists after remove member") { lists() == afterRemove }

            if (!refusal) {
                transport.enqueueOutcome("AddClubMember", clubAnswer("addClubMember"), false, 2000)
                val adding = async { runCatching { created.addMember(userIds = listOf("u2")) } }
                eventually("add member changed both lists") { lists() == listOf(listOf("u1", "u3", "u2"), listOf("u1", "u3", "u2")) }
                adding.await()
                assertEquals("$case: the lists after add member", listOf(listOf("u1", "u3", "u2"), listOf("u1", "u3", "u2")), lists())
            }

            transport.enqueueOutcome("LeaveClub", clubAnswer("leaveClub"), refusal, 2000)
            val leaving = async { runCatching { created.leave() } }
            eventually("leave removed the club from its list") { Query("Club", Club).readRow(created.id) == null }
            assertEquals("$case: the outcome of leave", refusal, leaving.await().isFailure)
            eventually("the club list after leave") { (Query("Club", Club).readRow(created.id) != null) == refusal }
        }
    }

    /** K12: a forward into the same chat and into another chat leaves the source row unchanged, upserts the answer row in its own chat, and the call returns the answer row. */
    @Test
    fun aForwardLeavesTheSourceUnchangedUpsertsTheAnswerRowAndReturnsIt() = bounded {
        val transport = MockTransport()
        transport.enqueue("SendLetter", LETTER_SENT)
        openClubSession(transport)
        val local = Letter.send(AccountScope.current(), chatId = "c1", text = "hello", at = 1).row
        withTimeout(5000) { local.settled(null) }
        val sourceId = withTimeout(5000) { local.confirmedId() }
        val source = Query("Letter", Letter).readRow(sourceId)!!

        for ((target, id) in listOf("c1" to "fwd-1", "c2" to "fwd-2")) {
            transport.enqueue("ForwardLetter", forwardAnswer(id, target))
            val answer = source.forward(targetChatId = target)
            assertTrue("$target: the call returned ${answer.id}, not the answer row", answer.id == id && answer.chatId == target)
            val unchanged = Query("Letter", Letter).readRow(sourceId)!!
            assertTrue("$target: the answer was written onto the source row", unchanged.chatId == "c1" && unchanged.text == "hello" && unchanged.at == 1L)
            assertEquals("$target: the answer row is not in its own chat", target, Query("Letter", Letter).readRow(id)!!.chatId)
        }
    }

    /** K22 (B6, C05): a forward shows a local copy in the thread of the target chat before the answer. The answer replaces the copy, a refusal removes it. */
    @Test
    fun aForwardShowsALocalCopyInTheTargetThreadAndARefusalRemovesIt() = bounded {
        for (refusal in listOf(false, true)) {
            val case = if (refusal) "refused" else "answered"
            val transport = MockTransport()
            transport.enqueue("SendLetter", LETTER_SENT)
            openClubSession(transport)
            val local = Letter.send(AccountScope.current(), chatId = "c1", text = "hello", at = 1).row
            withTimeout(5000) { local.settled(null) }
            val sourceId = withTimeout(5000) { local.confirmedId() }
            val source = Query("Letter", Letter).readRow(sourceId)!!
            val thread = Letter.thread("c2")
            assertTrue(thread.read().isEmpty())

            transport.enqueueOutcome("ForwardLetter", forwardAnswer("fwd-1", "c2"), refusal, 2000)
            val started = System.nanoTime()
            val forwarding = async { runCatching { source.forward(targetChatId = "c2") } }
            eventually("the target thread shows the copy") { thread.read().size == 1 }
            assertTrue("$case: the copy appeared only after the answer", (System.nanoTime() - started) < 1_500_000_000L)
            val copy = thread.read().first()
            assertTrue("$case: the copy is ${copy.id}", copy.id != "fwd-1" && copy.text == "hello" && copy.chatId == "c2")
            assertEquals("$case: the outcome of forward", refusal, forwarding.await().isFailure)
            eventually("the target thread shows the outcome") { thread.read().map { it.id } == (if (refusal) emptyList() else listOf("fwd-1")) }
        }
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.Club
import com.noma4i.dblayer.models.Letter
import com.noma4i.dblayer.models.Person
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
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

    /** Opens a session with a club whose two member lists hold u1 u2 u3 u4 (JOINED) and u2 u1 u3 u4 (OWNER_FIRST). */
    private suspend fun openClubWithMembers(transport: MockTransport): Triple<Club, Query<Person>, Query<Person>> {
        transport.enqueue("CreateClub", CLUB_CREATED)
        transport.enqueue("ClubMembers", membersReply(listOf("u1", "u2", "u3", "u4")))
        transport.enqueue("ClubMembers", membersReply(listOf("u2", "u1", "u3", "u4")))
        openClubSession(transport)
        val created = Club.create(name = "friends")
        val joined = Person.clubMembers(created.id, "JOINED")
        val owners = Person.clubMembers(created.id, "OWNER_FIRST")
        joined.load()
        owners.load()
        return Triple(created, joined, owners)
    }

    /** K13 (B6, C05): remove member and add member change both lists before the answer. The answer keeps the change, a refusal restores the ids and the order of both lists. */
    @Test
    fun aMemberActionChangesBothListsBeforeTheAnswerAndARefusalRestoresThem() = bounded {
        for ((removing, refusal) in listOf(true to false, true to true, false to false, false to true)) {
            val case = "${if (removing) "remove" else "add"} ${if (refusal) "refused" else "answered"}"
            val transport = MockTransport()
            val (created, joined, owners) = openClubWithMembers(transport)
            suspend fun lists() = listOf(joined.read().map { it.id }, owners.read().map { it.id })
            val original = listOf(listOf("u1", "u2", "u3", "u4"), listOf("u2", "u1", "u3", "u4"))
            if (!removing) {
                transport.enqueue("RemoveClubMember", clubAnswer("removeClubMember"))
                created.removeMember(userIds = listOf("u4"))
                assertEquals("$case: the setup", listOf(listOf("u1", "u2", "u3"), listOf("u2", "u1", "u3")), lists())
            }
            val user = if (removing) "u2" else "u4"
            val hold = transport.hold(if (removing) "RemoveClubMember" else "AddClubMember", clubAnswer(if (removing) "removeClubMember" else "addClubMember"), if (refusal) FORBIDDEN else null)
            val acting = async { runCatching { if (removing) created.removeMember(userIds = listOf(user)) else created.addMember(userIds = listOf(user)) } }
            withTimeout(5000) { hold.entered() }
            val during = if (removing) listOf(listOf("u1", "u3", "u4"), listOf("u1", "u3", "u4")) else original
            assertEquals("$case: the lists before the answer", during, lists())
            hold.release()
            assertEquals("$case: the outcome", refusal, acting.await().isFailure)
            val after = if (refusal) (if (removing) original else listOf(listOf("u1", "u2", "u3"), listOf("u2", "u1", "u3"))) else during
            eventually("the lists after the outcome") { lists() == after }
        }
    }

    /** A member whose row is not cached is skipped by the optimistic join: the call ends with the answer and the lists keep their ids. */
    @Test
    fun addingAMemberWhoseRowIsNotCachedEndsWithTheAnswer() = bounded {
        val transport = MockTransport()
        val (created, joined, owners) = openClubWithMembers(transport)
        transport.enqueue("AddClubMember", clubAnswer("addClubMember"))
        created.addMember(userIds = listOf("u9"))
        assertEquals(1, transport.calls.count { it.operationName == "AddClubMember" })
        assertEquals(listOf("u1", "u2", "u3", "u4"), joined.read().map { it.id })
        assertEquals(listOf("u2", "u1", "u3", "u4"), owners.read().map { it.id })
    }

    /** K32: a member added or removed ends with the member lists of the server in both orders. The added user appears in both lists and the removed one leaves them. */
    @Test
    fun anAddedAndARemovedMemberEndWithTheMemberListsOfTheServer() = bounded {
        val transport = MockTransport()
        val (created, joined, owners) = openClubWithMembers(transport)
        suspend fun lists() = listOf(joined.read().map { it.id }, owners.read().map { it.id })
        suspend fun readAgain(joinedIds: List<String>, ownerIds: List<String>) {
            transport.enqueue("ClubMembers", membersReply(joinedIds))
            joined.load()
            transport.enqueue("ClubMembers", membersReply(ownerIds))
            owners.load()
        }
        val added = listOf(listOf("u1", "u2", "u3", "u4", "u9"), listOf("u2", "u1", "u3", "u4", "u9"))
        transport.enqueue("AddClubMember", clubAnswer("addClubMember"))
        created.addMember(userIds = listOf("u9"))
        readAgain(added[0], added[1])
        assertEquals("the lists after the add", added, lists())
        val removed = listOf(listOf("u1", "u2", "u3", "u4"), listOf("u2", "u1", "u3", "u4"))
        transport.enqueue("RemoveClubMember", clubAnswer("removeClubMember"))
        created.removeMember(userIds = listOf("u9"))
        readAgain(removed[0], removed[1])
        assertEquals("the lists after the removal", removed, lists())
    }

    /** K33 (A1): a call that waits for the answer of a mutation returns with that answer while a load of the list the mutation refetches is held. The list is read again once that load ends. */
    @Test
    fun aMutationReturnsWithItsAnswerWhileALoadOfItsRefetchedListIsHeld() = bounded {
        val transport = MockTransport()
        val (created, joined, _) = openClubWithMembers(transport)
        val scope = CoroutineScope(SupervisorJob())
        val list = ListReader(joined, scope)
        list.waitForInitialLoad()
        val current = listOf("u1", "u2", "u3", "u4")
        val loadHold = transport.hold("ClubMembers", membersReply(current))
        val addHold = transport.hold("AddClubMember", clubAnswer("addClubMember"))
        val rereadHold = transport.hold("ClubMembers", membersReply(current + "u9"))
        val refreshing = async { list.refresh() }
        withTimeout(5000) { loadHold.entered() }
        val adding = async { created.addMember(userIds = listOf("u9")) }
        withTimeout(5000) { addHold.entered() }
        addHold.release()
        try {
            awaitWithin("the add to return with its answer while the list load is held") { adding.await() }
        } finally {
            loadHold.release()
        }
        refreshing.await()
        withTimeout(5000) { rereadHold.entered() }
        rereadHold.release()

        eventually("the list is read again after the held load") { list.rows.value.map { it.id } == current + "u9" }
        scope.cancel()
    }

    /** K13 (B6, C05): leave removes the club from its list before the answer. The answer keeps it away, a refusal puts it back. */
    @Test
    fun leaveRemovesTheClubFromItsListBeforeTheAnswerAndARefusalRestoresIt() = bounded {
        for (refusal in listOf(false, true)) {
            val transport = MockTransport()
            transport.enqueue("CreateClub", CLUB_CREATED)
            openClubSession(transport)
            val created = Club.create(name = "friends")
            val hold = transport.hold("LeaveClub", clubAnswer("leaveClub"), if (refusal) FORBIDDEN else null)
            val leaving = async { runCatching { created.leave() } }
            withTimeout(5000) { hold.entered() }
            assertTrue("the club stayed in its list before the answer", Query("Club", Club).readRow(created.id) == null)
            hold.release()
            assertEquals("the outcome of leave", refusal, leaving.await().isFailure)
            eventually("the club list after the outcome") { (Query("Club", Club).readRow(created.id) != null) == refusal }
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

            val hold = transport.hold("ForwardLetter", forwardAnswer("fwd-1", "c2"), if (refusal) FORBIDDEN else null)
            val forwarding = async { runCatching { source.forward(targetChatId = "c2") } }
            withTimeout(5000) { hold.entered() }
            val copy = thread.read().firstOrNull()
            assertTrue("$case: the target thread has no copy before the answer", copy != null)
            assertTrue("$case: the copy is ${copy!!.id}", copy.id != "fwd-1" && copy.text == "hello" && copy.chatId == "c2")
            hold.release()
            assertEquals("$case: the outcome of forward", refusal, forwarding.await().isFailure)
            eventually("the target thread shows the outcome") { thread.read().map { it.id } == (if (refusal) emptyList() else listOf("fwd-1")) }
        }
    }
}

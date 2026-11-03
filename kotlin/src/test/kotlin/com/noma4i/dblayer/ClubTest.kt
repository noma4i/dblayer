package com.noma4i.dblayer

import com.noma4i.dblayer.models.Club
import com.noma4i.dblayer.models.Letter
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

private const val CLUB_CREATED = """{"data":{"createClub":{"__typename":"Club","id":"g1","name":"friends"}}}"""
private const val LETTER_SENT = """{"data":{"sendLetter":{"__typename":"Letter","id":"l1","chatId":"c1","text":"hello","at":1}}}"""
private val CLUB_DEFS = """[{"name":"CreateClub","document":"mutation CreateClub(${'$'}name: String!) { createClub(name: ${'$'}name) { id name left membership } }"},{"name":"LeaveClub","document":"mutation LeaveClub(${'$'}clubId: ID!) { leaveClub(clubId: ${'$'}clubId) { id name left membership } }"},{"name":"AddClubMember","document":"mutation AddClubMember(${'$'}clubId: ID!) { addClubMember(clubId: ${'$'}clubId) { id name left membership } }"},{"name":"RemoveClubMember","document":"mutation RemoveClubMember(${'$'}clubId: ID!) { removeClubMember(clubId: ${'$'}clubId) { id name left membership } }"},{"name":"SendLetter","document":"mutation SendLetter(${'$'}chatId: ID!, ${'$'}text: String!, ${'$'}at: Int!) { sendLetter(chatId: ${'$'}chatId, text: ${'$'}text, at: ${'$'}at) { id chatId text at } }"},{"name":"ForwardLetter","document":"mutation ForwardLetter(${'$'}letterId: ID!, ${'$'}chatId: ID!) { forwardLetter(letterId: ${'$'}letterId, chatId: ${'$'}chatId) { letter { id chatId text at } } }"}]"""

private fun clubAnswer(root: String, left: Boolean, membership: String?): String {
    val state = membership?.let { "\"$it\"" } ?: "null"
    return """{"data":{"$root":{"__typename":"Club","id":"g1","name":"friends","left":$left,"membership":$state}}}"""
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

    /** K13 (B6): leave, add member and remove member show their change at once as unsent, before the server answers, and end as synced after the answer. */
    @Test
    fun aClubActionShowsAsUnsentBeforeTheAnswerAndSyncedAfter() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateClub", CLUB_CREATED)
        openClubSession(transport)
        val created = Club.create(name = "friends")
        val actions = listOf(
            listOf("leave", "LeaveClub", "leaveClub", "true", ""),
            listOf("add_member", "AddClubMember", "addClubMember", "false", "added"),
            listOf("remove_member", "RemoveClubMember", "removeClubMember", "false", "removed"),
        )
        for ((action, operation, root, left, membership) in actions) {
            transport.enqueue(operation, clubAnswer(root, left == "true", membership.ifEmpty { null }), 600)
            val changed = when (action) {
                "leave" -> created.leave()
                "add_member" -> created.addMember()
                else -> created.removeMember()
            }
            assertTrue("$action: the row was synced before the server answered", changed.sync.state != SyncState.Synced)
            val shown = if (membership.isEmpty()) changed.left == true else changed.membership == membership
            assertTrue("$action: the change was not shown before the answer", shown)
            withTimeout(5000) { changed.settled(action) }
            assertEquals("$action: the row did not end as synced", SyncState.Synced, club(created.id).sync.state)
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
}

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

private val FORBIDDEN = DbException.Http(403.toUShort(), "forbidden")

private fun forwardAnswer(id: String, chat: String) = """{"data":{"forwardLetter":{"letter":{"__typename":"Letter","id":"$id","chatId":"$chat","text":"hello","at":99}}}}"""

private class MemberEventsTransport(val base: MockTransport = MockTransport()) : Transport by base {
    val opened = CompletableDeferred<Unit>()
    override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink) {
        base.subscribe(request, sink)
        if (request.operationName == "MemberChanged") opened.complete(Unit)
    }
}

private fun clubModels(keep: Boolean = false, complete: Boolean = false, keepLetter: Boolean = false, events: Boolean = false, destroy: Boolean = false): String {
    if (!keep && !complete && !keepLetter && !events && !destroy) return modelsJSON
    return JsonArray(Json.parseToJsonElement(modelsJSON).jsonArray.map { model ->
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
    }).toString()
}

class ClubTest {
    @Test
    fun anOperationOutcomeCanBeReadTwiceFromItsReceipt() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateClub", CLUB_CREATED)
        transport.enqueue("AddClubMember", clubAnswer("addClubMember"))
        transport.enqueue("LeaveClub", clubAnswer("leaveClub"))
        openClubSession(transport)
        val club = confirmedClub()
        val receipt = club.addMember(userIds = listOf("u9"))
        assertEquals(club.id, withTimeout(5000) { receipt.outcome() }.confirmedValue?.id)
        assertTrue(withTimeout(5000) { club.leave().outcome() } is OperationOutcome.Confirmed)
        assertEquals(club.id, withTimeout(5000) { receipt.outcome() }.confirmedValue?.id)
    }

    private suspend fun confirmedClub(): Club {
        val receipt = withTimeout(5000) { Club.create(name = "friends") }
        check(withTimeout(5000) { receipt.outcome() } is OperationOutcome.Confirmed)
        return Query("Club", Club).read().first()
    }

    private suspend fun removeMemberConfirmed(club: Club) {
        val receipt = withTimeout(5000) { club.removeMember(userIds = listOf("u4")) }
        check(withTimeout(5000) { receipt.outcome() } is OperationOutcome.Confirmed)
    }

    private suspend fun confirmedLetter(): Letter {
        val receipt = withTimeout(5000) { Letter.send(AccountScope.current(), chatId = "c1", text = "hello", at = 1) }
        check(withTimeout(5000) { receipt.outcome() } is OperationOutcome.Confirmed)
        return Query("Letter", Letter).read().first()
    }

    @Test
    fun aPendingOperationSurvivesAnOldFormatWithoutRestoringTheCachedRow() = bounded {
        for (keep in listOf(false, true)) {
            val directory = java.nio.file.Files.createTempDirectory("dblayer-old-format").toFile()
            val config = Config(directory.resolve("old.sqlite").absolutePath, null, 32u)
            val models = clubModels(keepLetter = keep)
            try {
                reopenSigned(config, models, CLUB_DEFS)
                val receipt = Letter.send(AccountScope.current(), chatId = "c1", text = "held", at = 1)
                val id = requireNotNull(receipt.optimisticValue).id
                DBlayer.close()
                val sqlite = Native.load("sqlite3", LegacySqlite::class.java)
                val handle = PointerByReference()
                check(sqlite.sqlite3_open(requireNotNull(config.path), handle) == 0)
                val changed = sqlite.sqlite3_exec(handle.value, "DROP TABLE IF EXISTS op_effect_value; DROP TABLE IF EXISTS op_effect; DROP TABLE IF EXISTS scope_revision; UPDATE manifest SET value = 'old' WHERE key = 'format_version'", null, null, null)
                sqlite.sqlite3_close(handle.value)
                check(changed == 0)
                val transport = MockTransport()
                transport.enqueue("SendLetter", DbException.Http(422.toUShort(), "refused"))
                reopen(config, models, transport, CLUB_DEFS)
                eventually("the pending send reaches the server") { transport.calls.count { it.operationName == "SendLetter" } == 1 }
                assertEquals(1, transport.calls.count { it.operationName == "SendLetter" })
                assertTrue(Query("Letter", Letter).readRow(id) == null)
                transport.enqueue("SendLetter", DbException.Http(422.toUShort(), "refused again"))
                val retried = withTimeout(5000) { Letter.retry(id, null) }
                assertEquals(keep, retried.operation != null)
                if (keep) assertTrue(withTimeout(5000) { retried.outcome() } is OperationOutcome.Rejected)
                assertTrue(Query("Letter", Letter).readRow(id) == null)
                DBlayer.close()
            } finally {
                DBlayer.close()
                directory.deleteRecursively()
            }
        }
    }

    @Test
    fun aRefusedDestroyRestoresTheRowAccordingToItsDeclaration() = bounded {
        for (business in listOf(false, true)) {
            val transport = MockTransport()
            transport.enqueue("CreateClub", CLUB_CREATED)
            openClubSession(transport, clubModels(destroy = true))
            DBlayer.database.registerOperations("""[{"name":"DeleteClub","document":"mutation DeleteClub(${'$'}id: ID!) { deleteClub(id: ${'$'}id) { id } }"}]""")
            val club = confirmedClub()
            val hold = if (business) transport.hold("DeleteClub", """{"errors":[{"message":"refused","extensions":{"code":"REFUSED"}}]}""") else transport.hold("DeleteClub", failure = DbException.Http(422.toUShort(), "refused"))
            try {
                val receipt = club.destroy()
                withTimeout(5000) { hold.entered() }
                assertTrue(Query("Club", Club).readRow(club.id) == null)
                hold.release()
                assertTrue(withTimeout(5000) { receipt.outcome() } is OperationOutcome.Rejected)
                assertTrue(Query("Club", Club).readRow(club.id) != null)
            } finally {
                hold.release()
            }
        }
    }

    @Test
    fun joiningMultipleMembersKeepsEachOnceInOrder() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateClub", CLUB_CREATED)
        transport.enqueue("ClubMembers", membersReply(listOf("u1", "u2", "u3", "u4", "u5")))
        openClubSession(transport)
        val club = confirmedClub()
        val joined = Person.clubMembers(club.id, "JOINED")
        joined.load()
        transport.enqueue("RemoveClubMember", clubAnswer("removeClubMember"))
        val removed = club.removeMember(userIds = listOf("u4", "u5"))
        check(withTimeout(5000) { removed.outcome() } is OperationOutcome.Confirmed)
        val hold = transport.hold("AddClubMember", clubAnswer("addClubMember"))
        try {
            val receipt = withTimeout(5000) { club.addMember(userIds = listOf("u4", "u5", "u4")) }
            withTimeout(5000) { hold.entered() }
            assertEquals(listOf("u1", "u2", "u3", "u4", "u5"), joined.read().map { it.id })
            hold.release()
            assertTrue(withTimeout(5000) { receipt.outcome() } is OperationOutcome.Confirmed)
        } finally {
            hold.release()
        }
    }

    @Test
    fun aMemberEventKeepsTheOpenJoinInTheList() = bounded {
        val transport = MemberEventsTransport()
        transport.base.enqueue("CreateClub", CLUB_CREATED)
        transport.base.enqueue("ClubMembers", membersReply(listOf("u1", "u2", "u3", "u4")))
        openClubSession(transport, clubModels(events = true))
        DBlayer.database.registerOperations("""[{"name":"MemberChanged","document":"subscription MemberChanged { memberChanged { id name } }"}]""")
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
            withTimeout(5000) { transport.opened.await() }
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
                assertTrue(withTimeout(5000) { receipt.outcome() } is OperationOutcome.Confirmed)
            } finally {
                hold.release()
            }
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun aRefusedDurableSendKeepsItsRowWhenDeclared() = bounded {
      for (retry in listOf(false, true)) {
        val transport = MockTransport()
        openClubSession(transport, clubModels(keepLetter = true))
        val hold = transport.hold("SendLetter", failure = DbException.Http(422.toUShort(), "refused"))
        if (retry) transport.enqueue("SendLetter", LETTER_SENT)
        try {
            val sent = Letter.send(AccountScope.current(), chatId = "c1", text = "hello", at = 1)
            val local = sent.optimisticValue!!
            withTimeout(5000) { hold.entered() }
            assertTrue(Query("Letter", Letter).readRow(local.id) != null)
            hold.release()
            assertTrue(withTimeout(5000) { sent.outcome() } is OperationOutcome.Rejected)
            assertTrue(Query("Letter", Letter).readRow(local.id) != null)
            if (retry) {
                val retried = local.retry(null)
                assertTrue(withTimeout(5000) { retried.outcome() } is OperationOutcome.Confirmed)
            } else {
                local.discard(null)
            }
            assertEquals(if (retry) 1 else 0, Query("Letter", Letter).read().size)
        } finally {
            hold.release()
        }
      }
    }

    @Test
    fun aLaterConfirmedMembershipChangeSurvivesAnEarlierRefusal() = bounded {
      for (firstJoin in listOf(false, true)) {
        val transport = MockTransport()
        val (club, joined, _) = openClubWithMembers(transport)
        if (firstJoin) {
            transport.enqueue("RemoveClubMember", clubAnswer("removeClubMember"))
            removeMemberConfirmed(club)
        }
        assertEquals(!firstJoin, joined.read().any { it.id == "u4" })
        val old = transport.hold("ClubMembers", membersReply(if (firstJoin) listOf("u1", "u2", "u3") else listOf("u1", "u2", "u3", "u4")))
        val reading = async { joined.refresh() }
        withTimeout(5000) { old.entered() }
        val first = transport.hold(if (firstJoin) "AddClubMember" else "RemoveClubMember", failure = FORBIDDEN)
        val second = transport.hold(if (firstJoin) "RemoveClubMember" else "AddClubMember", clubAnswer(if (firstJoin) "removeClubMember" else "addClubMember"))
        val joining = async { if (firstJoin) club.addMember(userIds = listOf("u4")) else club.removeMember(userIds = listOf("u4")) }
        try {
            withTimeout(5000) { first.entered() }
            val firstReceipt = withTimeout(5000) { joining.await() }
            first.release()
            assertTrue(withTimeout(5000) { firstReceipt.outcome() } is OperationOutcome.Rejected)
            val secondReceipt = withTimeout(5000) { if (firstJoin) club.removeMember(userIds = listOf("u4")) else club.addMember(userIds = listOf("u4")) }
            withTimeout(5000) { second.entered() }
            second.release()
            assertTrue(withTimeout(5000) { secondReceipt.outcome() } is OperationOutcome.Confirmed)
            old.release()
            withTimeout(5000) { reading.await() }
            assertEquals(!firstJoin, joined.read().any { it.id == "u4" })
        } finally {
            first.release()
            second.release()
            old.release()
        }
      }
    }

    @Test
    fun forwardReturnsItsOptimisticValueBeforeTheServerAnswers() = bounded {
        val transport = MockTransport()
        transport.enqueue("SendLetter", LETTER_SENT)
        openClubSession(transport)
        val source = confirmedLetter()
        val hold = transport.hold("ForwardLetter", forwardAnswer("fwd-1", "c2"))
        val forward = async { source.forward(targetChatId = "c2") }
        try {
            withTimeout(5000) { hold.entered() }
            val receipt = withTimeoutOrNull(5000) { forward.await() }
            assertTrue("forward did not return before the server answer", receipt != null)
            val optimistic = receipt!!.optimisticValue!!
            assertTrue(optimistic.id != "fwd-1" && optimistic.chatId == "c2")
            val written = DBlayer.database.readRow(DBlayer.database.sessionEpoch(), "Letter", optimistic.id)!!
            assertEquals(receipt.revision, written.revision)
            val unchanged = Query("Letter", Letter).readRow(source.id)!!
            assertTrue(unchanged.chatId == source.chatId && unchanged.text == source.text)
            hold.release()
            assertEquals("fwd-1", withTimeout(5000) { receipt.outcome() }.confirmedValue?.id)
        } finally {
            hold.release()
        }
    }

    @Test
    fun anOfflineActionIsNotSentAfterReconnectButALetterIsSentOnce() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateClub", CLUB_CREATED)
        transport.enqueue("SendLetter", LETTER_SENT)
        openClubSession(transport)
        val created = confirmedClub()
        DBlayer.database.setOnline(false)
        assertTrue(runCatching { created.addMember(userIds = listOf("u9")) }.isFailure)
        val sent = Letter.send(AccountScope.current(), chatId = "c1", text = "hello", at = 1)
        assertTrue(sent.optimisticValue != null)
        assertEquals(0, transport.calls.count { it.operationName == "SendLetter" })
        DBlayer.database.setOnline(true)
        assertTrue(withTimeout(5000) { sent.outcome() } is OperationOutcome.Confirmed)
        assertEquals(1, transport.calls.count { it.operationName == "SendLetter" })
        assertEquals(0, transport.calls.count { it.operationName == "AddClubMember" })
    }

    @Test
    fun aCreateRefusalFollowsTheDeclaredPolicy() = bounded {
        for ((business, keep) in listOf(false to false, false to true, true to false, true to true)) {
            val transport = MockTransport()
            if (business) transport.enqueue("CreateClub", """{"errors":[{"message":"refused","extensions":{"code":"REFUSED"}}]}""")
            else transport.enqueue("CreateClub", DbException.Http(422.toUShort(), "refused"))
            openClubSession(transport, clubModels(keep))
            val receipt = Club.create(name = "friends")
            assertTrue(withTimeout(5000) { receipt.outcome() } is OperationOutcome.Rejected)
            assertEquals(1, transport.calls.count { it.operationName == "CreateClub" })
            assertEquals(if (keep) 1 else 0, Query("Club", Club).read().size)
        }
    }

    @Test
    fun anAcknowledgedMembershipChangeSurvivesAnOlderListAnswer() = bounded {
        for ((removing, answerFirst, complete) in listOf(Triple(false, false, false), Triple(false, true, false), Triple(true, false, false), Triple(true, true, false), Triple(false, false, true), Triple(false, true, true), Triple(true, false, true), Triple(true, true, true))) {
            val transport = MockTransport()
            val (created, joined, _) = openClubWithMembers(transport, clubModels(complete = complete))
            if (!removing) {
                transport.enqueue("RemoveClubMember", clubAnswer("removeClubMember"))
                removeMemberConfirmed(created)
            }
            val old = if (removing) listOf("u1", "u2", "u3", "u4") else listOf("u1", "u2", "u3")
            val hold = transport.hold("ClubMembers", membersReply(old))
            val reading = async { joined.refresh() }
            withTimeout(5000) { hold.entered() }
            val mutation = transport.hold(if (removing) "RemoveClubMember" else "AddClubMember", clubAnswer(if (removing) "removeClubMember" else "addClubMember"))
            val acting = async { if (removing) created.removeMember(userIds = listOf("u4")) else created.addMember(userIds = listOf("u4")) }
            try {
                withTimeout(5000) { mutation.entered() }
                val receipt = withTimeout(5000) { acting.await() }
                if (answerFirst) { mutation.release(); assertTrue(withTimeout(5000) { receipt.outcome() } is OperationOutcome.Confirmed) }
                hold.release()
                reading.await()
                assertEquals(!removing, joined.read().map { it.id }.contains("u4"))
                if (!answerFirst) { mutation.release(); assertTrue(withTimeout(5000) { receipt.outcome() } is OperationOutcome.Confirmed) }
            } finally {
                hold.release()
                mutation.release()
            }
        }
    }

    private suspend fun openClubSession(transport: Transport, models: String = modelsJSON) {
        reopen(Config(null, null, 32u), models, transport)
        DBlayer.database.registerOperations(CLUB_DEFS)
    }

    private suspend fun club(id: String) = Query("Club", Club).readRow(id)!!

    /** K04 (B4): a create the server answers returns the confirmed row, needs the network, and queues nothing offline. */
    @Test
    fun aServerCreateReturnsTheConfirmedRowAndNeedsTheNetwork() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateClub", CLUB_CREATED, 300)
        openClubSession(transport)
        val created = confirmedClub()
        assertTrue("the call returned before the server answered", created.id == "g1" && created.name == "friends" && created.sync.state == SyncState.Synced)
        assertEquals(1, transport.calls.count { it.operationName == "CreateClub" })

        DBlayer.database.setOnline(false)
        try { Club.create(name = "offline"); fail("a server create offline did not fail") } catch (error: DbException.Network) {}
        assertEquals(1, transport.calls.count { it.operationName == "CreateClub" })
    }

    /** Opens a session with a club whose two member lists hold u1 u2 u3 u4 (JOINED) and u2 u1 u3 u4 (OWNER_FIRST). */
    private suspend fun openClubWithMembers(transport: MockTransport, models: String = modelsJSON): Triple<Club, Query<Person>, Query<Person>> {
        transport.enqueue("CreateClub", CLUB_CREATED)
        transport.enqueue("ClubMembers", membersReply(listOf("u1", "u2", "u3", "u4")))
        transport.enqueue("ClubMembers", membersReply(listOf("u2", "u1", "u3", "u4")))
        openClubSession(transport, models)
        val created = confirmedClub()
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
                removeMemberConfirmed(created)
                assertEquals("$case: the setup", listOf(listOf("u1", "u2", "u3"), listOf("u2", "u1", "u3")), lists())
            }
            val user = if (removing) "u2" else "u4"
            val hold = transport.hold(if (removing) "RemoveClubMember" else "AddClubMember", clubAnswer(if (removing) "removeClubMember" else "addClubMember"), if (refusal) FORBIDDEN else null)
            val acting = async { if (removing) created.removeMember(userIds = listOf(user)) else created.addMember(userIds = listOf(user)) }
            withTimeout(5000) { hold.entered() }
            val during = if (removing) listOf(listOf("u1", "u3", "u4"), listOf("u1", "u3", "u4")) else original
            assertEquals("$case: the lists before the answer", during, lists())
            val receipt = withTimeout(5000) { acting.await() }
            hold.release()
            val outcome = withTimeout(5000) { receipt.outcome() }
            assertTrue("$case: the outcome", if (refusal) outcome is OperationOutcome.Rejected else outcome is OperationOutcome.Confirmed)
            val after = if (refusal) (if (removing) original else listOf(listOf("u1", "u2", "u3"), listOf("u2", "u1", "u3"))) else during
            assertEquals("$case: the lists after the outcome", after, lists())
        }
    }

    /** A member whose row is not cached is skipped by the optimistic join: the call ends with the answer and the lists keep their ids. */
    @Test
    fun addingAMemberWhoseRowIsNotCachedEndsWithTheAnswer() = bounded {
        val transport = MockTransport()
        val (created, joined, owners) = openClubWithMembers(transport)
        transport.enqueue("AddClubMember", clubAnswer("addClubMember"))
        val add = created.addMember(userIds = listOf("u9"))
        assertTrue(withTimeout(5000) { add.outcome() } is OperationOutcome.Confirmed)
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
        val add = created.addMember(userIds = listOf("u9"))
        assertTrue(withTimeout(5000) { add.outcome() } is OperationOutcome.Confirmed)
        readAgain(added[0], added[1])
        assertEquals("the lists after the add", added, lists())
        val removed = listOf(listOf("u1", "u2", "u3", "u4"), listOf("u2", "u1", "u3", "u4"))
        transport.enqueue("RemoveClubMember", clubAnswer("removeClubMember"))
        val remove = created.removeMember(userIds = listOf("u9"))
        assertTrue(withTimeout(5000) { remove.outcome() } is OperationOutcome.Confirmed)
        readAgain(removed[0], removed[1])
        assertEquals("the lists after the removal", removed, lists())
    }

    /** K33 (A1): a call that waits for the answer of a mutation returns with that answer while a load of the list the mutation refetches is held. The list is read again once that load ends. */
    @Test
    fun aMutationReturnsWithItsAnswerWhileALoadOfItsRefetchedListIsHeld() = bounded {
        val transport = MockTransport()
        val (created, joined, _) = openClubWithMembers(transport)
        val scope = CoroutineScope(SupervisorJob(coroutineContext[kotlinx.coroutines.Job]))
        val list = ListReader(joined, scope)
        list.waitForInitialLoad()
        val current = listOf("u1", "u2", "u3", "u4")
        val before = transport.calls.count { it.operationName == "ClubMembers" }
        val loadHold = transport.hold("ClubMembers", membersReply(current))
        val mutationHold = transport.hold("RemoveClubMember", clubAnswer("removeClubMember"))
        val rereadHold = transport.hold("ClubMembers", membersReply(current.dropLast(1) + "u9"))
        try {
            val refreshing = async { list.refresh() }
            withTimeout(5000) { loadHold.entered() }
            val removing = async { created.removeMember(userIds = listOf("u4")) }
            withTimeout(5000) { mutationHold.entered() }
            mutationHold.release()
            val receipt = awaitWithin("the mutation receipt") { removing.await() }
            assertTrue(awaitWithin("the mutation outcome while the list load is held") { receipt.outcome() } is OperationOutcome.Confirmed)
            assertEquals(before + 1, transport.calls.count { it.operationName == "ClubMembers" })
            loadHold.release()
            awaitWithin("the reread begins") { rereadHold.entered() }
            assertEquals(before + 2, transport.calls.count { it.operationName == "ClubMembers" })
            rereadHold.release()
            awaitWithin("the refresh to end once the holds are released") { refreshing.await() }
            awaitWithin("the list shows the reread") { list.rows.first { rows -> rows.map { it.id } == current.dropLast(1) + "u9" } }
            assertEquals(before + 2, transport.calls.count { it.operationName == "ClubMembers" })
        } finally {
            loadHold.release()
            mutationHold.release()
            rereadHold.release()
            scope.cancel()
        }
    }

    /** K13 (B6, C05): leave removes the club from its list before the answer. The answer keeps it away, a refusal puts it back. */
    @Test
    fun leaveRemovesTheClubFromItsListBeforeTheAnswerAndARefusalRestoresIt() = bounded {
        for (refusal in listOf(false, true)) {
            val transport = MockTransport()
            transport.enqueue("CreateClub", CLUB_CREATED)
            openClubSession(transport)
            val created = confirmedClub()
            val hold = transport.hold("LeaveClub", clubAnswer("leaveClub"), if (refusal) FORBIDDEN else null)
            val leaving = async { created.leave() }
            withTimeout(5000) { hold.entered() }
            assertTrue("the club stayed in its list before the answer", Query("Club", Club).readRow(created.id) == null)
            hold.release()
            val receipt = withTimeout(5000) { leaving.await() }
            assertEquals("the outcome of leave", refusal, withTimeout(5000) { receipt.outcome() } is OperationOutcome.Rejected)
            eventually("the club list after the outcome") { (Query("Club", Club).readRow(created.id) != null) == refusal }
        }
    }

    /** K12: a forward into the same chat and into another chat leaves the source row unchanged, upserts the answer row in its own chat, and the call returns the answer row. */
    @Test
    fun aForwardLeavesTheSourceUnchangedUpsertsTheAnswerRowAndReturnsIt() = bounded {
        val transport = MockTransport()
        transport.enqueue("SendLetter", LETTER_SENT)
        openClubSession(transport)
        val source = confirmedLetter()
        val sourceId = source.id

        for ((target, id) in listOf("c1" to "fwd-1", "c2" to "fwd-2")) {
            transport.enqueue("ForwardLetter", forwardAnswer(id, target))
            val answer = requireNotNull(withTimeout(5000) { source.forward(targetChatId = target).outcome() }.confirmedValue)
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
            val source = confirmedLetter()
            val thread = Letter.thread("c2")
            assertTrue(thread.read().isEmpty())

            val hold = transport.hold("ForwardLetter", forwardAnswer("fwd-1", "c2"), if (refusal) FORBIDDEN else null)
            val forwarding = async { source.forward(targetChatId = "c2") }
            withTimeout(5000) { hold.entered() }
            val copy = thread.read().firstOrNull()
            assertTrue("$case: the target thread has no copy before the answer", copy != null)
            assertTrue("$case: the copy is ${copy!!.id}", copy.id != "fwd-1" && copy.text == "hello" && copy.chatId == "c2")
            hold.release()
            val receipt = withTimeout(5000) { forwarding.await() }
            assertEquals("$case: the outcome of forward", refusal, withTimeout(5000) { receipt.outcome() } is OperationOutcome.Rejected)
            eventually("the target thread shows the outcome") { thread.read().map { it.id } == (if (refusal) emptyList() else listOf("fwd-1")) }
        }
    }
}

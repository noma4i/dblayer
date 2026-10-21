package com.noma4i.dblayer

import com.noma4i.dblayer.models.AttachmentUser
import com.noma4i.dblayer.models.GiftAttachment
import com.noma4i.dblayer.models.Message
import com.noma4i.dblayer.models.MessageAttachment
import com.noma4i.dblayer.models.MessageAttachmentKind
import com.noma4i.dblayer.models.MomentAttachment
import com.noma4i.dblayer.models.MomentEmojiProps
import com.noma4i.dblayer.models.MomentWidgetItemProps
import com.noma4i.dblayer.models.ProfileAttachment
import com.noma4i.dblayer.models.ReactionAttachment
import com.noma4i.dblayer.models.UnionSample
import com.noma4i.dblayer.models.modelsJSON
import java.io.File
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class UnionTest {
    private fun comparableJson(value: JsonElement): JsonElement = when (value) {
        is JsonObject -> JsonObject(value.filterValues { it != JsonNull }.mapValues { comparableJson(it.value) })
        is JsonArray -> JsonArray(value.map(::comparableJson))
        is JsonPrimitive -> value
    }

    private suspend fun open(transport: MockTransport? = null) = Database.open(Config(null, null, 64u), modelsJSON, transport).also { DBlayer.database = it }

    @Test
    fun aStoredUnionReadsFromCellsAndEncodesLikeTheSharedCases() = bounded {
        val database = open()
        for (row in parseJson(File("../fixtures/union.json").readText()) as JsonArray) {
            val fields = row.jsonObject
            val label = fields.getValue("name").toString()
            val values = buildJsonObject { fields["attachment"]?.let { put("attachment", it) } }
            val sample = UnionSample.read(database.create("UnionSample", values.toString(), "{}"))
            val encoded = sample.attachment?.let { jsonValue(it) } ?: JsonNull
            assertEquals(label, comparableJson(fields.getValue("output")), comparableJson(encoded))
            val input = fields["attachment"]
            if (input != null && input != JsonNull) assertEquals(label, dbJson.decodeFromJsonElement(MessageAttachment.serializer(), input), sample.attachment)
        }
    }

    @Test
    fun typedVariantsAreWrittenReadSwitchedAndKeptWhenUnknown() = bounded {
        open()
        val gift = MessageAttachment.Gift(GiftAttachment(id = "g2", tier = "silver", tags = listOf("x")))
        val props = MomentWidgetItemProps.Emoji(MomentEmojiProps(emoji = "fire"))
        val created = UnionSample.create(attachment = gift, props = props)
        assertEquals(gift, created.attachment)
        assertEquals(props, created.props)

        val reaction = MessageAttachment.Reaction(ReactionAttachment(body = "+1"))
        val switched = created.update(attachment = reaction)
        assertEquals(reaction, switched.attachment)
        assertEquals(props, switched.props)

        val future = MessageAttachment.Unknown("FutureAttachment")
        val kept = switched.update(attachment = future)
        assertEquals(future, kept.attachment)
        val again = kept.update(label = "renamed")
        assertEquals(future, again.attachment)
        assertEquals("renamed", again.label)

        val none = UnionSample.create()
        assertNull(none.attachment)
        assertNull(none.props)
    }

    @Test
    fun rowsFilterByTheVariantKind() = bounded {
        open()
        UnionSample.create(attachment = MessageAttachment.Moment(MomentAttachment(momentId = "m1")))
        UnionSample.create(attachment = MessageAttachment.Moment(MomentAttachment(momentId = "m2")))
        UnionSample.create(attachment = MessageAttachment.Reaction(ReactionAttachment(body = "x")))
        UnionSample.create(attachment = MessageAttachment.Unknown("FutureAttachment"))
        UnionSample.create()
        assertEquals(2L, UnionSample.where(UnionSample.attachment eq MessageAttachmentKind.Moment).count.current())
        assertEquals(0L, UnionSample.where(UnionSample.attachment eq MessageAttachmentKind.Gift).count.current())
        assertEquals(3L, UnionSample.where(UnionSample.attachment within listOf(MessageAttachmentKind.Moment, MessageAttachmentKind.Reaction)).count.current())
        assertEquals(1L, UnionSample.where(UnionSample.attachment.isNull).count.current())
    }

    private val nodes = mapOf(
        "m1" to """{"__typename":"GiftAttachment","id":"g1","giftTier":"gold","status":"sent","tags":["a"]}""",
        "m2" to """{"__typename":"MomentAttachment","momentId":7}""",
        "m3" to """{"__typename":"ProfileAttachment","user":{"id":"u1","name":"Ada"}}""",
        "m4" to """{"__typename":"ReactionAttachment","body":"+1"}""",
        "m5" to """{"__typename":"FutureAttachment","size":3}""",
        "m6" to "null",
    )

    private fun threadReply(attachments: Map<String, String>): String {
        val rows = attachments.keys.sorted().joinToString(",") { id ->
            """{"__typename":"Message","id":"$id","chatId":"c1","authorId":"a","text":"t","createdAt":"2026-10-0${id.drop(1)}T00:00:00.000Z","sequenceNumber":${id.drop(1)},"attachment":${attachments.getValue(id)}}"""
        }
        return """{"data":{"chat":{"messages":{"nodes":[$rows],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}"""
    }

    private val threadDocument = "query ChatThread(\$chatId: ID!, \$first: Int, \$after: String) { chat(id: \$chatId) { messages(first: \$first, after: \$after) { nodes { id chatId authorId text createdAt sequenceNumber attachment { __typename ... on GiftAttachment { id giftTier: kind status tags } ... on MomentAttachment { momentId caption } ... on ProfileAttachment { user { id name } } ... on ReactionAttachment { body } } } pageInfo { hasNextPage endCursor } } } }"

    private suspend fun attachmentOf(id: String) = Message.find(id).current()?.attachment

    @Test
    fun aServerAnswerCarriesEveryVariantAndLaterAnswersReplaceTheVariant() = bounded {
        val transport = MockTransport()
        transport.enqueue("ChatThread", threadReply(nodes))
        open(transport)
        DBlayer.database.registerOperations(JsonArray(listOf(buildJsonObject { put("name", JsonPrimitive("ChatThread")); put("document", JsonPrimitive(threadDocument)) })).toString())
        val thread = Message.thread("c1")
        thread.load()

        assertEquals(MessageAttachment.Gift(GiftAttachment(id = "g1", tier = "gold", status = "sent", tags = listOf("a"))), attachmentOf("m1"))
        assertEquals(MessageAttachment.Moment(MomentAttachment(momentId = "7", caption = null)), attachmentOf("m2"))
        assertEquals(MessageAttachment.Profile(ProfileAttachment(user = AttachmentUser(id = "u1", name = "Ada"))), attachmentOf("m3"))
        assertEquals(MessageAttachment.Reaction(ReactionAttachment(body = "+1")), attachmentOf("m4"))
        assertEquals(MessageAttachment.Unknown("FutureAttachment"), attachmentOf("m5"))
        assertNull(attachmentOf("m6"))
        assertEquals(1L, Message.where(Message.attachment eq MessageAttachmentKind.Moment).count.current())
        assertEquals(1L, Message.where(Message.attachment.isNull).count.current())

        val changed = nodes + mapOf(
            "m1" to """{"__typename":"ReactionAttachment","body":"now"}""",
            "m2" to """{"__typename":"MomentAttachment","momentId":8,"caption":"edited"}""",
            "m5" to """{"__typename":"GiftAttachment","id":"g9","giftTier":"bronze","status":null,"tags":[]}""",
            "m6" to """{"__typename":"ReactionAttachment","body":"late"}""",
        )
        transport.enqueue("ChatThread", threadReply(changed))
        thread.refresh()

        assertEquals(MessageAttachment.Reaction(ReactionAttachment(body = "now")), attachmentOf("m1"))
        assertEquals(MessageAttachment.Moment(MomentAttachment(momentId = "8", caption = "edited")), attachmentOf("m2"))
        assertEquals(MessageAttachment.Profile(ProfileAttachment(user = AttachmentUser(id = "u1", name = "Ada"))), attachmentOf("m3"))
        assertEquals(MessageAttachment.Gift(GiftAttachment(id = "g9", tier = "bronze", status = null, tags = emptyList())), attachmentOf("m5"))
        assertEquals(MessageAttachment.Reaction(ReactionAttachment(body = "late")), attachmentOf("m6"))
        assertEquals(1L, Message.where(Message.attachment eq MessageAttachmentKind.Gift).count.current())
        assertEquals(3L, Message.where(Message.attachment eq MessageAttachmentKind.Reaction).count.current())
    }

    @Test
    fun aCreatedMessageTakesItsVariantFromTheServerAnswer() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMessage", """{"data":{"createMessage":{"__typename":"Message","id":"x1","chatId":"c","authorId":"u1","text":"hi","createdAt":"2026-10-08T00:00:00.000Z","attachment":{"__typename":"MomentAttachment","momentId":"m9","caption":"look"}}}}""")
        open(transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMessage","document":"mutation CreateMessage(${'$'}chatId: ID!, ${'$'}text: String!, ${'$'}signedId: String, ${'$'}attachedUserId: ID) { createMessage(chatId: ${'$'}chatId, text: ${'$'}text, signedId: ${'$'}signedId, attachedUserId: ${'$'}attachedUserId) { id chatId authorId text createdAt attachment { __typename ... on MomentAttachment { momentId caption } } } }"}]""")

        val local = Message.create(chatId = "c", authorId = "u1", text = "hi")
        assertNull(local.attachment)
        withTimeout(5000) { local.settled() }
        assertEquals(MessageAttachment.Moment(MomentAttachment(momentId = "m9", caption = "look")), attachmentOf(withTimeout(5000) { local.confirmedId() }))
    }
}

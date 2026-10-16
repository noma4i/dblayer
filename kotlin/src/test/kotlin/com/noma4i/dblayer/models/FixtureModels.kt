package com.noma4i.dblayer.models

import com.noma4i.dblayer.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*

data class Chat(
    override val id: String,
    val title: String,
    val lastMessageAt: String? = null,
    val lastMessageId: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "Chat"
    companion object : ModelDecoder<Chat> {
        override val layout = "3a6adefbaba50c45e0170f495c76a9edb9914628792627415847915b498b1fa2"
        override val modelName = "Chat"
        override fun decode(row: WireRow): Chat {
            val r = CellReader(row.cells)
            return Chat(
                id = row.id,
                title = r.req(0, Cell::asText),
                lastMessageAt = r.opt(1, Cell::asText),
                lastMessageId = r.opt(2, Cell::asId),
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val title = Column<Chat, String>("title")
        val lastMessageAt = Column<Chat, String?>("lastMessageAt")
        val lastMessageId = Column<Chat, String?>("lastMessageId")
        fun find(id: String) = Query("Chat", this).find(id)
        fun where(condition: Condition) = Query("Chat", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Chat", this, ids)
        fun entity(id: String) = entityOf("Chat", this, id)
        val recent: Query<Chat> get() {
            var built = Query("Chat", this, relation = "recent", params = buildJsonObject {  })
            built = built.order(Order("lastMessageAt", "desc"))
            return built
        }
        suspend fun create(title: String, lastMessageAt: String? = null, lastMessageId: String? = null): Chat {
            val values = buildJsonObject {
                put("title", jsonValue(title))
                lastMessageAt?.let { put("lastMessageAt", jsonValue(it)) }
                lastMessageId?.let { put("lastMessageId", jsonValue(it)) }
            }
            return read(DBlayer.database.create("Chat", values.toString(), "{}"))
        }
    }
    suspend fun update(title: String? = null, lastMessageAt: String? = null, lastMessageId: String? = null): Chat {
        val values = buildJsonObject {
            title?.let { put("title", jsonValue(it)) }
            lastMessageAt?.let { put("lastMessageAt", jsonValue(it)) }
            lastMessageId?.let { put("lastMessageId", jsonValue(it)) }
        }
        return Chat.read(DBlayer.database.update(modelName, id, values.toString(), "{}"))
    }
    val messages get() = related("messages", Message)
    val lastMessage get() = related("lastMessage", Message)
}

data class Message(
    override val id: String,
    val chatId: String,
    val authorId: String,
    val text: String,
    val createdAt: String,
    val sequenceNumber: Long? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "Message"
    companion object : ModelDecoder<Message> {
        override val layout = "9af1f9a2c43c208c4601e2110b82e9dced385c8a9603c6ac4aad6ad9f55e13ab"
        override val modelName = "Message"
        override fun decode(row: WireRow): Message {
            val r = CellReader(row.cells)
            return Message(
                id = row.id,
                chatId = r.req(0, Cell::asId),
                authorId = r.req(1, Cell::asId),
                text = r.req(2, Cell::asText),
                createdAt = r.req(3, Cell::asText),
                sequenceNumber = r.opt(4, Cell::asInt),
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val chatId = Column<Message, String>("chatId")
        val authorId = Column<Message, String>("authorId")
        val text = Column<Message, String>("text")
        val createdAt = Column<Message, String>("createdAt")
        val sequenceNumber = Column<Message, Long?>("sequenceNumber")
        fun find(id: String) = Query("Message", this).find(id)
        fun where(condition: Condition) = Query("Message", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Message", this, ids)
        fun entity(id: String) = entityOf("Message", this, id)
        fun byChat(chatId: String): Query<Message> {
            var built = Query("Message", this, relation = "by_chat", params = buildJsonObject { put("chatId", jsonValue(chatId)) })
            built = built.where(Condition.equal("chatId", jsonValue(chatId)))
            built = built.order(Order("createdAt", "asc"))
            return built
        }
        fun thread(chatId: String): Query<Message> {
            var built = Query("Message", this, relation = "thread", params = buildJsonObject { put("chatId", jsonValue(chatId)) })
            return built
        }
        val history: Query<Message> get() {
            var built = Query("Message", this, relation = "history", params = buildJsonObject {  })
            return built
        }
        suspend fun create(chatId: String, authorId: String, text: String, attachedUserId: String? = null, signedId: String? = null): Message {
            val values = buildJsonObject {
                put("chatId", jsonValue(chatId))
                put("authorId", jsonValue(authorId))
                put("text", jsonValue(text))
            }
            val args = buildJsonObject {
                attachedUserId?.let { put("attachedUserId", jsonValue(it)) }
                signedId?.let { put("signedId", jsonValue(it)) }
            }
            return read(DBlayer.database.create("Message", values.toString(), args.toString()))
        }
    }
    suspend fun update(chatId: String? = null, authorId: String? = null, text: String? = null, createdAt: String? = null, sequenceNumber: Long? = null): Message {
        val values = buildJsonObject {
            chatId?.let { put("chatId", jsonValue(it)) }
            authorId?.let { put("authorId", jsonValue(it)) }
            text?.let { put("text", jsonValue(it)) }
            createdAt?.let { put("createdAt", jsonValue(it)) }
            sequenceNumber?.let { put("sequenceNumber", jsonValue(it)) }
        }
        return Message.read(DBlayer.database.update(modelName, id, values.toString(), "{}"))
    }
    val chat get() = related("chat", Chat)
}

data class Moment(
    override val id: String,
    val uuid: String,
    val userId: String? = null,
    val text: String? = null,
    val likedByMe: Boolean,
    val media: JsonElement,
    val tone: String? = null,
    val createdAt: String,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "Moment"
    companion object : ModelDecoder<Moment> {
        override val layout = "1b66c84852ce5a61eeef6f7c5acf272acbdc9e265628ebcf4f9429e7f94772b1"
        override val modelName = "Moment"
        override fun decode(row: WireRow): Moment {
            val r = CellReader(row.cells)
            return Moment(
                id = row.id,
                uuid = r.req(0, Cell::asText),
                userId = r.opt(1, Cell::asId),
                text = r.opt(2, Cell::asText),
                likedByMe = r.opt(3, Cell::asBool) ?: false,
                media = r.jsonOpt(4) ?: JsonObject(mapOf()),
                tone = r.opt(5, Cell::asText),
                createdAt = r.req(6, Cell::asText),
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val uuid = Column<Moment, String>("uuid")
        val userId = Column<Moment, String?>("userId")
        val text = Column<Moment, String?>("text")
        val likedByMe = Column<Moment, Boolean>("likedByMe")
        val media = Column<Moment, JsonElement>("media")
        val tone = Column<Moment, String?>("tone")
        val createdAt = Column<Moment, String>("createdAt")
        fun find(id: String) = Query("Moment", this).find(id)
        fun where(condition: Condition) = Query("Moment", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Moment", this, ids)
        fun entity(id: String) = entityOf("Moment", this, id)
        val recent: Query<Moment> get() {
            var built = Query("Moment", this, relation = "recent", params = buildJsonObject {  })
            built = built.order(Order("createdAt", "desc"))
            return built
        }
        fun byUser(userId: String?): Query<Moment> {
            var built = Query("Moment", this, relation = "by_user", params = buildJsonObject { put("userId", jsonValue(userId)) })
            built = built.where(Condition.equal("userId", jsonValue(userId)))
            built = built.order(Order("createdAt", "desc"))
            return built
        }
        val feed: Query<Moment> get() {
            var built = Query("Moment", this, relation = "feed", params = buildJsonObject {  })
            return built
        }
        suspend fun create(text: String? = null, media: JsonElement? = null): Moment {
            val values = buildJsonObject {
                text?.let { put("text", jsonValue(it)) }
                media?.let { put("media", jsonValue(it)) }
            }
            return read(DBlayer.database.create("Moment", values.toString(), "{}"))
        }
    }
    suspend fun update(uuid: String? = null, userId: String? = null, text: String? = null, likedByMe: Boolean? = null, media: JsonElement? = null, createdAt: String? = null): Moment {
        val values = buildJsonObject {
            uuid?.let { put("uuid", jsonValue(it)) }
            userId?.let { put("userId", jsonValue(it)) }
            text?.let { put("text", jsonValue(it)) }
            likedByMe?.let { put("likedByMe", jsonValue(it)) }
            media?.let { put("media", jsonValue(it)) }
            createdAt?.let { put("createdAt", jsonValue(it)) }
        }
        return Moment.read(DBlayer.database.update(modelName, id, values.toString(), "{}"))
    }
    suspend fun like(): Moment = act("like", JsonObject(parseJsonObject("""{"likedByMe":true}""") + mapOf()), JsonObject(mapOf()), Moment)
    suspend fun share(): ActionAnswer<Moment, MomentShareResult> = actAnswering("share", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf()), Moment, MomentShareResult.serializer())
    val author get() = related("author", User)
    val vibes get() = related("vibes", Vibe)
}

@Serializable
data class MomentShareResult(val copies: Int? = null, val successMessage: String? = null)

data class NestedSample(
    override val id: String,
    val phase: NestedSamplePhase? = null,
    val value: NestedValue? = null,
    val values: List<NestedValue>,
    val tree: NestedTree? = null,
    val replyTo: MessageReplyNode? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "NestedSample"
    companion object : ModelDecoder<NestedSample> {
        override val layout = "b98341f178193869c1bf875ecf8a6d93ab3789eaec78d978ad679f5ac20f46ca"
        override val modelName = "NestedSample"
        override fun decode(row: WireRow): NestedSample {
            val r = CellReader(row.cells)
            return NestedSample(
                id = row.id,
                phase = r.opt(0, Cell::asText)?.let { NestedSamplePhase.of(it) },
                value = r.objOpt(1) { NestedValue.fromCells(it) },
                values = r.arrOpt(2) { NestedValue.fromCells(it) } ?: listOf(),
                tree = r.objOpt(3) { NestedTree.fromCells(it) },
                replyTo = r.objOpt(4) { MessageReplyNode.fromCells(it) },
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val phase = Column<NestedSample, NestedSamplePhase?>("phase")
        val value = Column<NestedSample, NestedValue?>("value")
        val values = Column<NestedSample, List<NestedValue>>("values")
        val tree = Column<NestedSample, NestedTree?>("tree")
        val replyTo = Column<NestedSample, MessageReplyNode?>("replyTo")
        fun find(id: String) = Query("NestedSample", this).find(id)
        fun where(condition: Condition) = Query("NestedSample", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("NestedSample", this, ids)
        fun entity(id: String) = entityOf("NestedSample", this, id)
        suspend fun create(phase: NestedSamplePhase? = null, value: NestedValue? = null, values: List<NestedValue>? = null, tree: NestedTree? = null, replyTo: MessageReplyNode? = null): NestedSample {
            val values_ = buildJsonObject {
                phase?.let { put("phase", jsonValue(it)) }
                value?.let { put("value", jsonValue(it)) }
                values?.let { put("values", jsonValue(it)) }
                tree?.let { put("tree", jsonValue(it)) }
                replyTo?.let { put("replyTo", jsonValue(it)) }
            }
            return read(DBlayer.database.create("NestedSample", values_.toString(), "{}"))
        }
    }
    suspend fun update(phase: NestedSamplePhase? = null, value: NestedValue? = null, values: List<NestedValue>? = null, tree: NestedTree? = null, replyTo: MessageReplyNode? = null): NestedSample {
        val values_ = buildJsonObject {
            phase?.let { put("phase", jsonValue(it)) }
            value?.let { put("value", jsonValue(it)) }
            values?.let { put("values", jsonValue(it)) }
            tree?.let { put("tree", jsonValue(it)) }
            replyTo?.let { put("replyTo", jsonValue(it)) }
        }
        return NestedSample.read(DBlayer.database.update(modelName, id, values_.toString(), "{}"))
    }
}

data class NestedReference(
    override val id: String,
    val values: List<NestedValue>,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "NestedReference"
    companion object : ModelDecoder<NestedReference> {
        override val layout = "1f93bebc94e76afff074fd047e84d5066b4d70676ff703781e6eec12866579e4"
        override val modelName = "NestedReference"
        override fun decode(row: WireRow): NestedReference {
            val r = CellReader(row.cells)
            return NestedReference(
                id = row.id,
                values = r.arrOpt(0) { NestedValue.fromCells(it) } ?: listOf(),
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val values = Column<NestedReference, List<NestedValue>>("values")
        fun find(id: String) = Query("NestedReference", this).find(id)
        fun where(condition: Condition) = Query("NestedReference", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("NestedReference", this, ids)
        fun entity(id: String) = entityOf("NestedReference", this, id)
        suspend fun create(values: List<NestedValue>? = null): NestedReference {
            val values_ = buildJsonObject {
                values?.let { put("values", jsonValue(it)) }
            }
            return read(DBlayer.database.create("NestedReference", values_.toString(), "{}"))
        }
    }
    suspend fun update(values: List<NestedValue>? = null): NestedReference {
        val values_ = buildJsonObject {
            values?.let { put("values", jsonValue(it)) }
        }
        return NestedReference.read(DBlayer.database.update(modelName, id, values_.toString(), "{}"))
    }
}

data class Presence(
    override val id: String,
    val userId: String,
    val room: String,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "Presence"
    companion object : ModelDecoder<Presence> {
        override val layout = "6389b939e12075c3b9a36a258c0ebb41dee76a6ce786ccf5d21f28dad18bfc97"
        override val modelName = "Presence"
        override fun decode(row: WireRow): Presence {
            val r = CellReader(row.cells)
            return Presence(
                id = row.id,
                userId = r.req(0, Cell::asId),
                room = r.req(1, Cell::asText),
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val userId = Column<Presence, String>("userId")
        val room = Column<Presence, String>("room")
        fun find(id: String) = Query("Presence", this).find(id)
        fun where(condition: Condition) = Query("Presence", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Presence", this, ids)
        fun entity(id: String) = entityOf("Presence", this, id)
        fun forUser(userId: String): Query<Presence> {
            var built = Query("Presence", this, relation = "for_user", params = buildJsonObject { put("userId", jsonValue(userId)) })
            built = built.where(Condition.equal("userId", jsonValue(userId)))
            return built
        }
        val online: Query<Presence> get() {
            var built = Query("Presence", this, relation = "online", params = buildJsonObject {  })
            return built
        }
        suspend fun create(userId: String, room: String): Presence {
            val values = buildJsonObject {
                put("userId", jsonValue(userId))
                put("room", jsonValue(room))
            }
            return read(DBlayer.database.create("Presence", values.toString(), "{}"))
        }
    }
    suspend fun update(userId: String? = null, room: String? = null): Presence {
        val values = buildJsonObject {
            userId?.let { put("userId", jsonValue(it)) }
            room?.let { put("room", jsonValue(it)) }
        }
        return Presence.read(DBlayer.database.update(modelName, id, values.toString(), "{}"))
    }
    val user get() = related("user", User)
}

data class StorageRow(
    override val id: String,
    val record: String,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "StorageRow"
    companion object : ModelDecoder<StorageRow> {
        override val layout = "fe5fbb4f29b1efa251b84c3acf30c1c33a863db299f28340d13b798cba3d174f"
        override val modelName = "StorageRow"
        override fun decode(row: WireRow): StorageRow {
            val r = CellReader(row.cells)
            return StorageRow(
                id = row.id,
                record = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val record = Column<StorageRow, String>("record")
        fun find(id: String) = Query("StorageRow", this).find(id)
        fun where(condition: Condition) = Query("StorageRow", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("StorageRow", this, ids)
        fun entity(id: String) = entityOf("StorageRow", this, id)
        suspend fun create(record: String): StorageRow {
            val values = buildJsonObject {
                put("record", jsonValue(record))
            }
            return read(DBlayer.database.create("StorageRow", values.toString(), "{}"))
        }
    }
    suspend fun update(record: String? = null): StorageRow {
        val values = buildJsonObject {
            record?.let { put("record", jsonValue(it)) }
        }
        return StorageRow.read(DBlayer.database.update(modelName, id, values.toString(), "{}"))
    }
}

data class User(
    override val id: String,
    val name: String,
    val avatar: JsonElement? = null,
    val birthday: String? = null,
    val typing: Boolean,
    val mood: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "User"
    companion object : ModelDecoder<User> {
        override val layout = "4849bf8a3175334b01f39299c6e9512f671e8acabd8119121c8cc168047fb390"
        override val modelName = "User"
        override fun decode(row: WireRow): User {
            val r = CellReader(row.cells)
            return User(
                id = row.id,
                name = r.req(0, Cell::asText),
                avatar = r.jsonOpt(1),
                birthday = r.opt(2, Cell::asText),
                typing = r.opt(3, Cell::asBool) ?: false,
                mood = r.opt(4, Cell::asText),
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val name = Column<User, String>("name")
        val avatar = Column<User, JsonElement?>("avatar")
        val birthday = Column<User, String?>("birthday")
        fun find(id: String) = Query("User", this).find(id)
        fun where(condition: Condition) = Query("User", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("User", this, ids)
        fun entity(id: String) = entityOf("User", this, id)
        val profile: Query<User> get() {
            var built = Query("User", this, relation = "profile", params = buildJsonObject {  })
            return built
        }
        fun entity() = singletonEntityOf("User", this, profile)
        suspend fun create(name: String, avatar: JsonElement? = null, birthday: String? = null): User {
            val values = buildJsonObject {
                put("name", jsonValue(name))
                avatar?.let { put("avatar", jsonValue(it)) }
                birthday?.let { put("birthday", jsonValue(it)) }
            }
            return read(DBlayer.database.create("User", values.toString(), "{}"))
        }
    }
    suspend fun update(name: String? = null, avatar: JsonElement? = null, birthday: String? = null): User {
        val values = buildJsonObject {
            name?.let { put("name", jsonValue(it)) }
            avatar?.let { put("avatar", jsonValue(it)) }
            birthday?.let { put("birthday", jsonValue(it)) }
        }
        return User.read(DBlayer.database.update(modelName, id, values.toString(), "{}"))
    }
    suspend fun set(typing: Boolean? = null, mood: String? = null) {
        DBlayer.database.setEphemeral(modelName, id, buildJsonObject {
            typing?.let { put("typing", jsonValue(it)) }
            mood?.let { put("mood", jsonValue(it)) }
        }.toString())
    }
}

data class Vibe(
    override val id: String,
    val momentId: String,
    val userId: String,
    val kind: VibeKind,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "Vibe"
    companion object : ModelDecoder<Vibe> {
        override val layout = "091ba73dd97a9be52544a900fa4ddd6515d3e70202d2fb1eb080d7211c9610b0"
        override val modelName = "Vibe"
        override fun decode(row: WireRow): Vibe {
            val r = CellReader(row.cells)
            return Vibe(
                id = row.id,
                momentId = r.req(0, Cell::asId),
                userId = r.req(1, Cell::asId),
                kind = r.req(2, Cell::asText).let { VibeKind.of(it) },
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val momentId = Column<Vibe, String>("momentId")
        val userId = Column<Vibe, String>("userId")
        val kind = Column<Vibe, VibeKind>("kind")
        fun find(id: String) = Query("Vibe", this).find(id)
        fun where(condition: Condition) = Query("Vibe", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Vibe", this, ids)
        fun entity(id: String) = entityOf("Vibe", this, id)
        suspend fun create(momentId: String, userId: String, kind: VibeKind): Vibe {
            val values = buildJsonObject {
                put("momentId", jsonValue(momentId))
                put("userId", jsonValue(userId))
                put("kind", jsonValue(kind))
            }
            return read(DBlayer.database.create("Vibe", values.toString(), "{}"))
        }
    }
    suspend fun update(momentId: String? = null, userId: String? = null, kind: VibeKind? = null): Vibe {
        val values = buildJsonObject {
            momentId?.let { put("momentId", jsonValue(it)) }
            userId?.let { put("userId", jsonValue(it)) }
            kind?.let { put("kind", jsonValue(it)) }
        }
        return Vibe.read(DBlayer.database.update(modelName, id, values.toString(), "{}"))
    }
}

@Serializable
data class NestedValue(
    val title: String,
    val note: String? = null,
    val nullableDefault: String? = "default",
    @SerialName("owner_id") @Serializable(with = IDText::class) val ownerId: String? = null,
    val count: Long = 2L,
    val ratio: Double = 1.5,
    val enabled: Boolean = false,
    val createdAt: String? = null,
    val state: NestedValueState = NestedValueState.Ready,
    val labels: List<String> = listOf(),
    val counts: List<Long> = listOf(),
    val ratios: List<Double> = listOf(),
    val flags: List<Boolean> = listOf(),
    val dates: List<String> = listOf(),
    @Serializable(with = IDTexts::class) val owners: List<String> = listOf(),
    val payload: JsonElement? = null,
    val rawValue: JsonElement = JsonObject(mapOf("free" to JsonArray(listOf(JsonPrimitive(1), JsonPrimitive(true), JsonNull)))),
    val preferences: NestedValuePreferences = NestedValuePreferences(label = "base"),
    val presets: List<NestedValuePreferences> = listOf(NestedValuePreferences(label = "first")),
    val metadata: JsonElement? = null,
    val values: JsonElement? = null,
    val child: NestedValueChild? = null,
    val children: List<NestedValueChild> = listOf(),
) {
    companion object {
        fun fromCells(cells: List<Cell>): NestedValue {
            val r = CellReader(cells)
            return NestedValue(
                title = r.req(0, Cell::asText),
                note = r.opt(1, Cell::asText),
                nullableDefault = r.opt(2, Cell::asText) ?: "default",
                ownerId = r.opt(3, Cell::asId),
                count = r.opt(4, Cell::asInt) ?: 2L,
                ratio = r.opt(5, Cell::asDouble) ?: 1.5,
                enabled = r.opt(6, Cell::asBool) ?: false,
                createdAt = r.opt(7, Cell::asText),
                state = r.opt(8, Cell::asText)?.let { NestedValueState.of(it) } ?: NestedValueState.Ready,
                labels = r.listOpt(9, Cell::asText) ?: listOf(),
                counts = r.listOpt(10, Cell::asInt) ?: listOf(),
                ratios = r.listOpt(11, Cell::asDouble) ?: listOf(),
                flags = r.listOpt(12, Cell::asBool) ?: listOf(),
                dates = r.listOpt(13, Cell::asText) ?: listOf(),
                owners = r.listOpt(14, Cell::asId) ?: listOf(),
                payload = r.jsonOpt(15),
                rawValue = r.jsonOpt(16) ?: JsonObject(mapOf("free" to JsonArray(listOf(JsonPrimitive(1), JsonPrimitive(true), JsonNull)))),
                preferences = r.objOpt(17) { NestedValuePreferences.fromCells(it) } ?: NestedValuePreferences(label = "base"),
                presets = r.arrOpt(18) { NestedValuePreferences.fromCells(it) } ?: listOf(NestedValuePreferences(label = "first")),
                metadata = r.jsonOpt(19),
                values = r.jsonOpt(20),
                child = r.objOpt(21) { NestedValueChild.fromCells(it) },
                children = r.arrOpt(22) { NestedValueChild.fromCells(it) } ?: listOf(),
            )
        }
    }
}

@Serializable
data class NestedValuePreferences(
    val label: String,
    val active: Boolean = true,
) {
    companion object {
        fun fromCells(cells: List<Cell>): NestedValuePreferences {
            val r = CellReader(cells)
            return NestedValuePreferences(
                label = r.req(0, Cell::asText),
                active = r.opt(1, Cell::asBool) ?: true,
            )
        }
    }
}

@Serializable
data class NestedValueChild(
    val body: String? = null,
    @SerialName("x") val horizontal: Double = 0.0,
) {
    companion object {
        fun fromCells(cells: List<Cell>): NestedValueChild {
            val r = CellReader(cells)
            return NestedValueChild(
                body = r.opt(0, Cell::asText),
                horizontal = r.opt(1, Cell::asDouble) ?: 0.0,
            )
        }
    }
}

@Serializable
data class NestedTree(
    val name: String,
    val children: List<NestedTree> = listOf(),
) {
    companion object {
        fun fromCells(cells: List<Cell>): NestedTree {
            val r = CellReader(cells)
            return NestedTree(
                name = r.req(0, Cell::asText),
                children = r.arrOpt(1) { NestedTree.fromCells(it) } ?: listOf(),
            )
        }
    }
}

@Serializable
data class MessageReplyNode(
    @Serializable(with = IDText::class) val id: String? = null,
    val body: String? = null,
    val kind: String? = null,
    @Serializable(with = IDText::class) val userId: String? = null,
    val user: MessageUserRef? = null,
    val media: NestedMedia? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): MessageReplyNode {
            val r = CellReader(cells)
            return MessageReplyNode(
                id = r.opt(0, Cell::asId),
                body = r.opt(1, Cell::asText),
                kind = r.opt(2, Cell::asText),
                userId = r.opt(3, Cell::asId),
                user = r.objOpt(4) { MessageUserRef.fromCells(it) },
                media = r.objOpt(5) { NestedMedia.fromCells(it) },
            )
        }
    }
}

@Serializable
data class MessageUserRef(
    @Serializable(with = IDText::class) val id: String? = null,
    val fullName: String? = null,
    val name: String? = null,
    val username: String? = null,
    val avatarUrl: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): MessageUserRef {
            val r = CellReader(cells)
            return MessageUserRef(
                id = r.opt(0, Cell::asId),
                fullName = r.opt(1, Cell::asText),
                name = r.opt(2, Cell::asText),
                username = r.opt(3, Cell::asText),
                avatarUrl = r.opt(4, Cell::asText),
            )
        }
    }
}

@Serializable
data class NestedMedia(
    @Serializable(with = IDText::class) val id: String = "",
    val fileUrl: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): NestedMedia {
            val r = CellReader(cells)
            return NestedMedia(
                id = r.opt(0, Cell::asId) ?: "",
                fileUrl = r.opt(1, Cell::asText),
            )
        }
    }
}

@Serializable(with = NestedSamplePhaseSerializer::class)
sealed class NestedSamplePhase(override val raw: String) : RawEnum {
    data object Draft : NestedSamplePhase("draft")
    data object Live : NestedSamplePhase("live")
    data class Unknown(val value: String) : NestedSamplePhase(value)

    companion object {
        fun of(raw: String): NestedSamplePhase = when (raw) {
            "draft" -> Draft
            "live" -> Live
            else -> Unknown(raw)
        }
    }
}

object NestedSamplePhaseSerializer : RawEnumSerializer<NestedSamplePhase>("NestedSamplePhase", { NestedSamplePhase.of(it) })

@Serializable(with = NestedValueStateSerializer::class)
sealed class NestedValueState(override val raw: String) : RawEnum {
    data object Ready : NestedValueState("ready")
    data object Done : NestedValueState("done")
    data class Unknown(val value: String) : NestedValueState(value)

    companion object {
        fun of(raw: String): NestedValueState = when (raw) {
            "ready" -> Ready
            "done" -> Done
            else -> Unknown(raw)
        }
    }
}

object NestedValueStateSerializer : RawEnumSerializer<NestedValueState>("NestedValueState", { NestedValueState.of(it) })

@Serializable(with = VibeKindSerializer::class)
sealed class VibeKind(override val raw: String) : RawEnum {
    data object Like : VibeKind("like")
    data object Love : VibeKind("love")
    data object Laugh : VibeKind("laugh")
    data class Unknown(val value: String) : VibeKind(value)

    companion object {
        fun of(raw: String): VibeKind = when (raw) {
            "like" -> Like
            "love" -> Love
            "laugh" -> Laugh
            else -> Unknown(raw)
        }
    }
}

object VibeKindSerializer : RawEnumSerializer<VibeKind>("VibeKind", { VibeKind.of(it) })

val modelsJSON = """[{"key":"Chat","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastMessageAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastMessageId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"messages","kind":"has_many","target":"Message","foreign_key":"chatId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null},{"name":"lastMessage","kind":"belongs_to","target":"Message","foreign_key":"lastMessageId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"lastMessageAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateChat","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Message","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"authorId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"sequenceNumber","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"chat","kind":"belongs_to","target":"Chat","foreign_key":"chatId","dependent":null,"touches_parent":false,"sets":{"lastMessageAt":"createdAt","lastMessageId":"id"},"newer":[{"child":"createdAt","parent":"lastMessageAt"}],"from":null}],"relations":[{"name":"by_chat","by_columns":["chatId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"asc"}]},"joins_events":false,"joins_created":false},{"name":"thread","by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatThread","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[],"page_size":30,"variables":{},"root":"chat.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc_nulls_last"},{"column":"createdAt","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"history","by_columns":[],"text_params":[],"remote":{"operation":"ChatMessages","shape":{"type":"connection","direction":"backward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"1.minute","empty_stale_time":"10.seconds","selects_page_info":true,"updates":[],"page_size":30,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateMessage","variables":{"attachedUserId":"attachedUserId","chatId":"chatId","signedId":"signedId","text":"text"},"optimistic":{},"refetch":[],"args":{"attachedUserId":"id?","signedId":"string?"},"local":["authorId"],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"uuid","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"searchable":true},{"name":"likedByMe","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"media","kind":{"type":"object"},"nullable":false,"optional":false,"default":{},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tone","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":{"type":"local"}},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"author","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":true,"sets":{},"newer":[],"from":null},{"name":"vibes","kind":"has_many","target":"Vibe","foreign_key":"momentId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"by_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"feed","by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[{"subscription":"MomentEvents","deleted_by":"destroyed","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[["uuid"],["userId","createdAt"]],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateMoment","variables":{"media":"media","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"update","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"UpdateMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"destroy","root_shape":"destroy","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"DeleteMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"like","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"LikeMoment","variables":{"momentId":"id"},"optimistic":{"likedByMe":true},"refetch":["feed"],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"share","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"ShareMoment","variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"shareMoment.moment","lands":[],"result":{"copies":"int","successMessage":"string"}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"NestedSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"phase","kind":{"type":"enum","values":["draft","live"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedSamplePhase","merge":null},{"name":"value","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null,"fields":[{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"note","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"nullableDefault","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":"default","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ownerId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"owner_id","app_type":null,"merge":null},{"name":"count","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":2,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratio","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":1.5,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"enabled","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"state","kind":{"type":"enum","values":["ready","done"]},"nullable":false,"optional":false,"default":"ready","indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueState","merge":null},{"name":"labels","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"counts","kind":{"type":"list","element":"int"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratios","kind":{"type":"list","element":"num"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"flags","kind":{"type":"list","element":"bool"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"dates","kind":{"type":"list","element":"date"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"owners","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"payload","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"rawValue","kind":{"type":"raw"},"nullable":false,"optional":false,"default":{"free":[1,true,null]},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"preferences","kind":{"type":"object"},"nullable":false,"optional":false,"default":{"label":"base"},"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null,"fields":[{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":true,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"presets","kind":{"type":"array"},"nullable":false,"optional":false,"default":[{"label":"first"}],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null},{"name":"metadata","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"child","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"horizontal","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":"x","app_type":null,"merge":null}]},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null}]},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null},{"name":"tree","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null,"fields":[{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null}]},{"name":"replyTo","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageReplyNode","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageUserRef","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fullName","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"username","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatarUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedMedia","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":"","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"NestedReference","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"Presence","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"room","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"user","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"for_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"online","by_columns":[],"text_params":[],"remote":{"operation":"PresenceOnline","shape":{"type":"list","coverage":"complete"},"stale_time":"30.seconds","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"PresenceChanged","deleted_by":"left","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"StorageRow","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"record","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"User","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatar","kind":{"type":"file"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"birthday","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"profile","by_columns":[],"text_params":[],"remote":{"operation":"UserProfile","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"UserTyping","deleted_by":null,"set":{"typing":true},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[{"name":"typing","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":"5.seconds"},{"name":"mood","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":null}],"storage":"disk","expire_after":null,"listens":[]},{"key":"Vibe","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"enum","values":["like","love","laugh"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"VibeKind","merge":null}],"associations":[],"relations":[],"indexes":[["userId"]],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]}]"""

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
    val shareUrl: String? = null,
    val isPublic: Boolean? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "Chat"
    companion object : ModelDecoder<Chat> {
        override val layout = "9a2bed4e896c382db775b230c1ca95bd8142cfc7852a35c270abc4cbb3a6b541"
        override val modelName = "Chat"
        override fun decode(row: WireRow): Chat {
            val r = CellReader(row.cells)
            return Chat(
                id = row.id,
                title = r.req(0, Cell::asText),
                lastMessageAt = r.opt(1, Cell::asText),
                lastMessageId = r.opt(2, Cell::asId),
                shareUrl = r.opt(3, Cell::asText),
                isPublic = r.opt(4, Cell::asBool),
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val title = Column<Chat, String>("title")
        val lastMessageAt = Column<Chat, String?>("lastMessageAt")
        val lastMessageId = Column<Chat, String?>("lastMessageId")
        val shareUrl = Column<Chat, String?>("shareUrl")
        val isPublic = Column<Chat, Boolean?>("isPublic")
        fun find(id: String) = Query("Chat", this).find(id)
        fun where(condition: Condition) = Query("Chat", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Chat", this, ids)
        fun entity(id: String) = entityOf("Chat", this, id)
        suspend fun node(id: String, force: Boolean = false): Chat? = DBlayer.database.node("Chat", "node", id, force)?.let { read(it) }
        suspend fun nodes(ids: List<String>): List<Chat> = DBlayer.database.nodes("Chat", "nodes", ids).map { read(it) }
        val recent: Query<Chat> get() {
            var built = Query("Chat", this, relation = "recent", params = buildJsonObject {  })
            built = built.order(Order("lastMessageAt", "desc"))
            return built
        }
        suspend fun create(title: String, lastMessageAt: String? = null, lastMessageId: String? = null, shareUrl: String? = null, isPublic: Boolean? = null): Chat {
            val values = buildJsonObject {
                put("title", jsonValue(title))
                lastMessageAt?.let { put("lastMessageAt", jsonValue(it)) }
                lastMessageId?.let { put("lastMessageId", jsonValue(it)) }
                shareUrl?.let { put("shareUrl", jsonValue(it)) }
                isPublic?.let { put("isPublic", jsonValue(it)) }
            }
            return read(DBlayer.database.create("Chat", values.toString(), "{}"))
        }
    }
    suspend fun update(title: String? = null, lastMessageAt: String? = null, lastMessageId: String? = null, shareUrl: String? = null, isPublic: Boolean? = null): Chat {
        val values = buildJsonObject {
            title?.let { put("title", jsonValue(it)) }
            lastMessageAt?.let { put("lastMessageAt", jsonValue(it)) }
            lastMessageId?.let { put("lastMessageId", jsonValue(it)) }
            shareUrl?.let { put("shareUrl", jsonValue(it)) }
            isPublic?.let { put("isPublic", jsonValue(it)) }
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
    val attachment: MessageAttachment? = null,
    val media: MessageMedia? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "Message"
    companion object : ModelDecoder<Message> {
        override val layout = "8b789705ea70aee5243f555f1e6f2efd5c8f00d31f63826d8db1cf73fc4fb31e"
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
                attachment = r.objOpt(5) { MessageAttachment.fromCells(it) },
                media = r.objOpt(6) { MessageMedia.fromCells(it) },
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val chatId = Column<Message, String>("chatId")
        val authorId = Column<Message, String>("authorId")
        val text = Column<Message, String>("text")
        val createdAt = Column<Message, String>("createdAt")
        val sequenceNumber = Column<Message, Long?>("sequenceNumber")
        val attachment = Column<Message, MessageAttachmentKind>("attachment")
        val media = Column<Message, MessageMedia?>("media")
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
        suspend fun transcodeStatus(id: String, force: Boolean = false): Message? = DBlayer.database.node("Message", "transcode_status", id, force)?.let { read(it) }
        fun chatMedia(chatId: String): Query<Message> {
            var built = Query("Message", this, relation = "chat_media", params = buildJsonObject { put("chatId", jsonValue(chatId)) })
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
    suspend fun update(chatId: String? = null, authorId: String? = null, text: String? = null, createdAt: String? = null, sequenceNumber: Long? = null, attachment: MessageAttachment? = null, media: MessageMedia? = null): Message {
        val values = buildJsonObject {
            chatId?.let { put("chatId", jsonValue(it)) }
            authorId?.let { put("authorId", jsonValue(it)) }
            text?.let { put("text", jsonValue(it)) }
            createdAt?.let { put("createdAt", jsonValue(it)) }
            sequenceNumber?.let { put("sequenceNumber", jsonValue(it)) }
            attachment?.let { put("attachment", jsonValue(it)) }
            media?.let { put("media", jsonValue(it)) }
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
        suspend fun transcodeStatus(id: String, force: Boolean = false): Moment? = DBlayer.database.node("Moment", "transcode_status", id, force)?.let { read(it) }
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
        fun feedEdge(id: String): MomentFeedEdge? = feed.edgeValues(id)?.let { MomentFeedEdge(it) }
        suspend fun readFeedEdge(id: String): MomentFeedEdge? = feed.readEdgeValues(id)?.let { MomentFeedEdge(it) }
        fun vibeFeed(vibeId: String): Query<Moment> {
            var built = Query("Moment", this, relation = "vibe_feed", params = buildJsonObject { put("vibeId", jsonValue(vibeId)) })
            return built
        }
        fun vibeFeedEdge(vibeId: String, id: String): MomentVibeFeedEdge? = vibeFeed(vibeId).edgeValues(id)?.let { MomentVibeFeedEdge(it) }
        suspend fun readVibeFeedEdge(vibeId: String, id: String): MomentVibeFeedEdge? = vibeFeed(vibeId).readEdgeValues(id)?.let { MomentVibeFeedEdge(it) }
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

data class CustomScreen(
    override val id: String,
    val slug: String,
    val spec: ScreenSpec? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "CustomScreen"
    companion object : ModelDecoder<CustomScreen> {
        override val layout = "2ee8e83236c749b2249aed88873cc60db6064cf630c886ed538336525207ffbc"
        override val modelName = "CustomScreen"
        override fun decode(row: WireRow): CustomScreen {
            val r = CellReader(row.cells)
            return CustomScreen(
                id = row.id,
                slug = r.req(0, Cell::asText),
                spec = r.objOpt(1) { ScreenSpec.fromCells(it) },
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val slug = Column<CustomScreen, String>("slug")
        val spec = Column<CustomScreen, ScreenSpec?>("spec")
        fun find(id: String) = Query("CustomScreen", this).find(id)
        fun where(condition: Condition) = Query("CustomScreen", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("CustomScreen", this, ids)
        fun entity(id: String) = entityOf("CustomScreen", this, id)
        suspend fun node(id: String, force: Boolean = false): CustomScreen? = DBlayer.database.node("CustomScreen", "node", id, force)?.let { read(it) }
        suspend fun create(slug: String, spec: ScreenSpec? = null): CustomScreen {
            val values = buildJsonObject {
                put("slug", jsonValue(slug))
                spec?.let { put("spec", jsonValue(it)) }
            }
            return read(DBlayer.database.create("CustomScreen", values.toString(), "{}"))
        }
    }
    suspend fun update(slug: String? = null, spec: ScreenSpec? = null): CustomScreen {
        val values = buildJsonObject {
            slug?.let { put("slug", jsonValue(it)) }
            spec?.let { put("spec", jsonValue(it)) }
        }
        return CustomScreen.read(DBlayer.database.update(modelName, id, values.toString(), "{}"))
    }
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

data class UnionSample(
    override val id: String,
    val label: String? = null,
    val attachment: MessageAttachment? = null,
    val props: MomentWidgetItemProps? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "UnionSample"
    companion object : ModelDecoder<UnionSample> {
        override val layout = "1ee46c4fef7a5114130959afff43237741233a8211889420ea108ba15052bd58"
        override val modelName = "UnionSample"
        override fun decode(row: WireRow): UnionSample {
            val r = CellReader(row.cells)
            return UnionSample(
                id = row.id,
                label = r.opt(0, Cell::asText),
                attachment = r.objOpt(1) { MessageAttachment.fromCells(it) },
                props = r.objOpt(2) { MomentWidgetItemProps.fromCells(it) },
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val label = Column<UnionSample, String?>("label")
        val attachment = Column<UnionSample, MessageAttachmentKind>("attachment")
        val props = Column<UnionSample, MomentWidgetItemPropsKind>("props")
        fun find(id: String) = Query("UnionSample", this).find(id)
        fun where(condition: Condition) = Query("UnionSample", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("UnionSample", this, ids)
        fun entity(id: String) = entityOf("UnionSample", this, id)
        suspend fun create(label: String? = null, attachment: MessageAttachment? = null, props: MomentWidgetItemProps? = null): UnionSample {
            val values = buildJsonObject {
                label?.let { put("label", jsonValue(it)) }
                attachment?.let { put("attachment", jsonValue(it)) }
                props?.let { put("props", jsonValue(it)) }
            }
            return read(DBlayer.database.create("UnionSample", values.toString(), "{}"))
        }
    }
    suspend fun update(label: String? = null, attachment: MessageAttachment? = null, props: MomentWidgetItemProps? = null): UnionSample {
        val values = buildJsonObject {
            label?.let { put("label", jsonValue(it)) }
            attachment?.let { put("attachment", jsonValue(it)) }
            props?.let { put("props", jsonValue(it)) }
        }
        return UnionSample.read(DBlayer.database.update(modelName, id, values.toString(), "{}"))
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
        fun chatMembers(chatId: String): Query<User> {
            var built = Query("User", this, relation = "chat_members", params = buildJsonObject { put("chatId", jsonValue(chatId)) })
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

data class MomentFeedEdge(
    val impressionUuid: String? = null,
    val impressionScore: Long? = null
) {
    internal constructor(values: JsonObject) : this((values["impressionUuid"] as? JsonPrimitive)?.contentOrNull, (values["impressionScore"] as? JsonPrimitive)?.longOrNull)
}

data class MomentVibeFeedEdge(
    val impressionUuid: String? = null
) {
    internal constructor(values: JsonObject) : this((values["impressionUuid"] as? JsonPrimitive)?.contentOrNull)
}

@Serializable(with = MessageAttachmentSerializer::class)
sealed class MessageAttachment {
    data class Gift(val value: GiftAttachment) : MessageAttachment()
    data class Moment(val value: MomentAttachment) : MessageAttachment()
    data class Profile(val value: ProfileAttachment) : MessageAttachment()
    data class Reaction(val value: ReactionAttachment) : MessageAttachment()
    data class Unknown(val typename: String) : MessageAttachment()

    val kind: MessageAttachmentKind?
        get() = when (this) {
            is Gift -> MessageAttachmentKind.Gift
            is Moment -> MessageAttachmentKind.Moment
            is Profile -> MessageAttachmentKind.Profile
            is Reaction -> MessageAttachmentKind.Reaction
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): MessageAttachment {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "GiftAttachment" -> Gift(r.obj(1) { GiftAttachment.fromCells(it) })
                "MomentAttachment" -> Moment(r.obj(1) { MomentAttachment.fromCells(it) })
                "ProfileAttachment" -> Profile(r.obj(1) { ProfileAttachment.fromCells(it) })
                "ReactionAttachment" -> Reaction(r.obj(1) { ReactionAttachment.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object MessageAttachmentSerializer : UnionSerializer<MessageAttachment>("MessageAttachment") {
    override fun read(typename: String, body: JsonObject): MessageAttachment = when (typename) {
        "GiftAttachment" -> MessageAttachment.Gift(dbJson.decodeFromJsonElement(GiftAttachment.serializer(), body))
        "MomentAttachment" -> MessageAttachment.Moment(dbJson.decodeFromJsonElement(MomentAttachment.serializer(), body))
        "ProfileAttachment" -> MessageAttachment.Profile(dbJson.decodeFromJsonElement(ProfileAttachment.serializer(), body))
        "ReactionAttachment" -> MessageAttachment.Reaction(dbJson.decodeFromJsonElement(ReactionAttachment.serializer(), body))
        else -> MessageAttachment.Unknown(typename)
    }

    override fun write(value: MessageAttachment): Pair<String, JsonElement> = when (value) {
        is MessageAttachment.Gift -> "GiftAttachment" to dbJson.encodeToJsonElement(GiftAttachment.serializer(), value.value)
        is MessageAttachment.Moment -> "MomentAttachment" to dbJson.encodeToJsonElement(MomentAttachment.serializer(), value.value)
        is MessageAttachment.Profile -> "ProfileAttachment" to dbJson.encodeToJsonElement(ProfileAttachment.serializer(), value.value)
        is MessageAttachment.Reaction -> "ReactionAttachment" to dbJson.encodeToJsonElement(ReactionAttachment.serializer(), value.value)
        is MessageAttachment.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class MessageAttachmentKind {
    @SerialName("GiftAttachment") Gift,
    @SerialName("MomentAttachment") Moment,
    @SerialName("ProfileAttachment") Profile,
    @SerialName("ReactionAttachment") Reaction,
}

@Serializable
data class GiftAttachment(
    @Serializable(with = IDText::class) val id: String,
    @SerialName("giftTier") val tier: String,
    val status: String? = null,
    val tags: List<String> = listOf(),
) {
    companion object {
        fun fromCells(cells: List<Cell>): GiftAttachment {
            val r = CellReader(cells)
            return GiftAttachment(
                id = r.req(0, Cell::asId),
                tier = r.req(1, Cell::asText),
                status = r.opt(2, Cell::asText),
                tags = r.listOpt(3, Cell::asText) ?: listOf(),
            )
        }
    }
}

@Serializable
data class MomentAttachment(
    @Serializable(with = IDText::class) val momentId: String,
    val caption: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): MomentAttachment {
            val r = CellReader(cells)
            return MomentAttachment(
                momentId = r.req(0, Cell::asId),
                caption = r.opt(1, Cell::asText),
            )
        }
    }
}

@Serializable
data class ProfileAttachment(
    val user: AttachmentUser? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ProfileAttachment {
            val r = CellReader(cells)
            return ProfileAttachment(
                user = r.objOpt(0) { AttachmentUser.fromCells(it) },
            )
        }
    }
}

@Serializable
data class AttachmentUser(
    @Serializable(with = IDText::class) val id: String,
    val name: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): AttachmentUser {
            val r = CellReader(cells)
            return AttachmentUser(
                id = r.req(0, Cell::asId),
                name = r.opt(1, Cell::asText),
            )
        }
    }
}

@Serializable
data class ReactionAttachment(
    val body: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ReactionAttachment {
            val r = CellReader(cells)
            return ReactionAttachment(
                body = r.req(0, Cell::asText),
            )
        }
    }
}

@Serializable
data class MessageMedia(
    val kind: String? = null,
    val transcoded: Boolean? = null,
    val fileUrl: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): MessageMedia {
            val r = CellReader(cells)
            return MessageMedia(
                kind = r.opt(0, Cell::asText),
                transcoded = r.opt(1, Cell::asBool),
                fileUrl = r.opt(2, Cell::asText),
            )
        }
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

@Serializable
data class ScreenSpec(
    @Serializable(with = IDText::class) val root: String,
    val elements: List<ScreenElement>,
    val state: List<ScreenStateEntry>,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenSpec {
            val r = CellReader(cells)
            return ScreenSpec(
                root = r.req(0, Cell::asId),
                elements = r.arr(1) { ScreenElement.fromCells(it) },
                state = r.arr(2) { ScreenStateEntry.fromCells(it) },
            )
        }
    }
}

@Serializable(with = ScreenElementSerializer::class)
sealed class ScreenElement {
    data class Button(val value: ScreenButton) : ScreenElement()
    data class Column(val value: ScreenColumn) : ScreenElement()
    data class Heading(val value: ScreenHeading) : ScreenElement()
    data class Pressable(val value: ScreenPressable) : ScreenElement()
    data class Spacer(val value: ScreenSpacer) : ScreenElement()
    data class Switch(val value: ScreenSwitch) : ScreenElement()
    data class Unknown(val typename: String) : ScreenElement()

    val kind: ScreenElementKind?
        get() = when (this) {
            is Button -> ScreenElementKind.Button
            is Column -> ScreenElementKind.Column
            is Heading -> ScreenElementKind.Heading
            is Pressable -> ScreenElementKind.Pressable
            is Spacer -> ScreenElementKind.Spacer
            is Switch -> ScreenElementKind.Switch
            is Unknown -> null
        }

    val id: String?
        get() = when (this) {
            is Button -> value.id
            is Column -> value.id
            is Heading -> value.id
            is Pressable -> value.id
            is Spacer -> value.id
            is Switch -> value.id
            is Unknown -> null
        }

    val children: List<String>?
        get() = when (this) {
            is Button -> value.children
            is Column -> value.children
            is Heading -> value.children
            is Pressable -> value.children
            is Spacer -> value.children
            is Switch -> value.children
            is Unknown -> null
        }

    val press: ScreenAction?
        get() = when (this) {
            is Button -> value.press
            is Column -> value.press
            is Heading -> value.press
            is Pressable -> value.press
            is Spacer -> value.press
            is Switch -> value.press
            is Unknown -> null
        }

    val visible: ScreenVisibility?
        get() = when (this) {
            is Button -> value.visible
            is Column -> value.visible
            is Heading -> value.visible
            is Pressable -> value.visible
            is Spacer -> value.visible
            is Switch -> value.visible
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): ScreenElement {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "ScreenButton" -> Button(r.obj(1) { ScreenButton.fromCells(it) })
                "ScreenColumn" -> Column(r.obj(1) { ScreenColumn.fromCells(it) })
                "ScreenHeading" -> Heading(r.obj(1) { ScreenHeading.fromCells(it) })
                "ScreenPressable" -> Pressable(r.obj(1) { ScreenPressable.fromCells(it) })
                "ScreenSpacer" -> Spacer(r.obj(1) { ScreenSpacer.fromCells(it) })
                "ScreenSwitch" -> Switch(r.obj(1) { ScreenSwitch.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object ScreenElementSerializer : UnionSerializer<ScreenElement>("ScreenElement") {
    override fun read(typename: String, body: JsonObject): ScreenElement = when (typename) {
        "ScreenButton" -> ScreenElement.Button(dbJson.decodeFromJsonElement(ScreenButton.serializer(), body))
        "ScreenColumn" -> ScreenElement.Column(dbJson.decodeFromJsonElement(ScreenColumn.serializer(), body))
        "ScreenHeading" -> ScreenElement.Heading(dbJson.decodeFromJsonElement(ScreenHeading.serializer(), body))
        "ScreenPressable" -> ScreenElement.Pressable(dbJson.decodeFromJsonElement(ScreenPressable.serializer(), body))
        "ScreenSpacer" -> ScreenElement.Spacer(dbJson.decodeFromJsonElement(ScreenSpacer.serializer(), body))
        "ScreenSwitch" -> ScreenElement.Switch(dbJson.decodeFromJsonElement(ScreenSwitch.serializer(), body))
        else -> ScreenElement.Unknown(typename)
    }

    override fun write(value: ScreenElement): Pair<String, JsonElement> = when (value) {
        is ScreenElement.Button -> "ScreenButton" to dbJson.encodeToJsonElement(ScreenButton.serializer(), value.value)
        is ScreenElement.Column -> "ScreenColumn" to dbJson.encodeToJsonElement(ScreenColumn.serializer(), value.value)
        is ScreenElement.Heading -> "ScreenHeading" to dbJson.encodeToJsonElement(ScreenHeading.serializer(), value.value)
        is ScreenElement.Pressable -> "ScreenPressable" to dbJson.encodeToJsonElement(ScreenPressable.serializer(), value.value)
        is ScreenElement.Spacer -> "ScreenSpacer" to dbJson.encodeToJsonElement(ScreenSpacer.serializer(), value.value)
        is ScreenElement.Switch -> "ScreenSwitch" to dbJson.encodeToJsonElement(ScreenSwitch.serializer(), value.value)
        is ScreenElement.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class ScreenElementKind {
    @SerialName("ScreenButton") Button,
    @SerialName("ScreenColumn") Column,
    @SerialName("ScreenHeading") Heading,
    @SerialName("ScreenPressable") Pressable,
    @SerialName("ScreenSpacer") Spacer,
    @SerialName("ScreenSwitch") Switch,
}

@Serializable
data class ScreenButton(
    @Serializable(with = IDText::class) val id: String,
    @Serializable(with = IDTexts::class) val children: List<String>,
    val press: ScreenAction? = null,
    val visible: ScreenVisibility? = null,
    val label: ScreenTextValue? = null,
    val disabled: ScreenFlagValue? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenButton {
            val r = CellReader(cells)
            return ScreenButton(
                id = r.req(0, Cell::asId),
                children = r.list(1, Cell::asId),
                press = r.objOpt(2) { ScreenAction.fromCells(it) },
                visible = r.objOpt(3) { ScreenVisibility.fromCells(it) },
                label = r.objOpt(4) { ScreenTextValue.fromCells(it) },
                disabled = r.objOpt(5) { ScreenFlagValue.fromCells(it) },
            )
        }
    }
}

@Serializable(with = ScreenActionSerializer::class)
sealed class ScreenAction {
    data class Navigate(val value: ScreenNavigateAction) : ScreenAction()
    data class OpenChat(val value: ScreenOpenChatAction) : ScreenAction()
    data class OpenExternal(val value: ScreenOpenExternalAction) : ScreenAction()
    data class OpenProfile(val value: ScreenOpenProfileAction) : ScreenAction()
    data class Unknown(val typename: String) : ScreenAction()

    val kind: ScreenActionKind?
        get() = when (this) {
            is Navigate -> ScreenActionKind.Navigate
            is OpenChat -> ScreenActionKind.OpenChat
            is OpenExternal -> ScreenActionKind.OpenExternal
            is OpenProfile -> ScreenActionKind.OpenProfile
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): ScreenAction {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "ScreenNavigateAction" -> Navigate(r.obj(1) { ScreenNavigateAction.fromCells(it) })
                "ScreenOpenChatAction" -> OpenChat(r.obj(1) { ScreenOpenChatAction.fromCells(it) })
                "ScreenOpenExternalAction" -> OpenExternal(r.obj(1) { ScreenOpenExternalAction.fromCells(it) })
                "ScreenOpenProfileAction" -> OpenProfile(r.obj(1) { ScreenOpenProfileAction.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object ScreenActionSerializer : UnionSerializer<ScreenAction>("ScreenAction") {
    override fun read(typename: String, body: JsonObject): ScreenAction = when (typename) {
        "ScreenNavigateAction" -> ScreenAction.Navigate(dbJson.decodeFromJsonElement(ScreenNavigateAction.serializer(), body))
        "ScreenOpenChatAction" -> ScreenAction.OpenChat(dbJson.decodeFromJsonElement(ScreenOpenChatAction.serializer(), body))
        "ScreenOpenExternalAction" -> ScreenAction.OpenExternal(dbJson.decodeFromJsonElement(ScreenOpenExternalAction.serializer(), body))
        "ScreenOpenProfileAction" -> ScreenAction.OpenProfile(dbJson.decodeFromJsonElement(ScreenOpenProfileAction.serializer(), body))
        else -> ScreenAction.Unknown(typename)
    }

    override fun write(value: ScreenAction): Pair<String, JsonElement> = when (value) {
        is ScreenAction.Navigate -> "ScreenNavigateAction" to dbJson.encodeToJsonElement(ScreenNavigateAction.serializer(), value.value)
        is ScreenAction.OpenChat -> "ScreenOpenChatAction" to dbJson.encodeToJsonElement(ScreenOpenChatAction.serializer(), value.value)
        is ScreenAction.OpenExternal -> "ScreenOpenExternalAction" to dbJson.encodeToJsonElement(ScreenOpenExternalAction.serializer(), value.value)
        is ScreenAction.OpenProfile -> "ScreenOpenProfileAction" to dbJson.encodeToJsonElement(ScreenOpenProfileAction.serializer(), value.value)
        is ScreenAction.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class ScreenActionKind {
    @SerialName("ScreenNavigateAction") Navigate,
    @SerialName("ScreenOpenChatAction") OpenChat,
    @SerialName("ScreenOpenExternalAction") OpenExternal,
    @SerialName("ScreenOpenProfileAction") OpenProfile,
}

@Serializable
data class ScreenNavigateAction(
    val slug: String,
    val title: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenNavigateAction {
            val r = CellReader(cells)
            return ScreenNavigateAction(
                slug = r.req(0, Cell::asText),
                title = r.opt(1, Cell::asText),
            )
        }
    }
}

@Serializable
data class ScreenOpenChatAction(
    val chatId: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenOpenChatAction {
            val r = CellReader(cells)
            return ScreenOpenChatAction(
                chatId = r.req(0, Cell::asText),
            )
        }
    }
}

@Serializable
data class ScreenOpenExternalAction(
    val url: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenOpenExternalAction {
            val r = CellReader(cells)
            return ScreenOpenExternalAction(
                url = r.req(0, Cell::asText),
            )
        }
    }
}

@Serializable
data class ScreenOpenProfileAction(
    val userId: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenOpenProfileAction {
            val r = CellReader(cells)
            return ScreenOpenProfileAction(
                userId = r.req(0, Cell::asText),
            )
        }
    }
}

@Serializable(with = ScreenVisibilitySerializer::class)
sealed class ScreenVisibility {
    data class Condition(val value: ScreenCondition) : ScreenVisibility()
    data class VisibleAll(val value: ScreenVisibleAll) : ScreenVisibility()
    data class VisibleAny(val value: ScreenVisibleAny) : ScreenVisibility()
    data class VisibleFlag(val value: ScreenVisibleFlag) : ScreenVisibility()
    data class Unknown(val typename: String) : ScreenVisibility()

    val kind: ScreenVisibilityKind?
        get() = when (this) {
            is Condition -> ScreenVisibilityKind.Condition
            is VisibleAll -> ScreenVisibilityKind.VisibleAll
            is VisibleAny -> ScreenVisibilityKind.VisibleAny
            is VisibleFlag -> ScreenVisibilityKind.VisibleFlag
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): ScreenVisibility {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "ScreenCondition" -> Condition(r.obj(1) { ScreenCondition.fromCells(it) })
                "ScreenVisibleAll" -> VisibleAll(r.obj(1) { ScreenVisibleAll.fromCells(it) })
                "ScreenVisibleAny" -> VisibleAny(r.obj(1) { ScreenVisibleAny.fromCells(it) })
                "ScreenVisibleFlag" -> VisibleFlag(r.obj(1) { ScreenVisibleFlag.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object ScreenVisibilitySerializer : UnionSerializer<ScreenVisibility>("ScreenVisibility") {
    override fun read(typename: String, body: JsonObject): ScreenVisibility = when (typename) {
        "ScreenCondition" -> ScreenVisibility.Condition(dbJson.decodeFromJsonElement(ScreenCondition.serializer(), body))
        "ScreenVisibleAll" -> ScreenVisibility.VisibleAll(dbJson.decodeFromJsonElement(ScreenVisibleAll.serializer(), body))
        "ScreenVisibleAny" -> ScreenVisibility.VisibleAny(dbJson.decodeFromJsonElement(ScreenVisibleAny.serializer(), body))
        "ScreenVisibleFlag" -> ScreenVisibility.VisibleFlag(dbJson.decodeFromJsonElement(ScreenVisibleFlag.serializer(), body))
        else -> ScreenVisibility.Unknown(typename)
    }

    override fun write(value: ScreenVisibility): Pair<String, JsonElement> = when (value) {
        is ScreenVisibility.Condition -> "ScreenCondition" to dbJson.encodeToJsonElement(ScreenCondition.serializer(), value.value)
        is ScreenVisibility.VisibleAll -> "ScreenVisibleAll" to dbJson.encodeToJsonElement(ScreenVisibleAll.serializer(), value.value)
        is ScreenVisibility.VisibleAny -> "ScreenVisibleAny" to dbJson.encodeToJsonElement(ScreenVisibleAny.serializer(), value.value)
        is ScreenVisibility.VisibleFlag -> "ScreenVisibleFlag" to dbJson.encodeToJsonElement(ScreenVisibleFlag.serializer(), value.value)
        is ScreenVisibility.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class ScreenVisibilityKind {
    @SerialName("ScreenCondition") Condition,
    @SerialName("ScreenVisibleAll") VisibleAll,
    @SerialName("ScreenVisibleAny") VisibleAny,
    @SerialName("ScreenVisibleFlag") VisibleFlag,
}

@Serializable
data class ScreenCondition(
    val comparison: String,
    val negated: Boolean,
    val operand: ScreenOperand? = null,
    val path: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenCondition {
            val r = CellReader(cells)
            return ScreenCondition(
                comparison = r.req(0, Cell::asText),
                negated = r.req(1, Cell::asBool),
                operand = r.objOpt(2) { ScreenOperand.fromCells(it) },
                path = r.req(3, Cell::asText),
            )
        }
    }
}

@Serializable(with = ScreenOperandSerializer::class)
sealed class ScreenOperand {
    data class FlagLiteral(val value: ScreenFlagLiteral) : ScreenOperand()
    data class NumberLiteral(val value: ScreenNumberLiteral) : ScreenOperand()
    data class StateRef(val value: ScreenStateRef) : ScreenOperand()
    data class TextLiteral(val value: ScreenTextLiteral) : ScreenOperand()
    data class Unknown(val typename: String) : ScreenOperand()

    val kind: ScreenOperandKind?
        get() = when (this) {
            is FlagLiteral -> ScreenOperandKind.FlagLiteral
            is NumberLiteral -> ScreenOperandKind.NumberLiteral
            is StateRef -> ScreenOperandKind.StateRef
            is TextLiteral -> ScreenOperandKind.TextLiteral
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): ScreenOperand {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "ScreenFlagLiteral" -> FlagLiteral(r.obj(1) { ScreenFlagLiteral.fromCells(it) })
                "ScreenNumberLiteral" -> NumberLiteral(r.obj(1) { ScreenNumberLiteral.fromCells(it) })
                "ScreenStateRef" -> StateRef(r.obj(1) { ScreenStateRef.fromCells(it) })
                "ScreenTextLiteral" -> TextLiteral(r.obj(1) { ScreenTextLiteral.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object ScreenOperandSerializer : UnionSerializer<ScreenOperand>("ScreenOperand") {
    override fun read(typename: String, body: JsonObject): ScreenOperand = when (typename) {
        "ScreenFlagLiteral" -> ScreenOperand.FlagLiteral(dbJson.decodeFromJsonElement(ScreenFlagLiteral.serializer(), body))
        "ScreenNumberLiteral" -> ScreenOperand.NumberLiteral(dbJson.decodeFromJsonElement(ScreenNumberLiteral.serializer(), body))
        "ScreenStateRef" -> ScreenOperand.StateRef(dbJson.decodeFromJsonElement(ScreenStateRef.serializer(), body))
        "ScreenTextLiteral" -> ScreenOperand.TextLiteral(dbJson.decodeFromJsonElement(ScreenTextLiteral.serializer(), body))
        else -> ScreenOperand.Unknown(typename)
    }

    override fun write(value: ScreenOperand): Pair<String, JsonElement> = when (value) {
        is ScreenOperand.FlagLiteral -> "ScreenFlagLiteral" to dbJson.encodeToJsonElement(ScreenFlagLiteral.serializer(), value.value)
        is ScreenOperand.NumberLiteral -> "ScreenNumberLiteral" to dbJson.encodeToJsonElement(ScreenNumberLiteral.serializer(), value.value)
        is ScreenOperand.StateRef -> "ScreenStateRef" to dbJson.encodeToJsonElement(ScreenStateRef.serializer(), value.value)
        is ScreenOperand.TextLiteral -> "ScreenTextLiteral" to dbJson.encodeToJsonElement(ScreenTextLiteral.serializer(), value.value)
        is ScreenOperand.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class ScreenOperandKind {
    @SerialName("ScreenFlagLiteral") FlagLiteral,
    @SerialName("ScreenNumberLiteral") NumberLiteral,
    @SerialName("ScreenStateRef") StateRef,
    @SerialName("ScreenTextLiteral") TextLiteral,
}

@Serializable
data class ScreenFlagLiteral(
    val flag: Boolean,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenFlagLiteral {
            val r = CellReader(cells)
            return ScreenFlagLiteral(
                flag = r.req(0, Cell::asBool),
            )
        }
    }
}

@Serializable
data class ScreenNumberLiteral(
    val number: Double,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenNumberLiteral {
            val r = CellReader(cells)
            return ScreenNumberLiteral(
                number = r.req(0, Cell::asDouble),
            )
        }
    }
}

@Serializable
data class ScreenStateRef(
    val path: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenStateRef {
            val r = CellReader(cells)
            return ScreenStateRef(
                path = r.req(0, Cell::asText),
            )
        }
    }
}

@Serializable
data class ScreenTextLiteral(
    val text: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenTextLiteral {
            val r = CellReader(cells)
            return ScreenTextLiteral(
                text = r.req(0, Cell::asText),
            )
        }
    }
}

@Serializable
data class ScreenVisibleAll(
    val conditions: List<ScreenCondition>,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenVisibleAll {
            val r = CellReader(cells)
            return ScreenVisibleAll(
                conditions = r.arr(0) { ScreenCondition.fromCells(it) },
            )
        }
    }
}

@Serializable
data class ScreenVisibleAny(
    val conditions: List<ScreenCondition>,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenVisibleAny {
            val r = CellReader(cells)
            return ScreenVisibleAny(
                conditions = r.arr(0) { ScreenCondition.fromCells(it) },
            )
        }
    }
}

@Serializable
data class ScreenVisibleFlag(
    val flag: Boolean,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenVisibleFlag {
            val r = CellReader(cells)
            return ScreenVisibleFlag(
                flag = r.req(0, Cell::asBool),
            )
        }
    }
}

@Serializable(with = ScreenTextValueSerializer::class)
sealed class ScreenTextValue {
    data class StateRef(val value: ScreenStateRef) : ScreenTextValue()
    data class TextLiteral(val value: ScreenTextLiteral) : ScreenTextValue()
    data class Unknown(val typename: String) : ScreenTextValue()

    val kind: ScreenTextValueKind?
        get() = when (this) {
            is StateRef -> ScreenTextValueKind.StateRef
            is TextLiteral -> ScreenTextValueKind.TextLiteral
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): ScreenTextValue {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "ScreenStateRef" -> StateRef(r.obj(1) { ScreenStateRef.fromCells(it) })
                "ScreenTextLiteral" -> TextLiteral(r.obj(1) { ScreenTextLiteral.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object ScreenTextValueSerializer : UnionSerializer<ScreenTextValue>("ScreenTextValue") {
    override fun read(typename: String, body: JsonObject): ScreenTextValue = when (typename) {
        "ScreenStateRef" -> ScreenTextValue.StateRef(dbJson.decodeFromJsonElement(ScreenStateRef.serializer(), body))
        "ScreenTextLiteral" -> ScreenTextValue.TextLiteral(dbJson.decodeFromJsonElement(ScreenTextLiteral.serializer(), body))
        else -> ScreenTextValue.Unknown(typename)
    }

    override fun write(value: ScreenTextValue): Pair<String, JsonElement> = when (value) {
        is ScreenTextValue.StateRef -> "ScreenStateRef" to dbJson.encodeToJsonElement(ScreenStateRef.serializer(), value.value)
        is ScreenTextValue.TextLiteral -> "ScreenTextLiteral" to dbJson.encodeToJsonElement(ScreenTextLiteral.serializer(), value.value)
        is ScreenTextValue.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class ScreenTextValueKind {
    @SerialName("ScreenStateRef") StateRef,
    @SerialName("ScreenTextLiteral") TextLiteral,
}

@Serializable(with = ScreenFlagValueSerializer::class)
sealed class ScreenFlagValue {
    data class FlagLiteral(val value: ScreenFlagLiteral) : ScreenFlagValue()
    data class StateRef(val value: ScreenStateRef) : ScreenFlagValue()
    data class Unknown(val typename: String) : ScreenFlagValue()

    val kind: ScreenFlagValueKind?
        get() = when (this) {
            is FlagLiteral -> ScreenFlagValueKind.FlagLiteral
            is StateRef -> ScreenFlagValueKind.StateRef
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): ScreenFlagValue {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "ScreenFlagLiteral" -> FlagLiteral(r.obj(1) { ScreenFlagLiteral.fromCells(it) })
                "ScreenStateRef" -> StateRef(r.obj(1) { ScreenStateRef.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object ScreenFlagValueSerializer : UnionSerializer<ScreenFlagValue>("ScreenFlagValue") {
    override fun read(typename: String, body: JsonObject): ScreenFlagValue = when (typename) {
        "ScreenFlagLiteral" -> ScreenFlagValue.FlagLiteral(dbJson.decodeFromJsonElement(ScreenFlagLiteral.serializer(), body))
        "ScreenStateRef" -> ScreenFlagValue.StateRef(dbJson.decodeFromJsonElement(ScreenStateRef.serializer(), body))
        else -> ScreenFlagValue.Unknown(typename)
    }

    override fun write(value: ScreenFlagValue): Pair<String, JsonElement> = when (value) {
        is ScreenFlagValue.FlagLiteral -> "ScreenFlagLiteral" to dbJson.encodeToJsonElement(ScreenFlagLiteral.serializer(), value.value)
        is ScreenFlagValue.StateRef -> "ScreenStateRef" to dbJson.encodeToJsonElement(ScreenStateRef.serializer(), value.value)
        is ScreenFlagValue.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class ScreenFlagValueKind {
    @SerialName("ScreenFlagLiteral") FlagLiteral,
    @SerialName("ScreenStateRef") StateRef,
}

@Serializable
data class ScreenColumn(
    @Serializable(with = IDText::class) val id: String,
    @Serializable(with = IDTexts::class) val children: List<String>,
    val press: ScreenAction? = null,
    val visible: ScreenVisibility? = null,
    val gap: ScreenNumberValue? = null,
    val alignItems: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenColumn {
            val r = CellReader(cells)
            return ScreenColumn(
                id = r.req(0, Cell::asId),
                children = r.list(1, Cell::asId),
                press = r.objOpt(2) { ScreenAction.fromCells(it) },
                visible = r.objOpt(3) { ScreenVisibility.fromCells(it) },
                gap = r.objOpt(4) { ScreenNumberValue.fromCells(it) },
                alignItems = r.opt(5, Cell::asText),
            )
        }
    }
}

@Serializable(with = ScreenNumberValueSerializer::class)
sealed class ScreenNumberValue {
    data class NumberLiteral(val value: ScreenNumberLiteral) : ScreenNumberValue()
    data class StateRef(val value: ScreenStateRef) : ScreenNumberValue()
    data class Unknown(val typename: String) : ScreenNumberValue()

    val kind: ScreenNumberValueKind?
        get() = when (this) {
            is NumberLiteral -> ScreenNumberValueKind.NumberLiteral
            is StateRef -> ScreenNumberValueKind.StateRef
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): ScreenNumberValue {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "ScreenNumberLiteral" -> NumberLiteral(r.obj(1) { ScreenNumberLiteral.fromCells(it) })
                "ScreenStateRef" -> StateRef(r.obj(1) { ScreenStateRef.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object ScreenNumberValueSerializer : UnionSerializer<ScreenNumberValue>("ScreenNumberValue") {
    override fun read(typename: String, body: JsonObject): ScreenNumberValue = when (typename) {
        "ScreenNumberLiteral" -> ScreenNumberValue.NumberLiteral(dbJson.decodeFromJsonElement(ScreenNumberLiteral.serializer(), body))
        "ScreenStateRef" -> ScreenNumberValue.StateRef(dbJson.decodeFromJsonElement(ScreenStateRef.serializer(), body))
        else -> ScreenNumberValue.Unknown(typename)
    }

    override fun write(value: ScreenNumberValue): Pair<String, JsonElement> = when (value) {
        is ScreenNumberValue.NumberLiteral -> "ScreenNumberLiteral" to dbJson.encodeToJsonElement(ScreenNumberLiteral.serializer(), value.value)
        is ScreenNumberValue.StateRef -> "ScreenStateRef" to dbJson.encodeToJsonElement(ScreenStateRef.serializer(), value.value)
        is ScreenNumberValue.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class ScreenNumberValueKind {
    @SerialName("ScreenNumberLiteral") NumberLiteral,
    @SerialName("ScreenStateRef") StateRef,
}

@Serializable
data class ScreenHeading(
    @Serializable(with = IDText::class) val id: String,
    @Serializable(with = IDTexts::class) val children: List<String>,
    val press: ScreenAction? = null,
    val visible: ScreenVisibility? = null,
    val text: ScreenTextValue? = null,
    val level: Long? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenHeading {
            val r = CellReader(cells)
            return ScreenHeading(
                id = r.req(0, Cell::asId),
                children = r.list(1, Cell::asId),
                press = r.objOpt(2) { ScreenAction.fromCells(it) },
                visible = r.objOpt(3) { ScreenVisibility.fromCells(it) },
                text = r.objOpt(4) { ScreenTextValue.fromCells(it) },
                level = r.opt(5, Cell::asInt),
            )
        }
    }
}

@Serializable
data class ScreenPressable(
    @Serializable(with = IDText::class) val id: String,
    @Serializable(with = IDTexts::class) val children: List<String>,
    val press: ScreenAction? = null,
    val visible: ScreenVisibility? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenPressable {
            val r = CellReader(cells)
            return ScreenPressable(
                id = r.req(0, Cell::asId),
                children = r.list(1, Cell::asId),
                press = r.objOpt(2) { ScreenAction.fromCells(it) },
                visible = r.objOpt(3) { ScreenVisibility.fromCells(it) },
            )
        }
    }
}

@Serializable
data class ScreenSpacer(
    @Serializable(with = IDText::class) val id: String,
    @Serializable(with = IDTexts::class) val children: List<String>,
    val press: ScreenAction? = null,
    val visible: ScreenVisibility? = null,
    val length: ScreenNumberValue? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenSpacer {
            val r = CellReader(cells)
            return ScreenSpacer(
                id = r.req(0, Cell::asId),
                children = r.list(1, Cell::asId),
                press = r.objOpt(2) { ScreenAction.fromCells(it) },
                visible = r.objOpt(3) { ScreenVisibility.fromCells(it) },
                length = r.objOpt(4) { ScreenNumberValue.fromCells(it) },
            )
        }
    }
}

@Serializable
data class ScreenSwitch(
    @Serializable(with = IDText::class) val id: String,
    @Serializable(with = IDTexts::class) val children: List<String>,
    val press: ScreenAction? = null,
    val visible: ScreenVisibility? = null,
    val checked: ScreenFlagValue? = null,
    val label: ScreenTextValue? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenSwitch {
            val r = CellReader(cells)
            return ScreenSwitch(
                id = r.req(0, Cell::asId),
                children = r.list(1, Cell::asId),
                press = r.objOpt(2) { ScreenAction.fromCells(it) },
                visible = r.objOpt(3) { ScreenVisibility.fromCells(it) },
                checked = r.objOpt(4) { ScreenFlagValue.fromCells(it) },
                label = r.objOpt(5) { ScreenTextValue.fromCells(it) },
            )
        }
    }
}

@Serializable
data class ScreenStateEntry(
    val path: String,
    val value: ScreenStateValue,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenStateEntry {
            val r = CellReader(cells)
            return ScreenStateEntry(
                path = r.req(0, Cell::asText),
                value = r.obj(1) { ScreenStateValue.fromCells(it) },
            )
        }
    }
}

@Serializable(with = ScreenStateValueSerializer::class)
sealed class ScreenStateValue {
    data class Flag(val value: ScreenFlagLiteral) : ScreenStateValue()
    data class Number(val value: ScreenNumberLiteral) : ScreenStateValue()
    data class Text(val value: ScreenTextLiteral) : ScreenStateValue()
    data class Unknown(val typename: String) : ScreenStateValue()

    val kind: ScreenStateValueKind?
        get() = when (this) {
            is Flag -> ScreenStateValueKind.Flag
            is Number -> ScreenStateValueKind.Number
            is Text -> ScreenStateValueKind.Text
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): ScreenStateValue {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "ScreenFlagLiteral" -> Flag(r.obj(1) { ScreenFlagLiteral.fromCells(it) })
                "ScreenNumberLiteral" -> Number(r.obj(1) { ScreenNumberLiteral.fromCells(it) })
                "ScreenTextLiteral" -> Text(r.obj(1) { ScreenTextLiteral.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object ScreenStateValueSerializer : UnionSerializer<ScreenStateValue>("ScreenStateValue") {
    override fun read(typename: String, body: JsonObject): ScreenStateValue = when (typename) {
        "ScreenFlagLiteral" -> ScreenStateValue.Flag(dbJson.decodeFromJsonElement(ScreenFlagLiteral.serializer(), body))
        "ScreenNumberLiteral" -> ScreenStateValue.Number(dbJson.decodeFromJsonElement(ScreenNumberLiteral.serializer(), body))
        "ScreenTextLiteral" -> ScreenStateValue.Text(dbJson.decodeFromJsonElement(ScreenTextLiteral.serializer(), body))
        else -> ScreenStateValue.Unknown(typename)
    }

    override fun write(value: ScreenStateValue): Pair<String, JsonElement> = when (value) {
        is ScreenStateValue.Flag -> "ScreenFlagLiteral" to dbJson.encodeToJsonElement(ScreenFlagLiteral.serializer(), value.value)
        is ScreenStateValue.Number -> "ScreenNumberLiteral" to dbJson.encodeToJsonElement(ScreenNumberLiteral.serializer(), value.value)
        is ScreenStateValue.Text -> "ScreenTextLiteral" to dbJson.encodeToJsonElement(ScreenTextLiteral.serializer(), value.value)
        is ScreenStateValue.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class ScreenStateValueKind {
    @SerialName("ScreenFlagLiteral") Flag,
    @SerialName("ScreenNumberLiteral") Number,
    @SerialName("ScreenTextLiteral") Text,
}

@Serializable(with = MomentWidgetItemPropsSerializer::class)
sealed class MomentWidgetItemProps {
    data class Emoji(val value: MomentEmojiProps) : MomentWidgetItemProps()
    data class Label(val value: MomentLabelProps) : MomentWidgetItemProps()
    data class Unknown(val typename: String) : MomentWidgetItemProps()

    val kind: MomentWidgetItemPropsKind?
        get() = when (this) {
            is Emoji -> MomentWidgetItemPropsKind.Emoji
            is Label -> MomentWidgetItemPropsKind.Label
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): MomentWidgetItemProps {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "MomentEmojiProps" -> Emoji(r.obj(1) { MomentEmojiProps.fromCells(it) })
                "MomentLabelProps" -> Label(r.obj(1) { MomentLabelProps.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object MomentWidgetItemPropsSerializer : UnionSerializer<MomentWidgetItemProps>("MomentWidgetItemProps") {
    override fun read(typename: String, body: JsonObject): MomentWidgetItemProps = when (typename) {
        "MomentEmojiProps" -> MomentWidgetItemProps.Emoji(dbJson.decodeFromJsonElement(MomentEmojiProps.serializer(), body))
        "MomentLabelProps" -> MomentWidgetItemProps.Label(dbJson.decodeFromJsonElement(MomentLabelProps.serializer(), body))
        else -> MomentWidgetItemProps.Unknown(typename)
    }

    override fun write(value: MomentWidgetItemProps): Pair<String, JsonElement> = when (value) {
        is MomentWidgetItemProps.Emoji -> "MomentEmojiProps" to dbJson.encodeToJsonElement(MomentEmojiProps.serializer(), value.value)
        is MomentWidgetItemProps.Label -> "MomentLabelProps" to dbJson.encodeToJsonElement(MomentLabelProps.serializer(), value.value)
        is MomentWidgetItemProps.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class MomentWidgetItemPropsKind {
    @SerialName("MomentEmojiProps") Emoji,
    @SerialName("MomentLabelProps") Label,
}

@Serializable
data class MomentEmojiProps(
    val emoji: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): MomentEmojiProps {
            val r = CellReader(cells)
            return MomentEmojiProps(
                emoji = r.req(0, Cell::asText),
            )
        }
    }
}

@Serializable
data class MomentLabelProps(
    val text: String,
    val size: Long? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): MomentLabelProps {
            val r = CellReader(cells)
            return MomentLabelProps(
                text = r.req(0, Cell::asText),
                size = r.opt(1, Cell::asInt),
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

val modelsJSON = """[{"key":"Chat","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastMessageAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastMessageId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"shareUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"isPublic","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"messages","kind":"has_many","target":"Message","foreign_key":"chatId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null},{"name":"lastMessage","kind":"belongs_to","target":"Message","foreign_key":"lastMessageId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ChatNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[],"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"nodes","by_columns":[],"text_params":[],"remote":{"operation":"ChatsByIds","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"node":"many"},{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"lastMessageAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateChat","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Message","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"authorId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"sequenceNumber","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"attachment","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageAttachment","merge":null,"fields":[{"name":"gift","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"GiftAttachment","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tier","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"giftTier","app_type":null,"merge":null},{"name":"status","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tags","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"moment","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentAttachment","merge":null,"fields":[{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"caption","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"profile","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ProfileAttachment","merge":null,"fields":[{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"AttachmentUser","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"reaction","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ReactionAttachment","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageMedia","merge":{"type":"keys","keys":{"fileUrl":{"type":"non_empty"}}},"fields":[{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"transcoded","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}],"associations":[{"name":"chat","kind":"belongs_to","target":"Chat","foreign_key":"chatId","dependent":null,"touches_parent":false,"sets":{"lastMessageAt":"createdAt","lastMessageId":"id"},"newer":[{"child":"createdAt","parent":"lastMessageAt"}],"from":null}],"relations":[{"name":"by_chat","by_columns":["chatId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"asc"}]},"joins_events":false,"joins_created":false},{"name":"thread","by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatThread","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[],"page_size":30,"variables":{},"root":"chat.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc_nulls_last"},{"column":"createdAt","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"transcode_status","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"MessageTranscodeStatus","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[],"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"chat_media","by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[],"page_size":30,"variables":{},"root":"node.messages"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"history","by_columns":[],"text_params":[],"remote":{"operation":"ChatMessages","shape":{"type":"connection","direction":"backward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"1.minute","empty_stale_time":"10.seconds","selects_page_info":true,"updates":[],"page_size":30,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateMessage","variables":{"attachedUserId":"attachedUserId","chatId":"chatId","signedId":"signedId","text":"text"},"optimistic":{},"refetch":[],"args":{"attachedUserId":"id?","signedId":"string?"},"local":["authorId"],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"uuid","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"searchable":true},{"name":"likedByMe","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"media","kind":{"type":"object"},"nullable":false,"optional":false,"default":{},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tone","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":{"type":"local"}},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"author","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":true,"sets":{},"newer":[],"from":null},{"name":"vibes","kind":"has_many","target":"Vibe","foreign_key":"momentId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"transcode_status","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"MomentTranscodeStatus","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[],"page_size":null,"variables":{"momentId":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"by_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"feed","by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[{"subscription":"MomentEvents","deleted_by":"destroyed","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"impressionUuid","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"impressionScore","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"vibe_feed","by_columns":[],"text_params":["vibeId"],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[],"page_size":20,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"impressionUuid","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}],"indexes":[["uuid"],["userId","createdAt"]],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateMoment","variables":{"media":"media","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"update","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"UpdateMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"destroy","root_shape":"destroy","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"DeleteMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"like","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"LikeMoment","variables":{"momentId":"id"},"optimistic":{"likedByMe":true},"refetch":["feed"],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"share","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"ShareMoment","variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"shareMoment.moment","lands":[],"result":{"copies":"int","successMessage":"string"}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"NestedSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"phase","kind":{"type":"enum","values":["draft","live"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedSamplePhase","merge":null},{"name":"value","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null,"fields":[{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"note","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"nullableDefault","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":"default","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ownerId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"owner_id","app_type":null,"merge":null},{"name":"count","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":2,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratio","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":1.5,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"enabled","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"state","kind":{"type":"enum","values":["ready","done"]},"nullable":false,"optional":false,"default":"ready","indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueState","merge":null},{"name":"labels","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"counts","kind":{"type":"list","element":"int"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratios","kind":{"type":"list","element":"num"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"flags","kind":{"type":"list","element":"bool"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"dates","kind":{"type":"list","element":"date"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"owners","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"payload","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"rawValue","kind":{"type":"raw"},"nullable":false,"optional":false,"default":{"free":[1,true,null]},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"preferences","kind":{"type":"object"},"nullable":false,"optional":false,"default":{"label":"base"},"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null,"fields":[{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":true,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"presets","kind":{"type":"array"},"nullable":false,"optional":false,"default":[{"label":"first"}],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null},{"name":"metadata","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"child","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"horizontal","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":"x","app_type":null,"merge":null}]},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null}]},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null},{"name":"tree","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null,"fields":[{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null}]},{"name":"replyTo","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageReplyNode","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageUserRef","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fullName","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"username","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatarUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedMedia","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":"","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"NestedReference","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"Presence","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"room","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"user","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"for_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"online","by_columns":[],"text_params":[],"remote":{"operation":"PresenceOnline","shape":{"type":"list","coverage":"complete"},"stale_time":"30.seconds","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"PresenceChanged","deleted_by":"left","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"CustomScreen","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"spec","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenSpec","merge":null,"fields":[{"name":"root","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"elements","kind":{"type":"union_array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenElement","merge":null,"fields":[{"name":"button","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenButton","merge":null,"fields":[{"name":"label","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenTextValue","merge":null,"fields":[{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateRef","merge":null},{"name":"textLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenTextLiteral","merge":null}]},{"name":"disabled","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenFlagValue","merge":null,"fields":[{"name":"flagLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenFlagLiteral","merge":null},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateRef","merge":null}]}]},{"name":"column","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenColumn","merge":null,"fields":[{"name":"gap","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenNumberValue","merge":null,"fields":[{"name":"numberLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenNumberLiteral","merge":null},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateRef","merge":null}]},{"name":"alignItems","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"heading","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenHeading","merge":null,"fields":[{"name":"text","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenTextValue","merge":null},{"name":"level","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"pressable","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenPressable","merge":null},{"name":"spacer","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenSpacer","merge":null,"fields":[{"name":"length","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenNumberValue","merge":null}]},{"name":"switch","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenSwitch","merge":null,"fields":[{"name":"checked","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenFlagValue","merge":null},{"name":"label","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenTextValue","merge":null}]}],"shared":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"press","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenAction","merge":null,"fields":[{"name":"navigate","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenNavigateAction","merge":null,"fields":[{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openChat","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenOpenChatAction","merge":null,"fields":[{"name":"chatId","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openExternal","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenOpenExternalAction","merge":null,"fields":[{"name":"url","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openProfile","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenOpenProfileAction","merge":null,"fields":[{"name":"userId","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"visible","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenVisibility","merge":null,"fields":[{"name":"condition","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenCondition","merge":null,"fields":[{"name":"comparison","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"negated","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"operand","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenOperand","merge":null,"fields":[{"name":"flagLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenFlagLiteral","merge":null,"fields":[{"name":"flag","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"numberLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenNumberLiteral","merge":null,"fields":[{"name":"number","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateRef","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"textLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenTextLiteral","merge":null,"fields":[{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"visibleAll","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenVisibleAll","merge":null,"fields":[{"name":"conditions","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenCondition","merge":null}]},{"name":"visibleAny","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenVisibleAny","merge":null,"fields":[{"name":"conditions","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenCondition","merge":null}]},{"name":"visibleFlag","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenVisibleFlag","merge":null,"fields":[{"name":"flag","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}]},{"name":"state","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateEntry","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"union"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateValue","merge":null,"fields":[{"name":"flag","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenFlagLiteral","merge":null},{"name":"number","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenNumberLiteral","merge":null},{"name":"text","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenTextLiteral","merge":null}]}]}]}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ScreenNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[],"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"StorageRow","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"record","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"UnionSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"attachment","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageAttachment","merge":null},{"name":"props","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentWidgetItemProps","merge":null,"fields":[{"name":"emoji","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentEmojiProps","merge":null,"fields":[{"name":"emoji","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"label","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentLabelProps","merge":null,"fields":[{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"size","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"User","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatar","kind":{"type":"file"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"birthday","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"profile","by_columns":[],"text_params":[],"remote":{"operation":"UserProfile","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"UserTyping","deleted_by":null,"set":{"typing":true},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"chat_members","by_columns":[],"text_params":["chatId"],"remote":{"operation":"ChatMembers","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[],"page_size":30,"variables":{},"root":"node.members"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[{"name":"typing","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":"5.seconds"},{"name":"mood","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":null}],"storage":"disk","expire_after":null,"listens":[]},{"key":"Vibe","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"enum","values":["like","love","laugh"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"VibeKind","merge":null}],"associations":[],"relations":[],"indexes":[["userId"]],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]}]"""

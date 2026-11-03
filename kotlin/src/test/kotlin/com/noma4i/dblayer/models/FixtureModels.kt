package com.noma4i.dblayer.models

import androidx.compose.runtime.Immutable
import com.noma4i.dblayer.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*

@Immutable
data class Asset(
    override val id: String,
    val clientKey: String,
    val size: Long,
    val position: Long,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Asset"
    companion object : ModelDecoder<Asset> {
        override val layout = "c944299d26851922d3b1dce7742ce0d81a21d4035bc8de5b7c487960dc71565b"
        override val modelName = "Asset"
        override fun decode(row: WireRow): Asset {
            val r = CellReader(row.cells)
            return Asset(
                id = row.id,
                clientKey = r.req(0, Cell::asText),
                size = r.req(1, Cell::asInt),
                position = r.req(2, Cell::asInt),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val clientKey = Column<Asset, String>("clientKey")
        val size = Column<Asset, Long>("size")
        val position = Column<Asset, Long>("position")
        fun where(condition: Condition) = Query("Asset", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Asset", this, ids)
        suspend fun create(clientKey: String, size: Long, position: Long? = null): Asset = offCaller {
            val values = buildJsonObject {
                put("clientKey", jsonValue(clientKey))
                put("size", jsonValue(size))
                position?.let { put("position", jsonValue(it)) }
            }
            read(DBlayer.database.create("Asset", values.jsonText(), "{}"))
        }
    suspend fun update(clientKey: String? = null, size: Long? = null, position: Long? = null): Asset = offCaller {
        val values = buildJsonObject {
            clientKey?.let { put("clientKey", jsonValue(it)) }
            size?.let { put("size", jsonValue(it)) }
            position?.let { put("position", jsonValue(it)) }
        }
        Asset.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
    suspend fun createFile(account: AccountScope, request: UploadRequest): ActionAnswer<Asset, AssetCreateFileResult> = account.upload(this, "create_file", request, AssetCreateFileResult.serializer())
}

@Immutable
@Serializable
class AssetCreateFileResult

@Immutable
data class Container(
    override val id: String,
    val title: String,
    val lastRecordAt: String? = null,
    val lastRecordId: String? = null,
    val shareUrl: String? = null,
    val isPublic: Boolean? = null,
    val shelves: List<ContainerShelf>? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Container"
    companion object : ModelDecoder<Container> {
        override val layout = "02f63dae390b644410b9064b71f41d994f1ae9f8b197e65a5899bac8ce7a1a39"
        override val modelName = "Container"
        override fun decode(row: WireRow): Container {
            val r = CellReader(row.cells)
            return Container(
                id = row.id,
                title = r.req(0, Cell::asText),
                lastRecordAt = r.opt(1, Cell::asText),
                lastRecordId = r.opt(2, Cell::asId),
                shareUrl = r.opt(3, Cell::asText),
                isPublic = r.opt(4, Cell::asBool),
                shelves = r.listOpt(5, Cell::asText)?.map { ContainerShelf.of(it) },
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val title = Column<Container, String>("title")
        val lastRecordAt = Column<Container, String?>("lastRecordAt")
        val lastRecordId = Column<Container, String?>("lastRecordId")
        val shareUrl = Column<Container, String?>("shareUrl")
        val isPublic = Column<Container, Boolean?>("isPublic")
        val shelves = Column<Container, List<ContainerShelf>?>("shelves")
        fun where(condition: Condition) = Query("Container", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Container", this, ids)
        suspend fun node(id: String, force: Boolean = false): Container? = offCaller { DBlayer.database.node("Container", "node", id, force)?.let { read(it) } }
        suspend fun nodes(ids: List<String>): List<Container> = offCaller { DBlayer.database.nodes("Container", "nodes", ids).map { read(it) } }
        val recent: Query<Container> get() {
            var built = Query("Container", this, relation = "recent", params = buildJsonObject {  })
            built = built.order(Order("lastRecordAt", "desc"))
            return built
        }
        fun shelf(shelf: ContainerShelf): Query<Container> {
            var built = Query("Container", this, relation = "shelf", params = buildJsonObject { put("shelf", jsonValue(shelf)) })
            return built
        }
        suspend fun send(account: AccountScope, title: String, lastRecordAt: String? = null, lastRecordId: String? = null, shareUrl: String? = null, isPublic: Boolean? = null, shelves: List<ContainerShelf>? = null): Sent<Container> {
            val values = buildJsonObject {
                put("title", jsonValue(title))
                lastRecordAt?.let { put("lastRecordAt", jsonValue(it)) }
                lastRecordId?.let { put("lastRecordId", jsonValue(it)) }
                shareUrl?.let { put("shareUrl", jsonValue(it)) }
                isPublic?.let { put("isPublic", jsonValue(it)) }
                shelves?.let { put("shelves", jsonValue(it)) }
            }
            return account.send(this, values)
        }
    suspend fun update(title: String? = null, lastRecordAt: GraphQLNullable<String> = GraphQLNullable.Omitted, lastRecordId: GraphQLNullable<String> = GraphQLNullable.Omitted, shareUrl: GraphQLNullable<String> = GraphQLNullable.Omitted, isPublic: GraphQLNullable<Boolean> = GraphQLNullable.Omitted, shelves: GraphQLNullable<List<ContainerShelf>> = GraphQLNullable.Omitted): Container = offCaller {
        val values = buildJsonObject {
            title?.let { put("title", jsonValue(it)) }
            put("lastRecordAt", lastRecordAt)
            put("lastRecordId", lastRecordId)
            put("shareUrl", shareUrl)
            put("isPublic", isPublic)
            put("shelves", shelves)
        }
        Container.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
    suspend fun createPhoto(account: AccountScope, request: UploadRequest): ActionAnswer<Container, ContainerCreatePhotoResult> = account.upload(this, "create_photo", request, ContainerCreatePhotoResult.serializer())
    suspend fun updatePhoto(account: AccountScope, request: UploadRequest): ActionAnswer<Container, ContainerUpdatePhotoResult> = account.upload(this, "update_photo", request, ContainerUpdatePhotoResult.serializer())
    object Relations {
        val records = Association<Container, Record>("records", Record)
        val lastRecord = Association<Container, Record>("lastRecord", Record)
    }
}

@Immutable
@Serializable
class ContainerCreatePhotoResult

@Immutable
@Serializable
class ContainerUpdatePhotoResult

@Immutable
data class Document(
    override val id: String,
    val slug: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Document"
    companion object : ModelDecoder<Document> {
        override val layout = "d615354dfb7622fbb88361a2ddb81628ff95c636cd10ab34ae33905bf9d3816d"
        override val modelName = "Document"
        override fun decode(row: WireRow): Document {
            val r = CellReader(row.cells)
            return Document(
                id = row.id,
                slug = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val slug = Column<Document, String>("slug")
        fun where(condition: Condition) = Query("Document", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Document", this, ids)
        fun bySlug(slug: String, source: String): Query<Document> {
            var built = Query("Document", this, relation = "bySlug", params = buildJsonObject { put("slug", jsonValue(slug)); put("source", jsonValue(source)) })
            return built
        }
        suspend fun create(slug: String): Document = offCaller {
            val values = buildJsonObject {
                put("slug", jsonValue(slug))
            }
            read(DBlayer.database.create("Document", values.jsonText(), "{}"))
        }
    suspend fun update(slug: String? = null): Document = offCaller {
        val values = buildJsonObject {
            slug?.let { put("slug", jsonValue(it)) }
        }
        Document.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class Chat(
    override val id: String,
    val title: String,
    val serverRevision: Long,
    val lists: List<String>,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Chat"
    companion object : ModelDecoder<Chat> {
        override val layout = "b2e10e5dccda51fb5406718b018d2a6e51d4ce0f44de9b73f057b9f227ea29ed"
        override val modelName = "Chat"
        override fun decode(row: WireRow): Chat {
            val r = CellReader(row.cells)
            return Chat(
                id = row.id,
                title = r.req(0, Cell::asText),
                serverRevision = r.req(1, Cell::asInt),
                lists = r.list(2, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val title = Column<Chat, String>("title")
        val serverRevision = Column<Chat, Long>("serverRevision")
        val lists = Column<Chat, List<String>>("lists")
        fun where(condition: Condition) = Query("Chat", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Chat", this, ids)
        suspend fun node(id: String, force: Boolean = false): Chat? = offCaller { DBlayer.database.node("Chat", "node", id, force)?.let { read(it) } }
        fun list(list: String): Query<Chat> {
            var built = Query("Chat", this, relation = "list", params = buildJsonObject { put("list", jsonValue(list)) })
            return built
        }
        suspend fun create(title: String, serverRevision: Long, lists: List<String>): Chat = offCaller {
            val values = buildJsonObject {
                put("title", jsonValue(title))
                put("serverRevision", jsonValue(serverRevision))
                put("lists", jsonValue(lists))
            }
            read(DBlayer.database.create("Chat", values.jsonText(), "{}"))
        }
    suspend fun update(title: String? = null, serverRevision: Long? = null, lists: List<String>? = null): Chat = offCaller {
        val values = buildJsonObject {
            title?.let { put("title", jsonValue(it)) }
            serverRevision?.let { put("serverRevision", jsonValue(it)) }
            lists?.let { put("lists", jsonValue(it)) }
        }
        Chat.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class Message(
    override val id: String,
    val text: String,
    val chatId: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Message"
    companion object : ModelDecoder<Message> {
        override val layout = "7f04a6af8db05e525fca0f2c4ab814e68b7f988e351f5f29237e9671f3c26f53"
        override val modelName = "Message"
        override fun decode(row: WireRow): Message {
            val r = CellReader(row.cells)
            return Message(
                id = row.id,
                text = r.req(0, Cell::asText),
                chatId = r.req(1, Cell::asId),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val text = Column<Message, String>("text")
        val chatId = Column<Message, String>("chatId")
        fun where(condition: Condition) = Query("Message", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Message", this, ids)
        suspend fun node(id: String, force: Boolean = false): Message? = offCaller { DBlayer.database.node("Message", "node", id, force)?.let { read(it) } }
        suspend fun create(text: String, chatId: String): Message = offCaller {
            val values = buildJsonObject {
                put("text", jsonValue(text))
                put("chatId", jsonValue(chatId))
            }
            read(DBlayer.database.create("Message", values.jsonText(), "{}"))
        }
    suspend fun update(text: String? = null, chatId: String? = null): Message = offCaller {
        val values = buildJsonObject {
            text?.let { put("text", jsonValue(it)) }
            chatId?.let { put("chatId", jsonValue(it)) }
        }
        Message.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class InterfaceRecord(
    override val id: String,
    val slug: String,
    val spec: SampleSpec? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "InterfaceRecord"
    companion object : ModelDecoder<InterfaceRecord> {
        override val layout = "d9411739ed10137bf5f5d850dcceb18d129a6b2b3944bb2081ccf6eef529886f"
        override val modelName = "InterfaceRecord"
        override fun decode(row: WireRow): InterfaceRecord {
            val r = CellReader(row.cells)
            return InterfaceRecord(
                id = row.id,
                slug = r.req(0, Cell::asText),
                spec = r.objOpt(1) { SampleSpec.fromCells(it) },
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val slug = Column<InterfaceRecord, String>("slug")
        val spec = Column<InterfaceRecord, SampleSpec?>("spec")
        fun where(condition: Condition) = Query("InterfaceRecord", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("InterfaceRecord", this, ids)
        suspend fun node(id: String, force: Boolean = false): InterfaceRecord? = offCaller { DBlayer.database.node("InterfaceRecord", "node", id, force)?.let { read(it) } }
        suspend fun create(slug: String, spec: SampleSpec? = null): InterfaceRecord = offCaller {
            val values = buildJsonObject {
                put("slug", jsonValue(slug))
                spec?.let { put("spec", jsonValue(it)) }
            }
            read(DBlayer.database.create("InterfaceRecord", values.jsonText(), "{}"))
        }
    suspend fun update(slug: String? = null, spec: GraphQLNullable<SampleSpec> = GraphQLNullable.Omitted): InterfaceRecord = offCaller {
        val values = buildJsonObject {
            slug?.let { put("slug", jsonValue(it)) }
            put("spec", spec)
        }
        InterfaceRecord.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class KeywordRecord(
    override val id: String,
    val default: String,
    val `class`: String,
    val value: KeywordValue? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "KeywordRecord"
    companion object : ModelDecoder<KeywordRecord> {
        override val layout = "30d8a50e4efa9241aaa8a14cc65f1e12b424397931dbff303f3268636176a5f4"
        override val modelName = "KeywordRecord"
        override fun decode(row: WireRow): KeywordRecord {
            val r = CellReader(row.cells)
            return KeywordRecord(
                id = row.id,
                default = r.req(0, Cell::asText),
                `class` = r.req(1, Cell::asText),
                value = r.objOpt(2) { KeywordValue.fromCells(it) },
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val default = Column<KeywordRecord, String>("default")
        val `class` = Column<KeywordRecord, String>("class")
        val value = Column<KeywordRecord, KeywordValue?>("value")
        fun where(condition: Condition) = Query("KeywordRecord", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("KeywordRecord", this, ids)
        fun `when`(`class`: String): Query<KeywordRecord> {
            var built = Query("KeywordRecord", this, relation = "when", params = buildJsonObject { put("class", jsonValue(`class`)) })
            built = built.where(Condition.equal("class", jsonValue(`class`)))
            return built
        }
        suspend fun create(default: String, `class`: String, value: KeywordValue? = null): KeywordRecord = offCaller {
            val values = buildJsonObject {
                put("default", jsonValue(default))
                put("class", jsonValue(`class`))
                value?.let { put("value", jsonValue(it)) }
            }
            read(DBlayer.database.create("KeywordRecord", values.jsonText(), "{}"))
        }
    suspend fun update(default: String? = null, `class`: String? = null, value: GraphQLNullable<KeywordValue> = GraphQLNullable.Omitted): KeywordRecord = offCaller {
        val values = buildJsonObject {
            default?.let { put("default", jsonValue(it)) }
            `class`?.let { put("class", jsonValue(it)) }
            put("value", value)
        }
        KeywordRecord.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class MediaMessage(
    override val id: String,
    val chatId: String,
    val kind: MediaMessageKind,
    val mediaKind: MediaKindValue? = null,
    val sequenceNumber: Long? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "MediaMessage"
    companion object : ModelDecoder<MediaMessage> {
        override val layout = "7d8a82402f1136ae53881e394710bc6c3cc3c11271b6d196ad4a7ba2f647c1c9"
        override val modelName = "MediaMessage"
        override fun decode(row: WireRow): MediaMessage {
            val r = CellReader(row.cells)
            return MediaMessage(
                id = row.id,
                chatId = r.req(0, Cell::asId),
                kind = r.req(1, Cell::asText).let { MediaMessageKind.of(it) },
                mediaKind = r.opt(2, Cell::asText)?.let { MediaKindValue.of(it) },
                sequenceNumber = r.opt(3, Cell::asInt),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val chatId = Column<MediaMessage, String>("chatId")
        val kind = Column<MediaMessage, MediaMessageKind>("kind")
        val mediaKind = Column<MediaMessage, MediaKindValue?>("mediaKind")
        val sequenceNumber = Column<MediaMessage, Long?>("sequenceNumber")
        fun where(condition: Condition) = Query("MediaMessage", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("MediaMessage", this, ids)
        fun visualMedia(chatId: String): Query<MediaMessage> {
            var built = Query("MediaMessage", this, relation = "visual_media", params = buildJsonObject { put("chatId", jsonValue(chatId)) })
            return built
        }
        fun audioMedia(chatId: String): Query<MediaMessage> {
            var built = Query("MediaMessage", this, relation = "audio_media", params = buildJsonObject { put("chatId", jsonValue(chatId)) })
            return built
        }
        fun giftMedia(chatId: String): Query<MediaMessage> {
            var built = Query("MediaMessage", this, relation = "gift_media", params = buildJsonObject { put("chatId", jsonValue(chatId)) })
            return built
        }
        fun visualSince(chatId: String, afterSequence: Long): Query<MediaMessage> {
            var built = Query("MediaMessage", this, relation = "visual_since", params = buildJsonObject { put("chatId", jsonValue(chatId)); put("afterSequence", jsonValue(afterSequence)) })
            return built
        }
        fun audioSince(chatId: String, afterSequence: Long): Query<MediaMessage> {
            var built = Query("MediaMessage", this, relation = "audio_since", params = buildJsonObject { put("chatId", jsonValue(chatId)); put("afterSequence", jsonValue(afterSequence)) })
            return built
        }
        fun giftsSince(chatId: String, afterSequence: Long): Query<MediaMessage> {
            var built = Query("MediaMessage", this, relation = "gifts_since", params = buildJsonObject { put("chatId", jsonValue(chatId)); put("afterSequence", jsonValue(afterSequence)) })
            return built
        }
        suspend fun create(chatId: String, kind: MediaMessageKind, mediaKind: MediaKindValue? = null, sequenceNumber: Long? = null): MediaMessage = offCaller {
            val values = buildJsonObject {
                put("chatId", jsonValue(chatId))
                put("kind", jsonValue(kind))
                mediaKind?.let { put("mediaKind", jsonValue(it)) }
                sequenceNumber?.let { put("sequenceNumber", jsonValue(it)) }
            }
            read(DBlayer.database.create("MediaMessage", values.jsonText(), "{}"))
        }
    suspend fun update(chatId: String? = null, kind: MediaMessageKind? = null, mediaKind: GraphQLNullable<MediaKindValue> = GraphQLNullable.Omitted, sequenceNumber: GraphQLNullable<Long> = GraphQLNullable.Omitted): MediaMessage = offCaller {
        val values = buildJsonObject {
            chatId?.let { put("chatId", jsonValue(it)) }
            kind?.let { put("kind", jsonValue(it)) }
            put("mediaKind", mediaKind)
            put("sequenceNumber", sequenceNumber)
        }
        MediaMessage.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
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
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Moment"
    companion object : ModelDecoder<Moment> {
        override val layout = "c505026a0e516c5ada9c1476fe85ce3827b542e9154608c489ed8729e13d96bc"
        override val modelName = "Moment"
        override fun decode(row: WireRow): Moment {
            val r = CellReader(row.cells)
            return Moment(
                id = row.id,
                uuid = r.req(0, Cell::asText),
                userId = r.opt(1, Cell::asId),
                text = r.opt(2, Cell::asText),
                likedByMe = r.req(3, Cell::asBool),
                media = r.json(4),
                tone = r.opt(5, Cell::asText),
                createdAt = r.req(6, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val uuid = Column<Moment, String>("uuid")
        val userId = Column<Moment, String?>("userId")
        val text = Column<Moment, String?>("text")
        val likedByMe = Column<Moment, Boolean>("likedByMe")
        val media = Column<Moment, JsonElement>("media")
        val tone = Column<Moment, String?>("tone")
        val createdAt = Column<Moment, String>("createdAt")
        fun where(condition: Condition) = Query("Moment", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Moment", this, ids)
        suspend fun transcodeStatus(id: String, force: Boolean = false): Moment? = offCaller { DBlayer.database.node("Moment", "transcode_status", id, force)?.let { read(it) } }
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
        suspend fun feedEdge(id: String): MomentFeedEdge? = feed.edgeValues(id)?.let { MomentFeedEdge(it) }
        fun vibeFeed(vibeId: String): Query<Moment> {
            var built = Query("Moment", this, relation = "vibe_feed", params = buildJsonObject { put("vibeId", jsonValue(vibeId)) })
            return built
        }
        suspend fun vibeFeedEdge(vibeId: String, id: String): MomentVibeFeedEdge? = vibeFeed(vibeId).edgeValues(id)?.let { MomentVibeFeedEdge(it) }
        suspend fun send(account: AccountScope, text: String? = null, media: JsonElement? = null): Sent<Moment> {
            val values = buildJsonObject {
                text?.let { put("text", jsonValue(it)) }
                media?.let { put("media", jsonValue(it)) }
            }
            return account.send(this, values)
        }
    suspend fun update(uuid: String? = null, userId: GraphQLNullable<String> = GraphQLNullable.Omitted, text: GraphQLNullable<String> = GraphQLNullable.Omitted, likedByMe: Boolean? = null, media: JsonElement? = null, createdAt: String? = null): Moment = offCaller {
        val values = buildJsonObject {
            uuid?.let { put("uuid", jsonValue(it)) }
            put("userId", userId)
            put("text", text)
            likedByMe?.let { put("likedByMe", jsonValue(it)) }
            media?.let { put("media", jsonValue(it)) }
            createdAt?.let { put("createdAt", jsonValue(it)) }
        }
        Moment.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
    suspend fun like(): Moment = act("like", JsonObject(parseJsonObject("""{"likedByMe":true}""") + mapOf()), JsonObject(mapOf()), Moment)
    suspend fun share(): ActionAnswer<Moment, MomentShareResult> = actAnswering("share", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf()), Moment, MomentShareResult.serializer())
    suspend fun uploadClip(account: AccountScope, request: UploadRequest): ActionAnswer<Moment, MomentUploadClipResult> = account.upload(this, "upload_clip", request, MomentUploadClipResult.serializer())
    object Relations {
        val author = Association<Moment, User>("author", User)
        val vibes = Association<Moment, Vibe>("vibes", Vibe)
    }
}

@Immutable
@Serializable
data class MomentUploadClipResult(val successMessage: String? = null)

@Immutable
@Serializable
data class MomentShareResult(val copies: Long? = null, val successMessage: String? = null)

@Immutable
data class NestedSample(
    override val id: String,
    val phase: NestedSamplePhase? = null,
    val value: NestedValue? = null,
    val values: List<NestedValue>,
    val tree: NestedTree? = null,
    val replyTo: RecordReplyNode? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "NestedSample"
    companion object : ModelDecoder<NestedSample> {
        override val layout = "52afdc8e805947d086c496697532378f7b168d3fdb0d9896be0740560f80b97c"
        override val modelName = "NestedSample"
        override fun decode(row: WireRow): NestedSample {
            val r = CellReader(row.cells)
            return NestedSample(
                id = row.id,
                phase = r.opt(0, Cell::asText)?.let { NestedSamplePhase.of(it) },
                value = r.objOpt(1) { NestedValue.fromCells(it) },
                values = r.arr(2) { NestedValue.fromCells(it) },
                tree = r.objOpt(3) { NestedTree.fromCells(it) },
                replyTo = r.objOpt(4) { RecordReplyNode.fromCells(it) },
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val phase = Column<NestedSample, NestedSamplePhase?>("phase")
        val value = Column<NestedSample, NestedValue?>("value")
        val values = Column<NestedSample, List<NestedValue>>("values")
        val tree = Column<NestedSample, NestedTree?>("tree")
        val replyTo = Column<NestedSample, RecordReplyNode?>("replyTo")
        fun where(condition: Condition) = Query("NestedSample", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("NestedSample", this, ids)
        suspend fun create(phase: NestedSamplePhase? = null, value: NestedValue? = null, values: List<NestedValue>? = null, tree: NestedTree? = null, replyTo: RecordReplyNode? = null): NestedSample = offCaller {
            val values_ = buildJsonObject {
                phase?.let { put("phase", jsonValue(it)) }
                value?.let { put("value", jsonValue(it)) }
                values?.let { put("values", jsonValue(it)) }
                tree?.let { put("tree", jsonValue(it)) }
                replyTo?.let { put("replyTo", jsonValue(it)) }
            }
            read(DBlayer.database.create("NestedSample", values_.jsonText(), "{}"))
        }
    suspend fun update(phase: GraphQLNullable<NestedSamplePhase> = GraphQLNullable.Omitted, value: GraphQLNullable<NestedValue> = GraphQLNullable.Omitted, values: List<NestedValue>? = null, tree: GraphQLNullable<NestedTree> = GraphQLNullable.Omitted, replyTo: GraphQLNullable<RecordReplyNode> = GraphQLNullable.Omitted): NestedSample = offCaller {
        val values_ = buildJsonObject {
            put("phase", phase)
            put("value", value)
            values?.let { put("values", jsonValue(it)) }
            put("tree", tree)
            put("replyTo", replyTo)
        }
        NestedSample.read(DBlayer.database.update(modelName, id, values_.jsonText(), "{}"))
    }
}

@Immutable
data class NestedReference(
    override val id: String,
    val values: List<NestedValue>,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "NestedReference"
    companion object : ModelDecoder<NestedReference> {
        override val layout = "97882d0dff35deb4c13aa698fda6e4caa3110d8b23342a0c15c10b29c64bd1ed"
        override val modelName = "NestedReference"
        override fun decode(row: WireRow): NestedReference {
            val r = CellReader(row.cells)
            return NestedReference(
                id = row.id,
                values = r.arr(0) { NestedValue.fromCells(it) },
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val values = Column<NestedReference, List<NestedValue>>("values")
        fun where(condition: Condition) = Query("NestedReference", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("NestedReference", this, ids)
        suspend fun create(values: List<NestedValue>? = null): NestedReference = offCaller {
            val values_ = buildJsonObject {
                values?.let { put("values", jsonValue(it)) }
            }
            read(DBlayer.database.create("NestedReference", values_.jsonText(), "{}"))
        }
    suspend fun update(values: List<NestedValue>? = null): NestedReference = offCaller {
        val values_ = buildJsonObject {
            values?.let { put("values", jsonValue(it)) }
        }
        NestedReference.read(DBlayer.database.update(modelName, id, values_.jsonText(), "{}"))
    }
}

@Immutable
data class Notice(
    override val id: String,
    val body: String,
    val occurredAt: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Notice"
    companion object : ModelDecoder<Notice> {
        override val layout = "6c5b254256db9ea92b4b3a01919bd0240688c7bcff6fc7962fab448a43277dac"
        override val modelName = "Notice"
        override fun decode(row: WireRow): Notice {
            val r = CellReader(row.cells)
            return Notice(
                id = row.id,
                body = r.req(0, Cell::asText),
                occurredAt = r.req(1, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val body = Column<Notice, String>("body")
        val occurredAt = Column<Notice, String>("occurredAt")
        fun where(condition: Condition) = Query("Notice", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Notice", this, ids)
        val recent: Query<Notice> get() {
            var built = Query("Notice", this, relation = "recent", params = buildJsonObject {  })
            built = built.order(Order("occurredAt", "desc"))
            return built
        }
        suspend fun create(body: String, occurredAt: String): Notice = offCaller {
            val values = buildJsonObject {
                put("body", jsonValue(body))
                put("occurredAt", jsonValue(occurredAt))
            }
            read(DBlayer.database.create("Notice", values.jsonText(), "{}"))
        }
    suspend fun update(body: String? = null, occurredAt: String? = null): Notice = offCaller {
        val values = buildJsonObject {
            body?.let { put("body", jsonValue(it)) }
            occurredAt?.let { put("occurredAt", jsonValue(it)) }
        }
        Notice.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class InteractionNotice(
    override val id: String,
    val recordId: String,
    val actorUserId: String,
    val occurredAt: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "InteractionNotice"
    companion object : ModelDecoder<InteractionNotice> {
        override val layout = "7ed7b3ff621f791d10e5925656859f01e34c46cb5c1a2bee390ff6f057db4b6a"
        override val modelName = "InteractionNotice"
        override fun decode(row: WireRow): InteractionNotice {
            val r = CellReader(row.cells)
            return InteractionNotice(
                id = row.id,
                recordId = r.req(0, Cell::asId),
                actorUserId = r.req(1, Cell::asId),
                occurredAt = r.req(2, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val recordId = Column<InteractionNotice, String>("recordId")
        val actorUserId = Column<InteractionNotice, String>("actorUserId")
        val occurredAt = Column<InteractionNotice, String>("occurredAt")
        fun where(condition: Condition) = Query("InteractionNotice", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("InteractionNotice", this, ids)
        val recent: Query<InteractionNotice> get() {
            var built = Query("InteractionNotice", this, relation = "recent", params = buildJsonObject {  })
            built = built.order(Order("occurredAt", "desc"))
            return built
        }
        suspend fun create(recordId: String, actorUserId: String, occurredAt: String): InteractionNotice = offCaller {
            val values = buildJsonObject {
                put("recordId", jsonValue(recordId))
                put("actorUserId", jsonValue(actorUserId))
                put("occurredAt", jsonValue(occurredAt))
            }
            read(DBlayer.database.create("InteractionNotice", values.jsonText(), "{}"))
        }
    suspend fun update(recordId: String? = null, actorUserId: String? = null, occurredAt: String? = null): InteractionNotice = offCaller {
        val values = buildJsonObject {
            recordId?.let { put("recordId", jsonValue(it)) }
            actorUserId?.let { put("actorUserId", jsonValue(it)) }
            occurredAt?.let { put("occurredAt", jsonValue(it)) }
        }
        InteractionNotice.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class ChatAction(
    override val id: String,
    val chatId: String,
    val userId: String,
    val kind: String,
    val active: Boolean,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "ChatAction"
    companion object : ModelDecoder<ChatAction> {
        override val layout = "b55e02f22bda54283d7380d5f2aa30ed868acba6e775e077cf6f1b2e1b956caf"
        override val modelName = "ChatAction"
        override fun decode(row: WireRow): ChatAction {
            val r = CellReader(row.cells)
            return ChatAction(
                id = row.id,
                chatId = r.req(0, Cell::asId),
                userId = r.req(1, Cell::asId),
                kind = r.req(2, Cell::asText),
                active = r.req(3, Cell::asBool),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val chatId = Column<ChatAction, String>("chatId")
        val userId = Column<ChatAction, String>("userId")
        val kind = Column<ChatAction, String>("kind")
        val active = Column<ChatAction, Boolean>("active")
        fun where(condition: Condition) = Query("ChatAction", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("ChatAction", this, ids)
        fun byChat(chatId: String): Query<ChatAction> {
            var built = Query("ChatAction", this, relation = "by_chat", params = buildJsonObject { put("chatId", jsonValue(chatId)) })
            built = built.where(Condition.equal("chatId", jsonValue(chatId)))
            return built
        }
        suspend fun create(chatId: String, userId: String, kind: String, active: Boolean): ChatAction = offCaller {
            val values = buildJsonObject {
                put("chatId", jsonValue(chatId))
                put("userId", jsonValue(userId))
                put("kind", jsonValue(kind))
                put("active", jsonValue(active))
            }
            read(DBlayer.database.create("ChatAction", values.jsonText(), "{}"))
        }
    suspend fun update(chatId: String? = null, userId: String? = null, kind: String? = null, active: Boolean? = null): ChatAction = offCaller {
        val values = buildJsonObject {
            chatId?.let { put("chatId", jsonValue(it)) }
            userId?.let { put("userId", jsonValue(it)) }
            kind?.let { put("kind", jsonValue(it)) }
            active?.let { put("active", jsonValue(it)) }
        }
        ChatAction.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class ChatActivityNotice(
    override val id: String,
    val chatId: String,
    val userId: String,
    val kind: String,
    val active: Boolean,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "ChatActivityNotice"
    companion object : ModelDecoder<ChatActivityNotice> {
        override val layout = "b55e02f22bda54283d7380d5f2aa30ed868acba6e775e077cf6f1b2e1b956caf"
        override val modelName = "ChatActivityNotice"
        override fun decode(row: WireRow): ChatActivityNotice {
            val r = CellReader(row.cells)
            return ChatActivityNotice(
                id = row.id,
                chatId = r.req(0, Cell::asId),
                userId = r.req(1, Cell::asId),
                kind = r.req(2, Cell::asText),
                active = r.req(3, Cell::asBool),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val chatId = Column<ChatActivityNotice, String>("chatId")
        val userId = Column<ChatActivityNotice, String>("userId")
        val kind = Column<ChatActivityNotice, String>("kind")
        val active = Column<ChatActivityNotice, Boolean>("active")
        fun where(condition: Condition) = Query("ChatActivityNotice", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("ChatActivityNotice", this, ids)
        val recent: Query<ChatActivityNotice> get() {
            var built = Query("ChatActivityNotice", this, relation = "recent", params = buildJsonObject {  })
            built = built.order(Order("kind", "asc"))
            return built
        }
        suspend fun create(chatId: String, userId: String, kind: String, active: Boolean): ChatActivityNotice = offCaller {
            val values = buildJsonObject {
                put("chatId", jsonValue(chatId))
                put("userId", jsonValue(userId))
                put("kind", jsonValue(kind))
                put("active", jsonValue(active))
            }
            read(DBlayer.database.create("ChatActivityNotice", values.jsonText(), "{}"))
        }
    suspend fun update(chatId: String? = null, userId: String? = null, kind: String? = null, active: Boolean? = null): ChatActivityNotice = offCaller {
        val values = buildJsonObject {
            chatId?.let { put("chatId", jsonValue(it)) }
            userId?.let { put("userId", jsonValue(it)) }
            kind?.let { put("kind", jsonValue(it)) }
            active?.let { put("active", jsonValue(it)) }
        }
        ChatActivityNotice.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class Presence(
    override val id: String,
    val userId: String,
    val room: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
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
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val userId = Column<Presence, String>("userId")
        val room = Column<Presence, String>("room")
        fun where(condition: Condition) = Query("Presence", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Presence", this, ids)
        fun forUser(userId: String): Query<Presence> {
            var built = Query("Presence", this, relation = "for_user", params = buildJsonObject { put("userId", jsonValue(userId)) })
            built = built.where(Condition.equal("userId", jsonValue(userId)))
            return built
        }
        val online: Query<Presence> get() {
            var built = Query("Presence", this, relation = "online", params = buildJsonObject {  })
            return built
        }
        fun onChannel(channel: String): Query<Presence> {
            var built = Query("Presence", this, relation = "on_channel", params = buildJsonObject { put("channel", jsonValue(channel)) })
            return built
        }
        fun history(presenceId: String): Query<Presence> {
            var built = Query("Presence", this, relation = "history", params = buildJsonObject { put("presenceId", jsonValue(presenceId)) })
            return built
        }
        suspend fun create(userId: String, room: String): Presence = offCaller {
            val values = buildJsonObject {
                put("userId", jsonValue(userId))
                put("room", jsonValue(room))
            }
            read(DBlayer.database.create("Presence", values.jsonText(), "{}"))
        }
    suspend fun update(userId: String? = null, room: String? = null): Presence = offCaller {
        val values = buildJsonObject {
            userId?.let { put("userId", jsonValue(it)) }
            room?.let { put("room", jsonValue(it)) }
        }
        Presence.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
    object Relations {
        val user = Association<Presence, User>("user", User)
    }
}

@Immutable
data class Record(
    override val id: String,
    val containerId: String,
    val authorId: String,
    val regionId: String? = null,
    val text: String,
    val createdAt: String,
    val sequenceNumber: Long? = null,
    val reactions: List<RecordReactionNode>,
    val attachment: RecordAttachment? = null,
    val media: RecordMedia? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Record"
    companion object : ModelDecoder<Record> {
        override val layout = "33f44335aa00c05718e2368d6acb029ee8c5347b5113f9ee3cc8a5c394629c52"
        override val modelName = "Record"
        override fun decode(row: WireRow): Record {
            val r = CellReader(row.cells)
            return Record(
                id = row.id,
                containerId = r.req(0, Cell::asId),
                authorId = r.req(1, Cell::asId),
                regionId = r.opt(2, Cell::asId),
                text = r.req(3, Cell::asText),
                createdAt = r.req(4, Cell::asText),
                sequenceNumber = r.opt(5, Cell::asInt),
                reactions = r.arr(6) { RecordReactionNode.fromCells(it) },
                attachment = r.objOpt(7) { RecordAttachment.fromCells(it) },
                media = r.objOpt(8) { RecordMedia.fromCells(it) },
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val containerId = Column<Record, String>("containerId")
        val authorId = Column<Record, String>("authorId")
        val regionId = Column<Record, String?>("regionId")
        val text = Column<Record, String>("text")
        val createdAt = Column<Record, String>("createdAt")
        val sequenceNumber = Column<Record, Long?>("sequenceNumber")
        val reactions = Column<Record, List<RecordReactionNode>>("reactions")
        val attachment = Column<Record, RecordAttachmentKind>("attachment")
        val media = Column<Record, RecordMedia?>("media")
        fun where(condition: Condition) = Query("Record", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Record", this, ids)
        fun byContainer(containerId: String): Query<Record> {
            var built = Query("Record", this, relation = "by_container", params = buildJsonObject { put("containerId", jsonValue(containerId)) })
            built = built.where(Condition.equal("containerId", jsonValue(containerId)))
            built = built.order(Order("createdAt", "asc"))
            return built
        }
        fun thread(containerId: String): Query<Record> {
            var built = Query("Record", this, relation = "thread", params = buildJsonObject { put("containerId", jsonValue(containerId)) })
            return built
        }
        suspend fun node(id: String, force: Boolean = false): Record? = offCaller { DBlayer.database.node("Record", "node", id, force)?.let { read(it) } }
        suspend fun transcodeStatus(id: String, force: Boolean = false): Record? = offCaller { DBlayer.database.node("Record", "transcode_status", id, force)?.let { read(it) } }
        fun containerMedia(containerId: String): Query<Record> {
            var built = Query("Record", this, relation = "container_media", params = buildJsonObject { put("containerId", jsonValue(containerId)) })
            return built
        }
        val history: Query<Record> get() {
            var built = Query("Record", this, relation = "history", params = buildJsonObject {  })
            return built
        }
        suspend fun send(account: AccountScope, containerId: String, authorId: String? = null, regionId: String? = null, text: String, attachedUserId: String? = null, signedId: String? = null): Sent<Record> {
            val values = buildJsonObject {
                put("containerId", jsonValue(containerId))
                authorId?.let { put("authorId", jsonValue(it)) }
                regionId?.let { put("regionId", jsonValue(it)) }
                put("text", jsonValue(text))
            }
            val args = buildJsonObject {
                attachedUserId?.let { put("attachedUserId", jsonValue(it)) }
                signedId?.let { put("signedId", jsonValue(it)) }
            }
            return account.send(this, values, args)
        }
    suspend fun update(containerId: String? = null, authorId: String? = null, regionId: GraphQLNullable<String> = GraphQLNullable.Omitted, text: String? = null, createdAt: String? = null, sequenceNumber: GraphQLNullable<Long> = GraphQLNullable.Omitted, reactions: List<RecordReactionNode>? = null, attachment: GraphQLNullable<RecordAttachment> = GraphQLNullable.Omitted, media: GraphQLNullable<RecordMedia> = GraphQLNullable.Omitted): Record = offCaller {
        val values = buildJsonObject {
            containerId?.let { put("containerId", jsonValue(it)) }
            authorId?.let { put("authorId", jsonValue(it)) }
            put("regionId", regionId)
            text?.let { put("text", jsonValue(it)) }
            createdAt?.let { put("createdAt", jsonValue(it)) }
            put("sequenceNumber", sequenceNumber)
            reactions?.let { put("reactions", jsonValue(it)) }
            put("attachment", attachment)
            put("media", media)
        }
        Record.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
    suspend fun react(reactionBody: String, userId: String): Record = act("react", JsonObject(parseJsonObject("""{"reactions":{"${'$'}keyed":{"by":"userId","element":{"body":{"${'$'}arg":"reactionBody"},"userId":{"${'$'}arg":"userId"}}}}}""") + mapOf()), JsonObject(mapOf("reactionBody" to jsonValue(reactionBody), "userId" to jsonValue(userId))), Record)
    object Relations {
        val container = Association<Record, Container>("container", Container)
    }
}

@Immutable
data class CurrentUser(
    override val id: String,
    val name: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "CurrentUser"
    companion object : ModelDecoder<CurrentUser> {
        override val layout = "daaaa5a3ca91ab534db47731be1fca27d87f8dd345d457bfd43bf77767339316"
        override val modelName = "CurrentUser"
        override fun decode(row: WireRow): CurrentUser {
            val r = CellReader(row.cells)
            return CurrentUser(
                id = row.id,
                name = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val name = Column<CurrentUser, String>("name")
        fun where(condition: Condition) = Query("CurrentUser", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("CurrentUser", this, ids)
        val current: Query<CurrentUser> get() {
            var built = Query("CurrentUser", this, relation = "current", params = buildJsonObject {  })
            return built
        }
        suspend fun create(name: String): CurrentUser = offCaller {
            val values = buildJsonObject {
                put("name", jsonValue(name))
            }
            read(DBlayer.database.create("CurrentUser", values.jsonText(), "{}"))
        }
    suspend fun update(name: String? = null): CurrentUser = offCaller {
        val values = buildJsonObject {
            name?.let { put("name", jsonValue(it)) }
        }
        CurrentUser.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class CurrentRegion(
    override val id: String,
    val name: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "CurrentRegion"
    companion object : ModelDecoder<CurrentRegion> {
        override val layout = "daaaa5a3ca91ab534db47731be1fca27d87f8dd345d457bfd43bf77767339316"
        override val modelName = "CurrentRegion"
        override fun decode(row: WireRow): CurrentRegion {
            val r = CellReader(row.cells)
            return CurrentRegion(
                id = row.id,
                name = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val name = Column<CurrentRegion, String>("name")
        fun where(condition: Condition) = Query("CurrentRegion", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("CurrentRegion", this, ids)
        val current: Query<CurrentRegion> get() {
            var built = Query("CurrentRegion", this, relation = "current", params = buildJsonObject {  })
            return built
        }
        suspend fun create(name: String): CurrentRegion = offCaller {
            val values = buildJsonObject {
                put("name", jsonValue(name))
            }
            read(DBlayer.database.create("CurrentRegion", values.jsonText(), "{}"))
        }
    suspend fun update(name: String? = null): CurrentRegion = offCaller {
        val values = buildJsonObject {
            name?.let { put("name", jsonValue(it)) }
        }
        CurrentRegion.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class CustomScreen(
    override val id: String,
    val slug: String,
    val version: Long,
    val requires: ScreenRequires,
    val spec: ScreenSpec? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "CustomScreen"
    companion object : ModelDecoder<CustomScreen> {
        override val layout = "3e13f0e795a7c1d99a23f73a144dd682d8dd7c01b156079f8dc93ce6122c9129"
        override val modelName = "CustomScreen"
        override fun decode(row: WireRow): CustomScreen {
            val r = CellReader(row.cells)
            return CustomScreen(
                id = row.id,
                slug = r.req(0, Cell::asText),
                version = r.req(1, Cell::asInt),
                requires = r.obj(2) { ScreenRequires.fromCells(it) },
                spec = r.objOpt(3) { ScreenSpec.fromCells(it) },
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val slug = Column<CustomScreen, String>("slug")
        val version = Column<CustomScreen, Long>("version")
        val requires = Column<CustomScreen, ScreenRequires>("requires")
        val spec = Column<CustomScreen, ScreenSpec?>("spec")
        fun where(condition: Condition) = Query("CustomScreen", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("CustomScreen", this, ids)
        suspend fun node(id: String, force: Boolean = false): CustomScreen? = offCaller { DBlayer.database.node("CustomScreen", "node", id, force)?.let { read(it) } }
        suspend fun create(slug: String, version: Long, requires: ScreenRequires, spec: ScreenSpec? = null): CustomScreen = offCaller {
            val values = buildJsonObject {
                put("slug", jsonValue(slug))
                put("version", jsonValue(version))
                put("requires", jsonValue(requires))
                spec?.let { put("spec", jsonValue(it)) }
            }
            read(DBlayer.database.create("CustomScreen", values.jsonText(), "{}"))
        }
    suspend fun update(slug: String? = null, version: Long? = null, requires: ScreenRequires? = null, spec: GraphQLNullable<ScreenSpec> = GraphQLNullable.Omitted): CustomScreen = offCaller {
        val values = buildJsonObject {
            slug?.let { put("slug", jsonValue(it)) }
            version?.let { put("version", jsonValue(it)) }
            requires?.let { put("requires", jsonValue(it)) }
            put("spec", spec)
        }
        CustomScreen.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class StorageRow(
    override val id: String,
    val record: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
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
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val record = Column<StorageRow, String>("record")
        fun where(condition: Condition) = Query("StorageRow", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("StorageRow", this, ids)
        suspend fun create(record: String): StorageRow = offCaller {
            val values = buildJsonObject {
                put("record", jsonValue(record))
            }
            read(DBlayer.database.create("StorageRow", values.jsonText(), "{}"))
        }
    suspend fun update(record: String? = null): StorageRow = offCaller {
        val values = buildJsonObject {
            record?.let { put("record", jsonValue(it)) }
        }
        StorageRow.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class Tail(
    override val id: String,
    val text: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Tail"
    companion object : ModelDecoder<Tail> {
        override val layout = "79f6ac4394adc1b4758abd377fb226a4146dcdcfa51a32720beacc9bac4a680e"
        override val modelName = "Tail"
        override fun decode(row: WireRow): Tail {
            val r = CellReader(row.cells)
            return Tail(
                id = row.id,
                text = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val text = Column<Tail, String>("text")
        fun where(condition: Condition) = Query("Tail", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Tail", this, ids)
        fun since(containerId: String, afterSequence: Long, pinned: Boolean): Query<Tail> {
            var built = Query("Tail", this, relation = "since", params = buildJsonObject { put("containerId", jsonValue(containerId)); put("afterSequence", jsonValue(afterSequence)); put("pinned", jsonValue(pinned)) })
            return built
        }
        suspend fun create(text: String): Tail = offCaller {
            val values = buildJsonObject {
                put("text", jsonValue(text))
            }
            read(DBlayer.database.create("Tail", values.jsonText(), "{}"))
        }
    suspend fun update(text: String? = null): Tail = offCaller {
        val values = buildJsonObject {
            text?.let { put("text", jsonValue(it)) }
        }
        Tail.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class UnionSample(
    override val id: String,
    val label: String? = null,
    val attachment: RecordAttachment? = null,
    val props: MomentWidgetItemProps? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "UnionSample"
    companion object : ModelDecoder<UnionSample> {
        override val layout = "423693dfea54829309e39b8ef392190ba49023893e1e8842786138a3b4a19c2a"
        override val modelName = "UnionSample"
        override fun decode(row: WireRow): UnionSample {
            val r = CellReader(row.cells)
            return UnionSample(
                id = row.id,
                label = r.opt(0, Cell::asText),
                attachment = r.objOpt(1) { RecordAttachment.fromCells(it) },
                props = r.objOpt(2) { MomentWidgetItemProps.fromCells(it) },
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val label = Column<UnionSample, String?>("label")
        val attachment = Column<UnionSample, RecordAttachmentKind>("attachment")
        val props = Column<UnionSample, MomentWidgetItemPropsKind>("props")
        fun where(condition: Condition) = Query("UnionSample", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("UnionSample", this, ids)
        suspend fun create(label: String? = null, attachment: RecordAttachment? = null, props: MomentWidgetItemProps? = null): UnionSample = offCaller {
            val values = buildJsonObject {
                label?.let { put("label", jsonValue(it)) }
                attachment?.let { put("attachment", jsonValue(it)) }
                props?.let { put("props", jsonValue(it)) }
            }
            read(DBlayer.database.create("UnionSample", values.jsonText(), "{}"))
        }
    suspend fun update(label: GraphQLNullable<String> = GraphQLNullable.Omitted, attachment: GraphQLNullable<RecordAttachment> = GraphQLNullable.Omitted, props: GraphQLNullable<MomentWidgetItemProps> = GraphQLNullable.Omitted): UnionSample = offCaller {
        val values = buildJsonObject {
            put("label", label)
            put("attachment", attachment)
            put("props", props)
        }
        UnionSample.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class User(
    override val id: String,
    val name: String,
    val avatar: FileInfo? = null,
    val birthday: String? = null,
    val typing: Boolean,
    val mood: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
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
                avatar = r.fileOpt(1),
                birthday = r.opt(2, Cell::asText),
                typing = r.req(3, Cell::asBool),
                mood = r.opt(4, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val avatar = FileField<User>("avatar")
        val name = Column<User, String>("name")
        val birthday = Column<User, String?>("birthday")
        fun where(condition: Condition) = Query("User", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("User", this, ids)
        val profile: Query<User> get() {
            var built = Query("User", this, relation = "profile", params = buildJsonObject {  })
            return built
        }
        fun containerMembers(containerId: String): Query<User> {
            var built = Query("User", this, relation = "container_members", params = buildJsonObject { put("containerId", jsonValue(containerId)) })
            return built
        }
        fun completeMembers(containerId: String): Query<User> {
            var built = Query("User", this, relation = "complete_members", params = buildJsonObject { put("containerId", jsonValue(containerId)) })
            return built
        }
        suspend fun create(name: String, birthday: String? = null): User = offCaller {
            val values = buildJsonObject {
                put("name", jsonValue(name))
                birthday?.let { put("birthday", jsonValue(it)) }
            }
            read(DBlayer.database.create("User", values.jsonText(), "{}"))
        }
    suspend fun update(name: String? = null, birthday: GraphQLNullable<String> = GraphQLNullable.Omitted): User = offCaller {
        val values = buildJsonObject {
            name?.let { put("name", jsonValue(it)) }
            put("birthday", birthday)
        }
        User.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
    suspend fun set(typing: Boolean? = null, mood: GraphQLNullable<String> = GraphQLNullable.Omitted) {
        offCaller { DBlayer.database.setEphemeral(modelName, id, buildJsonObject {
            typing?.let { put("typing", jsonValue(it)) }
            put("mood", mood)
        }.jsonText()) }
    }
}

@Immutable
data class Vibe(
    override val id: String,
    val momentId: String,
    val userId: String,
    val kind: VibeKind,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
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
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val momentId = Column<Vibe, String>("momentId")
        val userId = Column<Vibe, String>("userId")
        val kind = Column<Vibe, VibeKind>("kind")
        fun where(condition: Condition) = Query("Vibe", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Vibe", this, ids)
        suspend fun create(momentId: String, userId: String, kind: VibeKind): Vibe = offCaller {
            val values = buildJsonObject {
                put("momentId", jsonValue(momentId))
                put("userId", jsonValue(userId))
                put("kind", jsonValue(kind))
            }
            read(DBlayer.database.create("Vibe", values.jsonText(), "{}"))
        }
    suspend fun update(momentId: String? = null, userId: String? = null, kind: VibeKind? = null): Vibe = offCaller {
        val values = buildJsonObject {
            momentId?.let { put("momentId", jsonValue(it)) }
            userId?.let { put("userId", jsonValue(it)) }
            kind?.let { put("kind", jsonValue(it)) }
        }
        Vibe.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class MomentFeedEdge(
    val impressionUuid: String? = null,
    val impressionScore: Long? = null
) {
    internal constructor(values: JsonObject) : this((values["impressionUuid"] as? JsonPrimitive)?.contentOrNull, (values["impressionScore"] as? JsonPrimitive)?.longOrNull)
}

@Immutable
data class MomentVibeFeedEdge(
    val impressionUuid: String? = null,
    val score: Long? = null,
    val rank: FeedRank? = null
) {
    internal constructor(values: JsonObject) : this((values["impressionUuid"] as? JsonPrimitive)?.contentOrNull, (values["score"] as? JsonPrimitive)?.longOrNull, (values["rank"] as? JsonPrimitive)?.contentOrNull?.let { FeedRank.of(it) })
}

@Immutable
@Serializable
data class SampleSpec(
    @Serializable(with = IDText::class) val root: String,
    val elements: List<SampleElement>,
    val state: List<SampleStateEntry>,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleSpec {
            val r = CellReader(cells)
            return SampleSpec(
                root = r.req(0, Cell::asId),
                elements = r.arr(1) { SampleElement.fromCells(it) },
                state = r.arr(2) { SampleStateEntry.fromCells(it) },
            )
        }
    }
}

@Immutable
@Serializable(with = SampleElementSerializer::class)
sealed class SampleElement {
    @Immutable
    data class Button(val value: SampleButton) : SampleElement()
    @Immutable
    data class Column(val value: SampleColumn) : SampleElement()
    @Immutable
    data class Heading(val value: SampleHeading) : SampleElement()
    @Immutable
    data class Pressable(val value: SamplePressable) : SampleElement()
    @Immutable
    data class Spacer(val value: SampleSpacer) : SampleElement()
    @Immutable
    data class Switch(val value: SampleSwitch) : SampleElement()
    @Immutable
    data class Unknown(val typename: String) : SampleElement()

    val kind: SampleElementKind?
        get() = when (this) {
            is Button -> SampleElementKind.Button
            is Column -> SampleElementKind.Column
            is Heading -> SampleElementKind.Heading
            is Pressable -> SampleElementKind.Pressable
            is Spacer -> SampleElementKind.Spacer
            is Switch -> SampleElementKind.Switch
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

    val press: SampleAction?
        get() = when (this) {
            is Button -> value.press
            is Column -> value.press
            is Heading -> value.press
            is Pressable -> value.press
            is Spacer -> value.press
            is Switch -> value.press
            is Unknown -> null
        }

    val visible: SampleVisibility?
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
        fun fromCells(cells: List<Cell>): SampleElement {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "SampleButton" -> Button(r.obj(1) { SampleButton.fromCells(it) })
                "SampleColumn" -> Column(r.obj(1) { SampleColumn.fromCells(it) })
                "SampleHeading" -> Heading(r.obj(1) { SampleHeading.fromCells(it) })
                "SamplePressable" -> Pressable(r.obj(1) { SamplePressable.fromCells(it) })
                "SampleSpacer" -> Spacer(r.obj(1) { SampleSpacer.fromCells(it) })
                "SampleSwitch" -> Switch(r.obj(1) { SampleSwitch.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object SampleElementSerializer : UnionSerializer<SampleElement>("SampleElement") {
    override fun read(typename: String, body: JsonObject): SampleElement = when (typename) {
        "SampleButton" -> SampleElement.Button(dbJson.decodeFromJsonElement(SampleButton.serializer(), body))
        "SampleColumn" -> SampleElement.Column(dbJson.decodeFromJsonElement(SampleColumn.serializer(), body))
        "SampleHeading" -> SampleElement.Heading(dbJson.decodeFromJsonElement(SampleHeading.serializer(), body))
        "SamplePressable" -> SampleElement.Pressable(dbJson.decodeFromJsonElement(SamplePressable.serializer(), body))
        "SampleSpacer" -> SampleElement.Spacer(dbJson.decodeFromJsonElement(SampleSpacer.serializer(), body))
        "SampleSwitch" -> SampleElement.Switch(dbJson.decodeFromJsonElement(SampleSwitch.serializer(), body))
        else -> SampleElement.Unknown(typename)
    }

    override fun write(value: SampleElement): Pair<String, JsonElement> = when (value) {
        is SampleElement.Button -> "SampleButton" to dbJson.encodeToJsonElement(SampleButton.serializer(), value.value)
        is SampleElement.Column -> "SampleColumn" to dbJson.encodeToJsonElement(SampleColumn.serializer(), value.value)
        is SampleElement.Heading -> "SampleHeading" to dbJson.encodeToJsonElement(SampleHeading.serializer(), value.value)
        is SampleElement.Pressable -> "SamplePressable" to dbJson.encodeToJsonElement(SamplePressable.serializer(), value.value)
        is SampleElement.Spacer -> "SampleSpacer" to dbJson.encodeToJsonElement(SampleSpacer.serializer(), value.value)
        is SampleElement.Switch -> "SampleSwitch" to dbJson.encodeToJsonElement(SampleSwitch.serializer(), value.value)
        is SampleElement.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class SampleElementKind {
    @SerialName("SampleButton") Button,
    @SerialName("SampleColumn") Column,
    @SerialName("SampleHeading") Heading,
    @SerialName("SamplePressable") Pressable,
    @SerialName("SampleSpacer") Spacer,
    @SerialName("SampleSwitch") Switch,
}

@Immutable
@Serializable
data class SampleButton(
    @Serializable(with = IDText::class) val id: String,
    @Serializable(with = IDTexts::class) val children: List<String>,
    val press: SampleAction? = null,
    val visible: SampleVisibility? = null,
    val label: SampleTextValue? = null,
    val disabled: SampleFlagValue? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleButton {
            val r = CellReader(cells)
            return SampleButton(
                id = r.req(0, Cell::asId),
                children = r.list(1, Cell::asId),
                press = r.objOpt(2) { SampleAction.fromCells(it) },
                visible = r.objOpt(3) { SampleVisibility.fromCells(it) },
                label = r.objOpt(4) { SampleTextValue.fromCells(it) },
                disabled = r.objOpt(5) { SampleFlagValue.fromCells(it) },
            )
        }
    }
}

@Immutable
@Serializable(with = SampleActionSerializer::class)
sealed class SampleAction {
    @Immutable
    data class Navigate(val value: SampleNavigateAction) : SampleAction()
    @Immutable
    data class OpenRecord(val value: SampleOpenRecordAction) : SampleAction()
    @Immutable
    data class OpenExternal(val value: SampleOpenExternalAction) : SampleAction()
    @Immutable
    data class OpenProfile(val value: SampleOpenProfileAction) : SampleAction()
    @Immutable
    data class Unknown(val typename: String) : SampleAction()

    val kind: SampleActionKind?
        get() = when (this) {
            is Navigate -> SampleActionKind.Navigate
            is OpenRecord -> SampleActionKind.OpenRecord
            is OpenExternal -> SampleActionKind.OpenExternal
            is OpenProfile -> SampleActionKind.OpenProfile
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): SampleAction {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "SampleNavigateAction" -> Navigate(r.obj(1) { SampleNavigateAction.fromCells(it) })
                "SampleOpenRecordAction" -> OpenRecord(r.obj(1) { SampleOpenRecordAction.fromCells(it) })
                "SampleOpenExternalAction" -> OpenExternal(r.obj(1) { SampleOpenExternalAction.fromCells(it) })
                "SampleOpenProfileAction" -> OpenProfile(r.obj(1) { SampleOpenProfileAction.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object SampleActionSerializer : UnionSerializer<SampleAction>("SampleAction") {
    override fun read(typename: String, body: JsonObject): SampleAction = when (typename) {
        "SampleNavigateAction" -> SampleAction.Navigate(dbJson.decodeFromJsonElement(SampleNavigateAction.serializer(), body))
        "SampleOpenRecordAction" -> SampleAction.OpenRecord(dbJson.decodeFromJsonElement(SampleOpenRecordAction.serializer(), body))
        "SampleOpenExternalAction" -> SampleAction.OpenExternal(dbJson.decodeFromJsonElement(SampleOpenExternalAction.serializer(), body))
        "SampleOpenProfileAction" -> SampleAction.OpenProfile(dbJson.decodeFromJsonElement(SampleOpenProfileAction.serializer(), body))
        else -> SampleAction.Unknown(typename)
    }

    override fun write(value: SampleAction): Pair<String, JsonElement> = when (value) {
        is SampleAction.Navigate -> "SampleNavigateAction" to dbJson.encodeToJsonElement(SampleNavigateAction.serializer(), value.value)
        is SampleAction.OpenRecord -> "SampleOpenRecordAction" to dbJson.encodeToJsonElement(SampleOpenRecordAction.serializer(), value.value)
        is SampleAction.OpenExternal -> "SampleOpenExternalAction" to dbJson.encodeToJsonElement(SampleOpenExternalAction.serializer(), value.value)
        is SampleAction.OpenProfile -> "SampleOpenProfileAction" to dbJson.encodeToJsonElement(SampleOpenProfileAction.serializer(), value.value)
        is SampleAction.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class SampleActionKind {
    @SerialName("SampleNavigateAction") Navigate,
    @SerialName("SampleOpenRecordAction") OpenRecord,
    @SerialName("SampleOpenExternalAction") OpenExternal,
    @SerialName("SampleOpenProfileAction") OpenProfile,
}

@Immutable
@Serializable
data class SampleNavigateAction(
    val slug: String,
    val title: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleNavigateAction {
            val r = CellReader(cells)
            return SampleNavigateAction(
                slug = r.req(0, Cell::asText),
                title = r.opt(1, Cell::asText),
            )
        }
    }
}

@Immutable
@Serializable
data class SampleOpenRecordAction(
    val recordId: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleOpenRecordAction {
            val r = CellReader(cells)
            return SampleOpenRecordAction(
                recordId = r.req(0, Cell::asText),
            )
        }
    }
}

@Immutable
@Serializable
data class SampleOpenExternalAction(
    val url: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleOpenExternalAction {
            val r = CellReader(cells)
            return SampleOpenExternalAction(
                url = r.req(0, Cell::asText),
            )
        }
    }
}

@Immutable
@Serializable
data class SampleOpenProfileAction(
    val userId: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleOpenProfileAction {
            val r = CellReader(cells)
            return SampleOpenProfileAction(
                userId = r.req(0, Cell::asText),
            )
        }
    }
}

@Immutable
@Serializable(with = SampleVisibilitySerializer::class)
sealed class SampleVisibility {
    @Immutable
    data class Condition(val value: SampleCondition) : SampleVisibility()
    @Immutable
    data class VisibleAll(val value: SampleVisibleAll) : SampleVisibility()
    @Immutable
    data class VisibleAny(val value: SampleVisibleAny) : SampleVisibility()
    @Immutable
    data class VisibleFlag(val value: SampleVisibleFlag) : SampleVisibility()
    @Immutable
    data class Unknown(val typename: String) : SampleVisibility()

    val kind: SampleVisibilityKind?
        get() = when (this) {
            is Condition -> SampleVisibilityKind.Condition
            is VisibleAll -> SampleVisibilityKind.VisibleAll
            is VisibleAny -> SampleVisibilityKind.VisibleAny
            is VisibleFlag -> SampleVisibilityKind.VisibleFlag
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): SampleVisibility {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "SampleCondition" -> Condition(r.obj(1) { SampleCondition.fromCells(it) })
                "SampleVisibleAll" -> VisibleAll(r.obj(1) { SampleVisibleAll.fromCells(it) })
                "SampleVisibleAny" -> VisibleAny(r.obj(1) { SampleVisibleAny.fromCells(it) })
                "SampleVisibleFlag" -> VisibleFlag(r.obj(1) { SampleVisibleFlag.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object SampleVisibilitySerializer : UnionSerializer<SampleVisibility>("SampleVisibility") {
    override fun read(typename: String, body: JsonObject): SampleVisibility = when (typename) {
        "SampleCondition" -> SampleVisibility.Condition(dbJson.decodeFromJsonElement(SampleCondition.serializer(), body))
        "SampleVisibleAll" -> SampleVisibility.VisibleAll(dbJson.decodeFromJsonElement(SampleVisibleAll.serializer(), body))
        "SampleVisibleAny" -> SampleVisibility.VisibleAny(dbJson.decodeFromJsonElement(SampleVisibleAny.serializer(), body))
        "SampleVisibleFlag" -> SampleVisibility.VisibleFlag(dbJson.decodeFromJsonElement(SampleVisibleFlag.serializer(), body))
        else -> SampleVisibility.Unknown(typename)
    }

    override fun write(value: SampleVisibility): Pair<String, JsonElement> = when (value) {
        is SampleVisibility.Condition -> "SampleCondition" to dbJson.encodeToJsonElement(SampleCondition.serializer(), value.value)
        is SampleVisibility.VisibleAll -> "SampleVisibleAll" to dbJson.encodeToJsonElement(SampleVisibleAll.serializer(), value.value)
        is SampleVisibility.VisibleAny -> "SampleVisibleAny" to dbJson.encodeToJsonElement(SampleVisibleAny.serializer(), value.value)
        is SampleVisibility.VisibleFlag -> "SampleVisibleFlag" to dbJson.encodeToJsonElement(SampleVisibleFlag.serializer(), value.value)
        is SampleVisibility.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class SampleVisibilityKind {
    @SerialName("SampleCondition") Condition,
    @SerialName("SampleVisibleAll") VisibleAll,
    @SerialName("SampleVisibleAny") VisibleAny,
    @SerialName("SampleVisibleFlag") VisibleFlag,
}

@Immutable
@Serializable
data class SampleCondition(
    val comparison: String,
    val negated: Boolean,
    val operand: SampleOperand? = null,
    val path: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleCondition {
            val r = CellReader(cells)
            return SampleCondition(
                comparison = r.req(0, Cell::asText),
                negated = r.req(1, Cell::asBool),
                operand = r.objOpt(2) { SampleOperand.fromCells(it) },
                path = r.req(3, Cell::asText),
            )
        }
    }
}

@Immutable
@Serializable(with = SampleOperandSerializer::class)
sealed class SampleOperand {
    @Immutable
    data class FlagLiteral(val value: SampleFlagLiteral) : SampleOperand()
    @Immutable
    data class NumberLiteral(val value: SampleNumberLiteral) : SampleOperand()
    @Immutable
    data class StateRef(val value: SampleStateRef) : SampleOperand()
    @Immutable
    data class TextLiteral(val value: SampleTextLiteral) : SampleOperand()
    @Immutable
    data class Unknown(val typename: String) : SampleOperand()

    val kind: SampleOperandKind?
        get() = when (this) {
            is FlagLiteral -> SampleOperandKind.FlagLiteral
            is NumberLiteral -> SampleOperandKind.NumberLiteral
            is StateRef -> SampleOperandKind.StateRef
            is TextLiteral -> SampleOperandKind.TextLiteral
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): SampleOperand {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "SampleFlagLiteral" -> FlagLiteral(r.obj(1) { SampleFlagLiteral.fromCells(it) })
                "SampleNumberLiteral" -> NumberLiteral(r.obj(1) { SampleNumberLiteral.fromCells(it) })
                "SampleStateRef" -> StateRef(r.obj(1) { SampleStateRef.fromCells(it) })
                "SampleTextLiteral" -> TextLiteral(r.obj(1) { SampleTextLiteral.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object SampleOperandSerializer : UnionSerializer<SampleOperand>("SampleOperand") {
    override fun read(typename: String, body: JsonObject): SampleOperand = when (typename) {
        "SampleFlagLiteral" -> SampleOperand.FlagLiteral(dbJson.decodeFromJsonElement(SampleFlagLiteral.serializer(), body))
        "SampleNumberLiteral" -> SampleOperand.NumberLiteral(dbJson.decodeFromJsonElement(SampleNumberLiteral.serializer(), body))
        "SampleStateRef" -> SampleOperand.StateRef(dbJson.decodeFromJsonElement(SampleStateRef.serializer(), body))
        "SampleTextLiteral" -> SampleOperand.TextLiteral(dbJson.decodeFromJsonElement(SampleTextLiteral.serializer(), body))
        else -> SampleOperand.Unknown(typename)
    }

    override fun write(value: SampleOperand): Pair<String, JsonElement> = when (value) {
        is SampleOperand.FlagLiteral -> "SampleFlagLiteral" to dbJson.encodeToJsonElement(SampleFlagLiteral.serializer(), value.value)
        is SampleOperand.NumberLiteral -> "SampleNumberLiteral" to dbJson.encodeToJsonElement(SampleNumberLiteral.serializer(), value.value)
        is SampleOperand.StateRef -> "SampleStateRef" to dbJson.encodeToJsonElement(SampleStateRef.serializer(), value.value)
        is SampleOperand.TextLiteral -> "SampleTextLiteral" to dbJson.encodeToJsonElement(SampleTextLiteral.serializer(), value.value)
        is SampleOperand.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class SampleOperandKind {
    @SerialName("SampleFlagLiteral") FlagLiteral,
    @SerialName("SampleNumberLiteral") NumberLiteral,
    @SerialName("SampleStateRef") StateRef,
    @SerialName("SampleTextLiteral") TextLiteral,
}

@Immutable
@Serializable
data class SampleFlagLiteral(
    val flag: Boolean,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleFlagLiteral {
            val r = CellReader(cells)
            return SampleFlagLiteral(
                flag = r.req(0, Cell::asBool),
            )
        }
    }
}

@Immutable
@Serializable
data class SampleNumberLiteral(
    val number: Double,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleNumberLiteral {
            val r = CellReader(cells)
            return SampleNumberLiteral(
                number = r.req(0, Cell::asDouble),
            )
        }
    }
}

@Immutable
@Serializable
data class SampleStateRef(
    val path: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleStateRef {
            val r = CellReader(cells)
            return SampleStateRef(
                path = r.req(0, Cell::asText),
            )
        }
    }
}

@Immutable
@Serializable
data class SampleTextLiteral(
    val text: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleTextLiteral {
            val r = CellReader(cells)
            return SampleTextLiteral(
                text = r.req(0, Cell::asText),
            )
        }
    }
}

@Immutable
@Serializable
data class SampleVisibleAll(
    val conditions: List<SampleCondition>,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleVisibleAll {
            val r = CellReader(cells)
            return SampleVisibleAll(
                conditions = r.arr(0) { SampleCondition.fromCells(it) },
            )
        }
    }
}

@Immutable
@Serializable
data class SampleVisibleAny(
    val conditions: List<SampleCondition>,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleVisibleAny {
            val r = CellReader(cells)
            return SampleVisibleAny(
                conditions = r.arr(0) { SampleCondition.fromCells(it) },
            )
        }
    }
}

@Immutable
@Serializable
data class SampleVisibleFlag(
    val flag: Boolean,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleVisibleFlag {
            val r = CellReader(cells)
            return SampleVisibleFlag(
                flag = r.req(0, Cell::asBool),
            )
        }
    }
}

@Immutable
@Serializable(with = SampleTextValueSerializer::class)
sealed class SampleTextValue {
    @Immutable
    data class StateRef(val value: SampleStateRef) : SampleTextValue()
    @Immutable
    data class TextLiteral(val value: SampleTextLiteral) : SampleTextValue()
    @Immutable
    data class Unknown(val typename: String) : SampleTextValue()

    val kind: SampleTextValueKind?
        get() = when (this) {
            is StateRef -> SampleTextValueKind.StateRef
            is TextLiteral -> SampleTextValueKind.TextLiteral
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): SampleTextValue {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "SampleStateRef" -> StateRef(r.obj(1) { SampleStateRef.fromCells(it) })
                "SampleTextLiteral" -> TextLiteral(r.obj(1) { SampleTextLiteral.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object SampleTextValueSerializer : UnionSerializer<SampleTextValue>("SampleTextValue") {
    override fun read(typename: String, body: JsonObject): SampleTextValue = when (typename) {
        "SampleStateRef" -> SampleTextValue.StateRef(dbJson.decodeFromJsonElement(SampleStateRef.serializer(), body))
        "SampleTextLiteral" -> SampleTextValue.TextLiteral(dbJson.decodeFromJsonElement(SampleTextLiteral.serializer(), body))
        else -> SampleTextValue.Unknown(typename)
    }

    override fun write(value: SampleTextValue): Pair<String, JsonElement> = when (value) {
        is SampleTextValue.StateRef -> "SampleStateRef" to dbJson.encodeToJsonElement(SampleStateRef.serializer(), value.value)
        is SampleTextValue.TextLiteral -> "SampleTextLiteral" to dbJson.encodeToJsonElement(SampleTextLiteral.serializer(), value.value)
        is SampleTextValue.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class SampleTextValueKind {
    @SerialName("SampleStateRef") StateRef,
    @SerialName("SampleTextLiteral") TextLiteral,
}

@Immutable
@Serializable(with = SampleFlagValueSerializer::class)
sealed class SampleFlagValue {
    @Immutable
    data class FlagLiteral(val value: SampleFlagLiteral) : SampleFlagValue()
    @Immutable
    data class StateRef(val value: SampleStateRef) : SampleFlagValue()
    @Immutable
    data class Unknown(val typename: String) : SampleFlagValue()

    val kind: SampleFlagValueKind?
        get() = when (this) {
            is FlagLiteral -> SampleFlagValueKind.FlagLiteral
            is StateRef -> SampleFlagValueKind.StateRef
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): SampleFlagValue {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "SampleFlagLiteral" -> FlagLiteral(r.obj(1) { SampleFlagLiteral.fromCells(it) })
                "SampleStateRef" -> StateRef(r.obj(1) { SampleStateRef.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object SampleFlagValueSerializer : UnionSerializer<SampleFlagValue>("SampleFlagValue") {
    override fun read(typename: String, body: JsonObject): SampleFlagValue = when (typename) {
        "SampleFlagLiteral" -> SampleFlagValue.FlagLiteral(dbJson.decodeFromJsonElement(SampleFlagLiteral.serializer(), body))
        "SampleStateRef" -> SampleFlagValue.StateRef(dbJson.decodeFromJsonElement(SampleStateRef.serializer(), body))
        else -> SampleFlagValue.Unknown(typename)
    }

    override fun write(value: SampleFlagValue): Pair<String, JsonElement> = when (value) {
        is SampleFlagValue.FlagLiteral -> "SampleFlagLiteral" to dbJson.encodeToJsonElement(SampleFlagLiteral.serializer(), value.value)
        is SampleFlagValue.StateRef -> "SampleStateRef" to dbJson.encodeToJsonElement(SampleStateRef.serializer(), value.value)
        is SampleFlagValue.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class SampleFlagValueKind {
    @SerialName("SampleFlagLiteral") FlagLiteral,
    @SerialName("SampleStateRef") StateRef,
}

@Immutable
@Serializable
data class SampleColumn(
    @Serializable(with = IDText::class) val id: String,
    @Serializable(with = IDTexts::class) val children: List<String>,
    val press: SampleAction? = null,
    val visible: SampleVisibility? = null,
    val gap: SampleNumberValue? = null,
    val alignItems: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleColumn {
            val r = CellReader(cells)
            return SampleColumn(
                id = r.req(0, Cell::asId),
                children = r.list(1, Cell::asId),
                press = r.objOpt(2) { SampleAction.fromCells(it) },
                visible = r.objOpt(3) { SampleVisibility.fromCells(it) },
                gap = r.objOpt(4) { SampleNumberValue.fromCells(it) },
                alignItems = r.opt(5, Cell::asText),
            )
        }
    }
}

@Immutable
@Serializable(with = SampleNumberValueSerializer::class)
sealed class SampleNumberValue {
    @Immutable
    data class NumberLiteral(val value: SampleNumberLiteral) : SampleNumberValue()
    @Immutable
    data class StateRef(val value: SampleStateRef) : SampleNumberValue()
    @Immutable
    data class Unknown(val typename: String) : SampleNumberValue()

    val kind: SampleNumberValueKind?
        get() = when (this) {
            is NumberLiteral -> SampleNumberValueKind.NumberLiteral
            is StateRef -> SampleNumberValueKind.StateRef
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): SampleNumberValue {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "SampleNumberLiteral" -> NumberLiteral(r.obj(1) { SampleNumberLiteral.fromCells(it) })
                "SampleStateRef" -> StateRef(r.obj(1) { SampleStateRef.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object SampleNumberValueSerializer : UnionSerializer<SampleNumberValue>("SampleNumberValue") {
    override fun read(typename: String, body: JsonObject): SampleNumberValue = when (typename) {
        "SampleNumberLiteral" -> SampleNumberValue.NumberLiteral(dbJson.decodeFromJsonElement(SampleNumberLiteral.serializer(), body))
        "SampleStateRef" -> SampleNumberValue.StateRef(dbJson.decodeFromJsonElement(SampleStateRef.serializer(), body))
        else -> SampleNumberValue.Unknown(typename)
    }

    override fun write(value: SampleNumberValue): Pair<String, JsonElement> = when (value) {
        is SampleNumberValue.NumberLiteral -> "SampleNumberLiteral" to dbJson.encodeToJsonElement(SampleNumberLiteral.serializer(), value.value)
        is SampleNumberValue.StateRef -> "SampleStateRef" to dbJson.encodeToJsonElement(SampleStateRef.serializer(), value.value)
        is SampleNumberValue.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class SampleNumberValueKind {
    @SerialName("SampleNumberLiteral") NumberLiteral,
    @SerialName("SampleStateRef") StateRef,
}

@Immutable
@Serializable
data class SampleHeading(
    @Serializable(with = IDText::class) val id: String,
    @Serializable(with = IDTexts::class) val children: List<String>,
    val press: SampleAction? = null,
    val visible: SampleVisibility? = null,
    val text: SampleTextValue? = null,
    val level: Long? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleHeading {
            val r = CellReader(cells)
            return SampleHeading(
                id = r.req(0, Cell::asId),
                children = r.list(1, Cell::asId),
                press = r.objOpt(2) { SampleAction.fromCells(it) },
                visible = r.objOpt(3) { SampleVisibility.fromCells(it) },
                text = r.objOpt(4) { SampleTextValue.fromCells(it) },
                level = r.opt(5, Cell::asInt),
            )
        }
    }
}

@Immutable
@Serializable
data class SamplePressable(
    @Serializable(with = IDText::class) val id: String,
    @Serializable(with = IDTexts::class) val children: List<String>,
    val press: SampleAction? = null,
    val visible: SampleVisibility? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SamplePressable {
            val r = CellReader(cells)
            return SamplePressable(
                id = r.req(0, Cell::asId),
                children = r.list(1, Cell::asId),
                press = r.objOpt(2) { SampleAction.fromCells(it) },
                visible = r.objOpt(3) { SampleVisibility.fromCells(it) },
            )
        }
    }
}

@Immutable
@Serializable
data class SampleSpacer(
    @Serializable(with = IDText::class) val id: String,
    @Serializable(with = IDTexts::class) val children: List<String>,
    val press: SampleAction? = null,
    val visible: SampleVisibility? = null,
    val length: SampleNumberValue? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleSpacer {
            val r = CellReader(cells)
            return SampleSpacer(
                id = r.req(0, Cell::asId),
                children = r.list(1, Cell::asId),
                press = r.objOpt(2) { SampleAction.fromCells(it) },
                visible = r.objOpt(3) { SampleVisibility.fromCells(it) },
                length = r.objOpt(4) { SampleNumberValue.fromCells(it) },
            )
        }
    }
}

@Immutable
@Serializable
data class SampleSwitch(
    @Serializable(with = IDText::class) val id: String,
    @Serializable(with = IDTexts::class) val children: List<String>,
    val press: SampleAction? = null,
    val visible: SampleVisibility? = null,
    val checked: SampleFlagValue? = null,
    val label: SampleTextValue? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleSwitch {
            val r = CellReader(cells)
            return SampleSwitch(
                id = r.req(0, Cell::asId),
                children = r.list(1, Cell::asId),
                press = r.objOpt(2) { SampleAction.fromCells(it) },
                visible = r.objOpt(3) { SampleVisibility.fromCells(it) },
                checked = r.objOpt(4) { SampleFlagValue.fromCells(it) },
                label = r.objOpt(5) { SampleTextValue.fromCells(it) },
            )
        }
    }
}

@Immutable
@Serializable
data class SampleStateEntry(
    val path: String,
    val value: SampleStateValue,
) {
    companion object {
        fun fromCells(cells: List<Cell>): SampleStateEntry {
            val r = CellReader(cells)
            return SampleStateEntry(
                path = r.req(0, Cell::asText),
                value = r.obj(1) { SampleStateValue.fromCells(it) },
            )
        }
    }
}

@Immutable
@Serializable(with = SampleStateValueSerializer::class)
sealed class SampleStateValue {
    @Immutable
    data class Flag(val value: SampleFlagLiteral) : SampleStateValue()
    @Immutable
    data class Number(val value: SampleNumberLiteral) : SampleStateValue()
    @Immutable
    data class Text(val value: SampleTextLiteral) : SampleStateValue()
    @Immutable
    data class Unknown(val typename: String) : SampleStateValue()

    val kind: SampleStateValueKind?
        get() = when (this) {
            is Flag -> SampleStateValueKind.Flag
            is Number -> SampleStateValueKind.Number
            is Text -> SampleStateValueKind.Text
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): SampleStateValue {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "SampleFlagLiteral" -> Flag(r.obj(1) { SampleFlagLiteral.fromCells(it) })
                "SampleNumberLiteral" -> Number(r.obj(1) { SampleNumberLiteral.fromCells(it) })
                "SampleTextLiteral" -> Text(r.obj(1) { SampleTextLiteral.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object SampleStateValueSerializer : UnionSerializer<SampleStateValue>("SampleStateValue") {
    override fun read(typename: String, body: JsonObject): SampleStateValue = when (typename) {
        "SampleFlagLiteral" -> SampleStateValue.Flag(dbJson.decodeFromJsonElement(SampleFlagLiteral.serializer(), body))
        "SampleNumberLiteral" -> SampleStateValue.Number(dbJson.decodeFromJsonElement(SampleNumberLiteral.serializer(), body))
        "SampleTextLiteral" -> SampleStateValue.Text(dbJson.decodeFromJsonElement(SampleTextLiteral.serializer(), body))
        else -> SampleStateValue.Unknown(typename)
    }

    override fun write(value: SampleStateValue): Pair<String, JsonElement> = when (value) {
        is SampleStateValue.Flag -> "SampleFlagLiteral" to dbJson.encodeToJsonElement(SampleFlagLiteral.serializer(), value.value)
        is SampleStateValue.Number -> "SampleNumberLiteral" to dbJson.encodeToJsonElement(SampleNumberLiteral.serializer(), value.value)
        is SampleStateValue.Text -> "SampleTextLiteral" to dbJson.encodeToJsonElement(SampleTextLiteral.serializer(), value.value)
        is SampleStateValue.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class SampleStateValueKind {
    @SerialName("SampleFlagLiteral") Flag,
    @SerialName("SampleNumberLiteral") Number,
    @SerialName("SampleTextLiteral") Text,
}

@Immutable
@Serializable
data class KeywordValue(
    val default: String,
    val `when`: String,
    val `class`: String,
    val container: String,
    val r: String,
    val child: KeywordChild = KeywordChild(default = "d", `when` = "w", `class` = "c"),
) {
    companion object {
        fun fromCells(cells: List<Cell>): KeywordValue {
            val r = CellReader(cells)
            return KeywordValue(
                default = r.req(0, Cell::asText),
                `when` = r.req(1, Cell::asText),
                `class` = r.req(2, Cell::asText),
                container = r.req(3, Cell::asText),
                r = r.req(4, Cell::asText),
                child = r.obj(5) { KeywordChild.fromCells(it) },
            )
        }
    }
}

@Immutable
@Serializable
data class KeywordChild(
    val default: String,
    val `when`: String,
    val `class`: String,
) {
    companion object {
        fun fromCells(cells: List<Cell>): KeywordChild {
            val r = CellReader(cells)
            return KeywordChild(
                default = r.req(0, Cell::asText),
                `when` = r.req(1, Cell::asText),
                `class` = r.req(2, Cell::asText),
            )
        }
    }
}

@Immutable
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
                nullableDefault = r.opt(2, Cell::asText),
                ownerId = r.opt(3, Cell::asId),
                count = r.req(4, Cell::asInt),
                ratio = r.req(5, Cell::asDouble),
                enabled = r.req(6, Cell::asBool),
                createdAt = r.opt(7, Cell::asText),
                state = r.req(8, Cell::asText).let { NestedValueState.of(it) },
                labels = r.list(9, Cell::asText),
                counts = r.list(10, Cell::asInt),
                ratios = r.list(11, Cell::asDouble),
                flags = r.list(12, Cell::asBool),
                dates = r.list(13, Cell::asText),
                owners = r.list(14, Cell::asId),
                payload = r.jsonOpt(15),
                rawValue = r.json(16),
                preferences = r.obj(17) { NestedValuePreferences.fromCells(it) },
                presets = r.arr(18) { NestedValuePreferences.fromCells(it) },
                metadata = r.jsonOpt(19),
                values = r.jsonOpt(20),
                child = r.objOpt(21) { NestedValueChild.fromCells(it) },
                children = r.arr(22) { NestedValueChild.fromCells(it) },
            )
        }
    }
}

@Immutable
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
                active = r.req(1, Cell::asBool),
            )
        }
    }
}

@Immutable
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
                horizontal = r.req(1, Cell::asDouble),
            )
        }
    }
}

@Immutable
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
                children = r.arr(1) { NestedTree.fromCells(it) },
            )
        }
    }
}

@Immutable
@Serializable
data class RecordReplyNode(
    @Serializable(with = IDText::class) val id: String? = null,
    val body: String? = null,
    val kind: String? = null,
    @Serializable(with = IDText::class) val userId: String? = null,
    val user: RecordUserRef? = null,
    val media: NestedMedia? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): RecordReplyNode {
            val r = CellReader(cells)
            return RecordReplyNode(
                id = r.opt(0, Cell::asId),
                body = r.opt(1, Cell::asText),
                kind = r.opt(2, Cell::asText),
                userId = r.opt(3, Cell::asId),
                user = r.objOpt(4) { RecordUserRef.fromCells(it) },
                media = r.objOpt(5) { NestedMedia.fromCells(it) },
            )
        }
    }
}

@Immutable
@Serializable
data class RecordUserRef(
    @Serializable(with = IDText::class) val id: String? = null,
    val fullName: String? = null,
    val name: String? = null,
    val username: String? = null,
    val avatarUrl: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): RecordUserRef {
            val r = CellReader(cells)
            return RecordUserRef(
                id = r.opt(0, Cell::asId),
                fullName = r.opt(1, Cell::asText),
                name = r.opt(2, Cell::asText),
                username = r.opt(3, Cell::asText),
                avatarUrl = r.opt(4, Cell::asText),
            )
        }
    }
}

@Immutable
@Serializable
data class NestedMedia(
    @Serializable(with = IDText::class) val id: String = "",
    val fileUrl: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): NestedMedia {
            val r = CellReader(cells)
            return NestedMedia(
                id = r.req(0, Cell::asId),
                fileUrl = r.opt(1, Cell::asText),
            )
        }
    }
}

@Immutable
@Serializable
data class RecordReactionNode(
    val body: String? = null,
    @SerialName("user.id") @Serializable(with = IDText::class) val userId: String? = null,
    val user: RecordReactionUser? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): RecordReactionNode {
            val r = CellReader(cells)
            return RecordReactionNode(
                body = r.opt(0, Cell::asText),
                userId = r.opt(1, Cell::asId),
                user = r.objOpt(2) { RecordReactionUser.fromCells(it) },
            )
        }
    }
}

@Immutable
@Serializable
data class RecordReactionUser(
    @Serializable(with = IDText::class) val id: String? = null,
    val name: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): RecordReactionUser {
            val r = CellReader(cells)
            return RecordReactionUser(
                id = r.opt(0, Cell::asId),
                name = r.opt(1, Cell::asText),
            )
        }
    }
}

@Immutable
@Serializable(with = RecordAttachmentSerializer::class)
sealed class RecordAttachment {
    @Immutable
    data class Reward(val value: RewardAttachment) : RecordAttachment()
    @Immutable
    data class Moment(val value: MomentAttachment) : RecordAttachment()
    @Immutable
    data class Profile(val value: ProfileAttachment) : RecordAttachment()
    @Immutable
    data class Reaction(val value: ReactionAttachment) : RecordAttachment()
    @Immutable
    data class Unknown(val typename: String) : RecordAttachment()

    val kind: RecordAttachmentKind?
        get() = when (this) {
            is Reward -> RecordAttachmentKind.Reward
            is Moment -> RecordAttachmentKind.Moment
            is Profile -> RecordAttachmentKind.Profile
            is Reaction -> RecordAttachmentKind.Reaction
            is Unknown -> null
        }

    companion object {
        fun fromCells(cells: List<Cell>): RecordAttachment {
            val r = CellReader(cells)
            return when (val typename = r.req(0, Cell::asText)) {
                "RewardAttachment" -> Reward(r.obj(1) { RewardAttachment.fromCells(it) })
                "MomentAttachment" -> Moment(r.obj(1) { MomentAttachment.fromCells(it) })
                "ProfileAttachment" -> Profile(r.obj(1) { ProfileAttachment.fromCells(it) })
                "ReactionAttachment" -> Reaction(r.obj(1) { ReactionAttachment.fromCells(it) })
                else -> Unknown(typename)
            }
        }
    }
}

object RecordAttachmentSerializer : UnionSerializer<RecordAttachment>("RecordAttachment") {
    override fun read(typename: String, body: JsonObject): RecordAttachment = when (typename) {
        "RewardAttachment" -> RecordAttachment.Reward(dbJson.decodeFromJsonElement(RewardAttachment.serializer(), body))
        "MomentAttachment" -> RecordAttachment.Moment(dbJson.decodeFromJsonElement(MomentAttachment.serializer(), body))
        "ProfileAttachment" -> RecordAttachment.Profile(dbJson.decodeFromJsonElement(ProfileAttachment.serializer(), body))
        "ReactionAttachment" -> RecordAttachment.Reaction(dbJson.decodeFromJsonElement(ReactionAttachment.serializer(), body))
        else -> RecordAttachment.Unknown(typename)
    }

    override fun write(value: RecordAttachment): Pair<String, JsonElement> = when (value) {
        is RecordAttachment.Reward -> "RewardAttachment" to dbJson.encodeToJsonElement(RewardAttachment.serializer(), value.value)
        is RecordAttachment.Moment -> "MomentAttachment" to dbJson.encodeToJsonElement(MomentAttachment.serializer(), value.value)
        is RecordAttachment.Profile -> "ProfileAttachment" to dbJson.encodeToJsonElement(ProfileAttachment.serializer(), value.value)
        is RecordAttachment.Reaction -> "ReactionAttachment" to dbJson.encodeToJsonElement(ReactionAttachment.serializer(), value.value)
        is RecordAttachment.Unknown -> value.typename to JsonObject(emptyMap())
    }
}

@Serializable
enum class RecordAttachmentKind {
    @SerialName("RewardAttachment") Reward,
    @SerialName("MomentAttachment") Moment,
    @SerialName("ProfileAttachment") Profile,
    @SerialName("ReactionAttachment") Reaction,
}

@Immutable
@Serializable
data class RewardAttachment(
    @Serializable(with = IDText::class) val id: String,
    @SerialName("rewardTier") val tier: String,
    val status: String? = null,
    val tags: List<String> = listOf(),
) {
    companion object {
        fun fromCells(cells: List<Cell>): RewardAttachment {
            val r = CellReader(cells)
            return RewardAttachment(
                id = r.req(0, Cell::asId),
                tier = r.req(1, Cell::asText),
                status = r.opt(2, Cell::asText),
                tags = r.list(3, Cell::asText),
            )
        }
    }
}

@Immutable
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

@Immutable
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

@Immutable
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

@Immutable
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

@Immutable
@Serializable
data class RecordMedia(
    val kind: String? = null,
    val transcoded: Boolean? = null,
    val fileUrl: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): RecordMedia {
            val r = CellReader(cells)
            return RecordMedia(
                kind = r.opt(0, Cell::asText),
                transcoded = r.opt(1, Cell::asBool),
                fileUrl = r.opt(2, Cell::asText),
            )
        }
    }
}

@Immutable
@Serializable
data class ScreenRequires(
    val ios: Long? = null,
    val android: Long? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenRequires {
            val r = CellReader(cells)
            return ScreenRequires(
                ios = r.opt(0, Cell::asInt),
                android = r.opt(1, Cell::asInt),
            )
        }
    }
}

@Immutable
@Serializable
data class ScreenSpec(
    @Serializable(with = IDText::class) val root: String,
    val state: List<ScreenStateEntry>,
    val elements: List<ScreenElement>,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenSpec {
            val r = CellReader(cells)
            return ScreenSpec(
                root = r.req(0, Cell::asId),
                state = r.arr(1) { ScreenStateEntry.fromCells(it) },
                elements = r.arr(2) { ScreenElement.fromCells(it) },
            )
        }
    }
}

@Immutable
@Serializable
data class ScreenStateEntry(
    val path: String,
    val value: JsonElement,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenStateEntry {
            val r = CellReader(cells)
            return ScreenStateEntry(
                path = r.req(0, Cell::asText),
                value = r.json(1),
            )
        }
    }
}

@Immutable
@Serializable
data class ScreenElement(
    @Serializable(with = IDText::class) val id: String,
    val type: String,
    val slot: String? = null,
    @Serializable(with = IDTexts::class) val children: List<String>,
    val props: JsonElement? = null,
    val visible: JsonElement? = null,
    val press: JsonElement? = null,
    val change: JsonElement? = null,
    val padding: String? = null,
    val margin: String? = null,
    val background: String? = null,
    val radius: String? = null,
    val expand: Boolean? = null,
    val align: String? = null,
    val accessibilityLabel: String? = null,
) {
    companion object {
        fun fromCells(cells: List<Cell>): ScreenElement {
            val r = CellReader(cells)
            return ScreenElement(
                id = r.req(0, Cell::asId),
                type = r.req(1, Cell::asText),
                slot = r.opt(2, Cell::asText),
                children = r.list(3, Cell::asId),
                props = r.jsonOpt(4),
                visible = r.jsonOpt(5),
                press = r.jsonOpt(6),
                change = r.jsonOpt(7),
                padding = r.opt(8, Cell::asText),
                margin = r.opt(9, Cell::asText),
                background = r.opt(10, Cell::asText),
                radius = r.opt(11, Cell::asText),
                expand = r.opt(12, Cell::asBool),
                align = r.opt(13, Cell::asText),
                accessibilityLabel = r.opt(14, Cell::asText),
            )
        }
    }
}

@Immutable
@Serializable(with = MomentWidgetItemPropsSerializer::class)
sealed class MomentWidgetItemProps {
    @Immutable
    data class Emoji(val value: MomentEmojiProps) : MomentWidgetItemProps()
    @Immutable
    data class Label(val value: MomentLabelProps) : MomentWidgetItemProps()
    @Immutable
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

@Immutable
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

@Immutable
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

@Immutable
@Serializable(with = ContainerShelfSerializer::class)
sealed class ContainerShelf(override val raw: String) : RawEnum {
    data object Primary : ContainerShelf("PRIMARY")
    data object Secondary : ContainerShelf("SECONDARY")
    @Immutable
    data class Unknown(val value: String) : ContainerShelf(value)

    companion object {
        fun of(raw: String): ContainerShelf = when (raw) {
            "PRIMARY" -> Primary
            "SECONDARY" -> Secondary
            else -> Unknown(raw)
        }
    }
}

object ContainerShelfSerializer : RawEnumSerializer<ContainerShelf>("ContainerShelf", { ContainerShelf.of(it) })

@Immutable
@Serializable(with = MediaMessageKindSerializer::class)
sealed class MediaMessageKind(override val raw: String) : RawEnum {
    data object Text : MediaMessageKind("TEXT")
    data object Gift : MediaMessageKind("GIFT")
    @Immutable
    data class Unknown(val value: String) : MediaMessageKind(value)

    companion object {
        fun of(raw: String): MediaMessageKind = when (raw) {
            "TEXT" -> Text
            "GIFT" -> Gift
            else -> Unknown(raw)
        }
    }
}

object MediaMessageKindSerializer : RawEnumSerializer<MediaMessageKind>("MediaMessageKind", { MediaMessageKind.of(it) })

@Immutable
@Serializable(with = MediaKindValueSerializer::class)
sealed class MediaKindValue(override val raw: String) : RawEnum {
    data object Audio : MediaKindValue("AUDIO")
    data object Image : MediaKindValue("IMAGE")
    data object Video : MediaKindValue("VIDEO")
    @Immutable
    data class Unknown(val value: String) : MediaKindValue(value)

    companion object {
        fun of(raw: String): MediaKindValue = when (raw) {
            "AUDIO" -> Audio
            "IMAGE" -> Image
            "VIDEO" -> Video
            else -> Unknown(raw)
        }
    }
}

object MediaKindValueSerializer : RawEnumSerializer<MediaKindValue>("MediaKindValue", { MediaKindValue.of(it) })

@Immutable
@Serializable(with = FeedRankSerializer::class)
sealed class FeedRank(override val raw: String) : RawEnum {
    data object Top : FeedRank("top")
    data object Normal : FeedRank("normal")
    @Immutable
    data class Unknown(val value: String) : FeedRank(value)

    companion object {
        fun of(raw: String): FeedRank = when (raw) {
            "top" -> Top
            "normal" -> Normal
            else -> Unknown(raw)
        }
    }
}

object FeedRankSerializer : RawEnumSerializer<FeedRank>("FeedRank", { FeedRank.of(it) })

@Immutable
@Serializable(with = NestedSamplePhaseSerializer::class)
sealed class NestedSamplePhase(override val raw: String) : RawEnum {
    data object Draft : NestedSamplePhase("draft")
    data object Live : NestedSamplePhase("live")
    @Immutable
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

@Immutable
@Serializable(with = NestedValueStateSerializer::class)
sealed class NestedValueState(override val raw: String) : RawEnum {
    data object Ready : NestedValueState("ready")
    data object Done : NestedValueState("done")
    @Immutable
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

@Immutable
@Serializable(with = VibeKindSerializer::class)
sealed class VibeKind(override val raw: String) : RawEnum {
    data object Like : VibeKind("like")
    data object Love : VibeKind("love")
    data object Laugh : VibeKind("laugh")
    @Immutable
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

val modelsJSON = """[{"key":"Asset","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"clientKey","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"size","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"position","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create_file","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"CreateAsset","upload":true,"durable":false,"variables":{"assetId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":["clientKey"],"ignore_response":false,"wrap":null,"root":"createAsset.asset","lands":[],"result":{}},{"batch":{"min":2,"max":10,"items":"items","single":"create_file","input":{"clientKey":"clientKey"},"renumber":{"position":"position","size":"size","position_from":0}},"client_mutation_id":false,"name":"create_files","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"CreateAssets","upload":true,"durable":false,"variables":{"assetId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":["clientKey"],"ignore_response":false,"wrap":null,"root":"createAssets.assets","lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Container","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastRecordAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastRecordId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"shareUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"isPublic","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"shelves","kind":{"type":"list","element":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ContainerShelf","values":["PRIMARY","SECONDARY"],"merge":null}],"associations":[{"name":"records","kind":"has_many","target":"Record","foreign_key":"containerId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null},{"name":"lastRecord","kind":"belongs_to","target":"Record","foreign_key":"lastRecordId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ContainerNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"nodes","by_columns":[],"text_params":[],"remote":{"operation":"ContainersByIds","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"node":"many"},{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"lastRecordAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"shelf","membership":{"shelf":{"contains":"shelves"}},"joins_node":true,"by_columns":[],"text_params":["shelf"],"text_types":{"shelf":"ContainerShelf"},"remote":{"operation":"ContainerShelfQuery","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":"containers"},"is_global":false,"sort":"server_order","joins_events":true,"joins_created":false}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateContainer","upload":false,"durable":true,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"create_photo","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ContainerGroupCreateFile","upload":true,"durable":false,"variables":{"containerId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"groupContainerCreate.container","lands":[],"result":{}},{"client_mutation_id":false,"name":"update_photo","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ContainerGroupUpdateFile","upload":true,"durable":false,"variables":{"containerId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"groupContainerUpdate.container","lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"CONTAINER","relation":"node","parameter":null,"removes_on":["DELETED","ACCESS_REVOKED"]}],"change_feed":{"subscription":"RecordEvents","root":"recordEvents.entries","area":"topic","kind":"action","id":"recordId"}},{"key":"Document","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"bySlug","updates":[{"subscription":"documentUpdated","deleted_by":null,"set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":["slug"],"text_params":["source"],"remote":{"operation":"documentBySlug","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":"documentBySlug"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Chat","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"serverRevision","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"revision","app_type":null,"merge":null},{"name":"lists","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ChatNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"list","membership":{"list":{"contains":"lists"}},"joins_node":true,"by_columns":[],"text_params":["list"],"remote":{"operation":"ChatList","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":"chats"},"is_global":false,"sort":"server_order","joins_events":true,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"MESSAGE","relation":"node","parameter":null,"payload":"chat"},{"area":"CHAT","relation":"node","parameter":null,"removes_on":["DELETED","ACCESS_REVOKED"]}],"server_revision":"serverRevision"},{"key":"Message","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"MessageNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"MESSAGE","relation":"node","parameter":null,"removes_on":["DELETED","ACCESS_REVOKED"],"payload":"message"}]},{"key":"InterfaceRecord","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"spec","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleSpec","merge":null,"fields":[{"name":"root","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"elements","kind":{"type":"union_array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleElement","merge":null,"fields":[{"name":"button","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleButton","merge":null,"fields":[{"name":"label","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextValue","merge":null,"fields":[{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null},{"name":"textLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextLiteral","merge":null}]},{"name":"disabled","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagValue","merge":null,"fields":[{"name":"flagLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagLiteral","merge":null},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null}]}]},{"name":"column","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleColumn","merge":null,"fields":[{"name":"gap","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberValue","merge":null,"fields":[{"name":"numberLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberLiteral","merge":null},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null}]},{"name":"alignItems","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"heading","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleHeading","merge":null,"fields":[{"name":"text","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextValue","merge":null},{"name":"level","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"pressable","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SamplePressable","merge":null},{"name":"spacer","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleSpacer","merge":null,"fields":[{"name":"length","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberValue","merge":null}]},{"name":"switch","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleSwitch","merge":null,"fields":[{"name":"checked","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagValue","merge":null},{"name":"label","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextValue","merge":null}]}],"shared":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"press","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleAction","merge":null,"fields":[{"name":"navigate","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNavigateAction","merge":null,"fields":[{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openRecord","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOpenRecordAction","merge":null,"fields":[{"name":"recordId","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openExternal","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOpenExternalAction","merge":null,"fields":[{"name":"url","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openProfile","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOpenProfileAction","merge":null,"fields":[{"name":"userId","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"visible","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibility","merge":null,"fields":[{"name":"condition","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleCondition","merge":null,"fields":[{"name":"comparison","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"negated","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"operand","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOperand","merge":null,"fields":[{"name":"flagLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagLiteral","merge":null,"fields":[{"name":"flag","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"numberLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberLiteral","merge":null,"fields":[{"name":"number","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"textLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextLiteral","merge":null,"fields":[{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"visibleAll","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibleAll","merge":null,"fields":[{"name":"conditions","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleCondition","merge":null}]},{"name":"visibleAny","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibleAny","merge":null,"fields":[{"name":"conditions","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleCondition","merge":null}]},{"name":"visibleFlag","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibleFlag","merge":null,"fields":[{"name":"flag","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}]},{"name":"state","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateEntry","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"union"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateValue","merge":null,"fields":[{"name":"flag","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagLiteral","merge":null},{"name":"number","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberLiteral","merge":null},{"name":"text","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextLiteral","merge":null}]}]}]}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"SampleNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"KeywordRecord","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"default","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"class","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"KeywordValue","merge":null,"fields":[{"name":"default","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"when","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"class","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"container","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"r","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"child","kind":{"type":"object"},"nullable":false,"optional":false,"default":{"class":"c","default":"d","when":"w"},"indexed":false,"id_from_parent":null,"from":null,"app_type":"KeywordChild","merge":null,"fields":[{"name":"default","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"when","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"class","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[{"name":"when","by_columns":["class"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"MediaMessage","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"enum","values":["TEXT","GIFT"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MediaMessageKind","merge":null},{"name":"mediaKind","kind":{"type":"enum","values":["AUDIO","IMAGE","VIDEO"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MediaKindValue","merge":null},{"name":"sequenceNumber","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"visual_media","row_rules":[{"field":"mediaKind","test":"in","values":["IMAGE","VIDEO"]}],"joins_node":true,"by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{"includeGifts":{"from":"constant","value":false},"mediaKinds":{"from":"constant","value":["IMAGE","VIDEO"]}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"audio_media","row_rules":[{"field":"mediaKind","test":"equals","value":"AUDIO"}],"joins_node":true,"by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{"includeGifts":{"from":"constant","value":false},"mediaKinds":{"from":"constant","value":["AUDIO"]}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"gift_media","row_rules":[{"field":"kind","test":"equals","value":"GIFT"}],"joins_node":true,"by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{"includeGifts":{"from":"constant","value":true}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"visual_since","row_rules":[{"field":"mediaKind","test":"in","values":["IMAGE","VIDEO"]},{"field":"sequenceNumber","test":"greater_than","parameter":"afterSequence"}],"joins_node":true,"by_columns":["chatId"],"text_params":["afterSequence"],"scalar_types":{"afterSequence":"int"},"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":50,"variables":{"includeGifts":{"from":"constant","value":false},"mediaKinds":{"from":"constant","value":["IMAGE","VIDEO"]}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"audio_since","row_rules":[{"field":"mediaKind","test":"equals","value":"AUDIO"},{"field":"sequenceNumber","test":"greater_than","parameter":"afterSequence"}],"joins_node":true,"by_columns":["chatId"],"text_params":["afterSequence"],"scalar_types":{"afterSequence":"int"},"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":50,"variables":{"includeGifts":{"from":"constant","value":false},"mediaKinds":{"from":"constant","value":["AUDIO"]}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"gifts_since","row_rules":[{"field":"kind","test":"equals","value":"GIFT"},{"field":"sequenceNumber","test":"greater_than","parameter":"afterSequence"}],"joins_node":true,"by_columns":["chatId"],"text_params":["afterSequence"],"scalar_types":{"afterSequence":"int"},"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":50,"variables":{"includeGifts":{"from":"constant","value":true}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false}],"indexes":[["chatId","sequenceNumber"]],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"uuid","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"searchable":true},{"name":"likedByMe","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"media","kind":{"type":"raw"},"nullable":false,"optional":false,"default":{},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tone","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":{"type":"local"}},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"author","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":true,"sets":{},"newer":[],"from":null},{"name":"vibes","kind":"has_many","target":"Vibe","foreign_key":"momentId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"transcode_status","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"MomentTranscodeStatus","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"momentId":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"by_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"feed","updates":[{"subscription":"MomentEvents","deleted_by":"destroyed","set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"impressionUuid","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"impressionScore","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"vibe_feed","by_columns":[],"text_params":["vibeId"],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"impressionUuid","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"score","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"impressionScore","app_type":null,"merge":null},{"name":"rank","kind":{"type":"enum","values":["top","normal"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"FeedRank","merge":null}]}],"indexes":[["uuid"],["userId","createdAt"]],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateMoment","upload":false,"durable":true,"variables":{"media":"media","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"update","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"UpdateMoment","upload":false,"durable":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"upload_clip","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"CreateMoment","upload":true,"durable":false,"variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"createMoment","lands":[],"result":{"successMessage":"string"}},{"client_mutation_id":false,"name":"destroy","root_shape":"destroy","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"DeleteMoment","upload":false,"durable":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"like","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"LikeMoment","upload":false,"durable":false,"variables":{"momentId":"id"},"optimistic":{"likedByMe":true},"refetch":["feed"],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"share","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ShareMoment","upload":false,"durable":false,"variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"shareMoment.moment","lands":[],"result":{"copies":"int","successMessage":"string"}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"NestedSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"phase","kind":{"type":"enum","values":["draft","live"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedSamplePhase","merge":null},{"name":"value","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null,"fields":[{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"note","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"nullableDefault","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":"default","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ownerId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"owner_id","app_type":null,"merge":null},{"name":"count","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":2,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratio","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":1.5,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"enabled","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"state","kind":{"type":"enum","values":["ready","done"]},"nullable":false,"optional":false,"default":"ready","indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueState","merge":null},{"name":"labels","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"counts","kind":{"type":"list","element":"int"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratios","kind":{"type":"list","element":"num"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"flags","kind":{"type":"list","element":"bool"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"dates","kind":{"type":"list","element":"date"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"owners","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"payload","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"rawValue","kind":{"type":"raw"},"nullable":false,"optional":false,"default":{"free":[1,true,null]},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"preferences","kind":{"type":"object"},"nullable":false,"optional":false,"default":{"label":"base"},"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null,"fields":[{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":true,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"presets","kind":{"type":"array"},"nullable":false,"optional":false,"default":[{"label":"first"}],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null},{"name":"metadata","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"child","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"horizontal","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":"x","app_type":null,"merge":null}]},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null}]},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null},{"name":"tree","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null,"fields":[{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null}]},{"name":"replyTo","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordReplyNode","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordUserRef","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fullName","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"username","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatarUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedMedia","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":"","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"NestedReference","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"Notice","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"occurredAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"recent","updates":[{"subscription":"BulletinAdded","deleted_by":null,"set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"occurredAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"InteractionNotice","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"recordId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"actorUserId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"occurredAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"recent","updates":[{"subscription":"RecordSignal","deleted_by":null,"set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"occurredAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"ChatAction","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"by_chat","updates":[{"subscription":"ChatActionSignal","deleted_by":null,"deleted_unless":"active","set":{},"removes":false,"debounce":null,"root":null,"id":["chatId","userId"],"apply":{"effect":"upsert","matches":{"kind":["RECORDING_AUDIO","RECORDING_VIDEO","TYPING","UPLOADING_PHOTO","UPLOADING_VIDEO"]}}}],"by_columns":["chatId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"8.seconds","listens":[]},{"key":"ChatActivityNotice","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"kind","direction":"asc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[{"subscription":"ChatActionSignal","deleted_by":null,"deleted_unless":"active","set":{},"removes":false,"debounce":null,"root":null,"id":["chatId","userId"],"apply":{"effect":"upsert","matches":{"kind":["SCREENSHOT"]}}}]},{"key":"Presence","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"room","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"user","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"for_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"online","updates":[{"subscription":"PresenceChanged","deleted_by":"left","set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":{"operation":"PresenceOnline","shape":{"type":"list","coverage":"complete"},"stale_time":"30.seconds","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"preload":{}},{"name":"on_channel","by_columns":[],"text_params":["channel"],"remote":{"operation":"PresenceOnChannel","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"preload":{"channel":["lobby","stage"]},"preload_order":["channel"]},{"name":"history","by_columns":[],"text_params":["presenceId"],"remote":{"operation":"PresenceHistory","shape":{"type":"list","coverage":"complete"},"stale_time":"12.hours","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"preload":{"presenceId":{"from":{"model":"Presence","relation":"online"}}},"preload_limit":40,"preload_order":["presenceId"]}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"Record","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"containerId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"authorId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"regionId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"sequenceNumber","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"reactions","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordReactionNode","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"user.id","app_type":null,"merge":null},{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"link":{"model":"User","id":"userId"},"app_type":"RecordReactionUser","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"attachment","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordAttachment","merge":null,"fields":[{"name":"reward","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RewardAttachment","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tier","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"rewardTier","app_type":null,"merge":null},{"name":"status","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tags","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"moment","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentAttachment","merge":null,"fields":[{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"caption","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"profile","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ProfileAttachment","merge":null,"fields":[{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"AttachmentUser","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"reaction","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ReactionAttachment","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordMedia","merge":{"type":"keys","keys":{"fileUrl":{"type":"non_empty"}}},"fields":[{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"transcoded","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}],"associations":[{"name":"container","kind":"belongs_to","target":"Container","foreign_key":"containerId","dependent":null,"touches_parent":false,"sets":{"lastRecordAt":"createdAt","lastRecordId":"id"},"newer":[{"child":"createdAt","parent":"lastRecordAt"}],"from":null}],"relations":[{"name":"by_container","by_columns":["containerId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"asc"}]},"joins_events":false,"joins_created":false},{"name":"thread","by_columns":["containerId"],"text_params":[],"remote":{"operation":"ContainerThread","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"container.records"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc_nulls_last"},{"column":"createdAt","direction":"asc"}]},"joins_events":true,"joins_created":false,"order_stable":true},{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"RecordNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"transcode_status","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"RecordTranscodeStatus","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"container_media","by_columns":["containerId"],"text_params":[],"remote":{"operation":"ContainerMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"node.records"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"history","by_columns":[],"text_params":[],"remote":{"operation":"ContainerRecords","shape":{"type":"connection","direction":"backward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"1.minute","empty_stale_time":"10.seconds","selects_page_info":true,"page_size":30,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateRecord","upload":false,"durable":true,"variables":{"attachedUserId":"attachedUserId","containerId":"containerId","signedId":"signedId","text":"text"},"optimistic":{},"refetch":[],"args":{"attachedUserId":"id?","signedId":"string?"},"local":["authorId","regionId"],"local_from":{"authorId":{"row":{"model":"CurrentUser","field":"id"}},"regionId":{"row":{"model":"CurrentRegion","field":"id"}}},"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"react","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"RecordReact","upload":false,"durable":false,"variables":{"recordId":"id"},"optimistic":{"reactions":{"${'$'}keyed":{"by":"userId","element":{"body":{"${'$'}arg":"reactionBody"},"userId":{"${'$'}arg":"userId"}}}}},"refetch":[],"args":{"reactionBody":"string","userId":"id"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"RECORD","relation":"observed","parameter":null,"observed":true}]},{"key":"CurrentUser","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"current","by_columns":[],"text_params":[],"remote":{"operation":"CurrentUserQuery","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"CurrentRegion","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"current","by_columns":[],"text_params":[],"remote":{"operation":"CurrentRegionQuery","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"CustomScreen","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"version","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"requires","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenRequires","merge":null,"fields":[{"name":"ios","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"android","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"spec","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenSpec","merge":null,"fields":[{"name":"root","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"state","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateEntry","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"raw"},"parse_json":true,"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"elements","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenElement","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"type","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slot","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"props","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"visible","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"press","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"change","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"padding","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"margin","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"background","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"radius","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"expand","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"align","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"accessibilityLabel","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ScreenNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"StorageRow","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"record","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Tail","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"since","by_columns":[],"text_params":["containerId","afterSequence","pinned"],"scalar_types":{"afterSequence":"int","pinned":"bool"},"remote":{"operation":"tailSince","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":50,"variables":{},"root":"tailSince"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"UnionSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"attachment","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordAttachment","merge":null},{"name":"props","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentWidgetItemProps","merge":null,"fields":[{"name":"emoji","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentEmojiProps","merge":null,"fields":[{"name":"emoji","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"label","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentLabelProps","merge":null,"fields":[{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"size","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"User","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatar","kind":{"type":"file"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"birthday","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"profile","updates":[{"subscription":"UserTyping","deleted_by":null,"set":{"typing":true},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":{"operation":"UserProfile","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"container_members","by_columns":[],"text_params":["containerId"],"remote":{"operation":"ContainerMembers","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"node.members","total_from":"node.membersCount"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"complete_members","by_columns":[],"text_params":["containerId"],"remote":{"operation":"ContainerMembers","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":100,"variables":{},"root":"node.members","total_from":"node.membersCount"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"members_complete":true}],"indexes":[],"actions":[],"ephemeral":[{"name":"typing","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":"5.seconds"},{"name":"mood","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":null}],"storage":"disk","expire_after":null,"listens":[]},{"key":"Vibe","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"enum","values":["like","love","laugh"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"VibeKind","merge":null}],"associations":[],"relations":[],"indexes":[["userId"]],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]}]"""

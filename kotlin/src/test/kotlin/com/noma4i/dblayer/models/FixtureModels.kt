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
        fun find(id: String) = Query("Asset", this).find(id)
        fun where(condition: Condition) = Query("Asset", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Asset", this, ids)
        fun entity(id: String) = entityOf("Asset", this, id)
        suspend fun create(clientKey: String, size: Long, position: Long? = null): Asset = offCaller {
            val values = buildJsonObject {
                put("clientKey", jsonValue(clientKey))
                put("size", jsonValue(size))
                position?.let { put("position", jsonValue(it)) }
            }
            read(DBlayer.database.create("Asset", values.jsonText(), "{}"))
        }
    }
    suspend fun update(clientKey: String? = null, size: Long? = null, position: Long? = null): Asset = offCaller {
        val values = buildJsonObject {
            clientKey?.let { put("clientKey", jsonValue(it)) }
            size?.let { put("size", jsonValue(it)) }
            position?.let { put("position", jsonValue(it)) }
        }
        Asset.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class CloudDraft(
    override val id: String,
    val title: String,
    val pinned: Boolean,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "CloudDraft"
    companion object : ModelDecoder<CloudDraft> {
        override val layout = "843523811e218b77097c60857b36db1da6c1c70a61a681db801de9c1cafc01d4"
        override val modelName = "CloudDraft"
        override fun decode(row: WireRow): CloudDraft {
            val r = CellReader(row.cells)
            return CloudDraft(
                id = row.id,
                title = r.req(0, Cell::asText),
                pinned = r.req(1, Cell::asBool),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val title = Column<CloudDraft, String>("title")
        val pinned = Column<CloudDraft, Boolean>("pinned")
        fun find(id: String) = Query("CloudDraft", this).find(id)
        fun where(condition: Condition) = Query("CloudDraft", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("CloudDraft", this, ids)
        fun entity(id: String) = entityOf("CloudDraft", this, id)
        suspend fun create(title: String, pinned: Boolean? = null): CloudDraft = offCaller {
            val values = buildJsonObject {
                put("title", jsonValue(title))
                pinned?.let { put("pinned", jsonValue(it)) }
            }
            read(DBlayer.database.create("CloudDraft", values.jsonText(), "{}"))
        }
    }
    suspend fun update(title: String? = null, pinned: Boolean? = null): CloudDraft = offCaller {
        val values = buildJsonObject {
            title?.let { put("title", jsonValue(it)) }
            pinned?.let { put("pinned", jsonValue(it)) }
        }
        CloudDraft.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class CloudCursor(
    override val id: String,
    val chatId: String,
    val readUntil: Long? = null,
    val scroll: Long,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "CloudCursor"
    companion object : ModelDecoder<CloudCursor> {
        override val layout = "38a702c7eb0eea2163cdf879fb6b3557e2d9619f5055cf04a7b44219eb282c71"
        override val modelName = "CloudCursor"
        override fun decode(row: WireRow): CloudCursor {
            val r = CellReader(row.cells)
            return CloudCursor(
                id = row.id,
                chatId = r.req(0, Cell::asId),
                readUntil = r.opt(1, Cell::asInt),
                scroll = r.req(2, Cell::asInt),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val chatId = Column<CloudCursor, String>("chatId")
        val readUntil = Column<CloudCursor, Long?>("readUntil")
        val scroll = Column<CloudCursor, Long>("scroll")
        fun find(id: String) = Query("CloudCursor", this).find(id)
        fun where(condition: Condition) = Query("CloudCursor", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("CloudCursor", this, ids)
        fun entity(id: String) = entityOf("CloudCursor", this, id)
        suspend fun create(chatId: String, readUntil: Long? = null, scroll: Long? = null): CloudCursor = offCaller {
            val values = buildJsonObject {
                put("chatId", jsonValue(chatId))
                readUntil?.let { put("readUntil", jsonValue(it)) }
                scroll?.let { put("scroll", jsonValue(it)) }
            }
            read(DBlayer.database.create("CloudCursor", values.jsonText(), "{}"))
        }
    }
    suspend fun update(chatId: String? = null, readUntil: GraphQLNullable<Long> = GraphQLNullable.Omitted, scroll: Long? = null): CloudCursor = offCaller {
        val values = buildJsonObject {
            chatId?.let { put("chatId", jsonValue(it)) }
            put("readUntil", readUntil)
            scroll?.let { put("scroll", jsonValue(it)) }
        }
        CloudCursor.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class CloudNote(
    override val id: String,
    val title: String,
    val tone: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "CloudNote"
    companion object : ModelDecoder<CloudNote> {
        override val layout = "fb792a558e5e3d997c3956ee988b6a08da3ed5a588fac9bd182fe533f1b216a1"
        override val modelName = "CloudNote"
        override fun decode(row: WireRow): CloudNote {
            val r = CellReader(row.cells)
            return CloudNote(
                id = row.id,
                title = r.req(0, Cell::asText),
                tone = r.opt(1, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val title = Column<CloudNote, String>("title")
        val tone = Column<CloudNote, String?>("tone")
        fun find(id: String) = Query("CloudNote", this).find(id)
        fun where(condition: Condition) = Query("CloudNote", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("CloudNote", this, ids)
        fun entity(id: String) = entityOf("CloudNote", this, id)
        suspend fun create(title: String): CloudNote = offCaller {
            val values = buildJsonObject {
                put("title", jsonValue(title))
            }
            read(DBlayer.database.create("CloudNote", values.jsonText(), "{}"))
        }
    }
    suspend fun update(title: String? = null): CloudNote = offCaller {
        val values = buildJsonObject {
            title?.let { put("title", jsonValue(it)) }
        }
        CloudNote.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

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
        val events: kotlinx.coroutines.flow.Flow<ModelEventItem<Container>> get() = modelEvents()
        fun find(id: String) = Query("Container", this).find(id)
        fun where(condition: Condition) = Query("Container", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Container", this, ids)
        fun entity(id: String) = entityOf("Container", this, id)
        suspend fun node(id: String, force: Boolean = false): Container? = offCaller { DBlayer.database.node("Container", "node", id, force)?.let { read(it) } }
        suspend fun nodes(ids: List<String>): List<Container> = offCaller { DBlayer.database.nodes("Container", "nodes", ids).map { read(it) } }
        val recent: Query<Container> get() {
            var built = Query("Container", this, relation = "recent", params = buildJsonObject {  })
            built = built.order(Order("lastRecordAt", "desc"))
            return built
        }
        suspend fun create(title: String, lastRecordAt: String? = null, lastRecordId: String? = null, shareUrl: String? = null, isPublic: Boolean? = null, shelves: List<ContainerShelf>? = null): Container = offCaller {
            val values = buildJsonObject {
                put("title", jsonValue(title))
                lastRecordAt?.let { put("lastRecordAt", jsonValue(it)) }
                lastRecordId?.let { put("lastRecordId", jsonValue(it)) }
                shareUrl?.let { put("shareUrl", jsonValue(it)) }
                isPublic?.let { put("isPublic", jsonValue(it)) }
                shelves?.let { put("shelves", jsonValue(it)) }
            }
            read(DBlayer.database.create("Container", values.jsonText(), "{}"))
        }
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
    val records get() = related("records", Record)
    val lastRecord get() = related("lastRecord", Record)
    object Relations {
        val records = Association<Container, Record>("records", Record)
        val lastRecord = Association<Container, Record>("lastRecord", Record)
    }
}

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
        fun find(id: String) = Query("Document", this).find(id)
        fun where(condition: Condition) = Query("Document", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Document", this, ids)
        fun entity(id: String) = entityOf("Document", this, id)
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
    }
    suspend fun update(slug: String? = null): Document = offCaller {
        val values = buildJsonObject {
            slug?.let { put("slug", jsonValue(it)) }
        }
        Document.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
}

@Immutable
data class Member(
    override val id: String,
    val name: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Member"
    companion object : ModelDecoder<Member> {
        override val layout = "daaaa5a3ca91ab534db47731be1fca27d87f8dd345d457bfd43bf77767339316"
        override val modelName = "Member"
        override fun decode(row: WireRow): Member {
            val r = CellReader(row.cells)
            return Member(
                id = row.id,
                name = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val name = Column<Member, String>("name")
        fun find(id: String) = Query("Member", this).find(id)
        fun where(condition: Condition) = Query("Member", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Member", this, ids)
        fun entity(id: String) = entityOf("Member", this, id)
        val blocked: Query<Member> get() {
            var built = Query("Member", this, relation = "blocked", params = buildJsonObject {  })
            return built
        }
        fun roster(groupId: String): Query<Member> {
            var built = Query("Member", this, relation = "roster", params = buildJsonObject { put("groupId", jsonValue(groupId)) })
            return built
        }
        suspend fun create(name: String): Member = offCaller {
            val values = buildJsonObject {
                put("name", jsonValue(name))
            }
            read(DBlayer.database.create("Member", values.jsonText(), "{}"))
        }
    }
    suspend fun update(name: String? = null): Member = offCaller {
        val values = buildJsonObject {
            name?.let { put("name", jsonValue(it)) }
        }
        Member.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
    suspend fun block(): Member = act("block", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf()), Member)
    suspend fun unblock(): Member = act("unblock", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf()), Member)
    suspend fun sendGift(giftId: String, groupId: String, price: Long? = null): ActionAnswer<Member, MemberSendGiftResult> = actAnswering("send_gift", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf("giftId" to jsonValue(giftId), "groupId" to jsonValue(groupId), "price" to jsonValue(price))), Member, MemberSendGiftResult.serializer())
}

@Immutable
@Serializable
data class MemberSendGiftResult(val successMessage: String? = null)

@Immutable
data class Group(
    override val id: String,
    val title: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Group"
    companion object : ModelDecoder<Group> {
        override val layout = "e9226eea266088346d0e13a891650ac9945faf4f4ff01bfa2daed4a6ed5e8092"
        override val modelName = "Group"
        override fun decode(row: WireRow): Group {
            val r = CellReader(row.cells)
            return Group(
                id = row.id,
                title = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val title = Column<Group, String>("title")
        fun find(id: String) = Query("Group", this).find(id)
        fun where(condition: Condition) = Query("Group", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Group", this, ids)
        fun entity(id: String) = entityOf("Group", this, id)
        fun open(folder: String): Query<Group> {
            var built = Query("Group", this, relation = "open", params = buildJsonObject { put("folder", jsonValue(folder)) })
            return built
        }
        suspend fun create(title: String): Group = offCaller {
            val values = buildJsonObject {
                put("title", jsonValue(title))
            }
            read(DBlayer.database.create("Group", values.jsonText(), "{}"))
        }
    }
    suspend fun update(title: String? = null): Group = offCaller {
        val values = buildJsonObject {
            title?.let { put("title", jsonValue(it)) }
        }
        Group.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
    suspend fun leave(): Group = act("leave", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf()), Group)
    suspend fun addMembers(memberIds: List<String>): Group = act("add_members", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf("memberIds" to jsonValue(memberIds))), Group)
    suspend fun removeMembers(memberIds: List<String>): Group = act("remove_members", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf("memberIds" to jsonValue(memberIds))), Group)
}

@Immutable
data class Post(
    override val id: String,
    val groupId: String,
    val body: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Post"
    companion object : ModelDecoder<Post> {
        override val layout = "9c21e8c0ae85a0915b63d54dbbb104c7c97c290f2efb93480da505b8dcd2b9f0"
        override val modelName = "Post"
        override fun decode(row: WireRow): Post {
            val r = CellReader(row.cells)
            return Post(
                id = row.id,
                groupId = r.req(0, Cell::asId),
                body = r.opt(1, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val groupId = Column<Post, String>("groupId")
        val body = Column<Post, String?>("body")
        fun find(id: String) = Query("Post", this).find(id)
        fun where(condition: Condition) = Query("Post", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Post", this, ids)
        fun entity(id: String) = entityOf("Post", this, id)
        fun thread(groupId: String): Query<Post> {
            var built = Query("Post", this, relation = "thread", params = buildJsonObject { put("groupId", jsonValue(groupId)) })
            return built
        }
        suspend fun create(groupId: String, body: String? = null): Post = offCaller {
            val values = buildJsonObject {
                put("groupId", jsonValue(groupId))
                body?.let { put("body", jsonValue(it)) }
            }
            read(DBlayer.database.create("Post", values.jsonText(), "{}"))
        }
    }
    suspend fun update(groupId: String? = null, body: GraphQLNullable<String> = GraphQLNullable.Omitted): Post = offCaller {
        val values = buildJsonObject {
            groupId?.let { put("groupId", jsonValue(it)) }
            put("body", body)
        }
        Post.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
    suspend fun forward(targetGroupId: String): Post = act("forward", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf("targetGroupId" to jsonValue(targetGroupId))), Post)
}

@Immutable
data class Purse(
    override val id: String,
    val balance: Long,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Purse"
    companion object : ModelDecoder<Purse> {
        override val layout = "f1120547156d9a9347d141deca8c3161b3f2ea808877f5c2116ffeb624fc8159"
        override val modelName = "Purse"
        override fun decode(row: WireRow): Purse {
            val r = CellReader(row.cells)
            return Purse(
                id = row.id,
                balance = r.req(0, Cell::asInt),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val balance = Column<Purse, Long>("balance")
        fun find(id: String) = Query("Purse", this).find(id)
        fun where(condition: Condition) = Query("Purse", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Purse", this, ids)
        fun entity(id: String) = entityOf("Purse", this, id)
        val current: Query<Purse> get() {
            var built = Query("Purse", this, relation = "current", params = buildJsonObject {  })
            return built
        }
        fun entity() = singletonEntityOf("Purse", this, current)
        suspend fun create(balance: Long): Purse = offCaller {
            val values = buildJsonObject {
                put("balance", jsonValue(balance))
            }
            read(DBlayer.database.create("Purse", values.jsonText(), "{}"))
        }
    }
    suspend fun update(balance: Long? = null): Purse = offCaller {
        val values = buildJsonObject {
            balance?.let { put("balance", jsonValue(it)) }
        }
        Purse.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
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
        fun find(id: String) = Query("InterfaceRecord", this).find(id)
        fun where(condition: Condition) = Query("InterfaceRecord", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("InterfaceRecord", this, ids)
        fun entity(id: String) = entityOf("InterfaceRecord", this, id)
        suspend fun node(id: String, force: Boolean = false): InterfaceRecord? = offCaller { DBlayer.database.node("InterfaceRecord", "node", id, force)?.let { read(it) } }
        suspend fun create(slug: String, spec: SampleSpec? = null): InterfaceRecord = offCaller {
            val values = buildJsonObject {
                put("slug", jsonValue(slug))
                spec?.let { put("spec", jsonValue(it)) }
            }
            read(DBlayer.database.create("InterfaceRecord", values.jsonText(), "{}"))
        }
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
        fun find(id: String) = Query("KeywordRecord", this).find(id)
        fun where(condition: Condition) = Query("KeywordRecord", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("KeywordRecord", this, ids)
        fun entity(id: String) = entityOf("KeywordRecord", this, id)
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
        fun find(id: String) = Query("Moment", this).find(id)
        fun where(condition: Condition) = Query("Moment", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Moment", this, ids)
        fun entity(id: String) = entityOf("Moment", this, id)
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
        suspend fun create(text: String? = null, media: JsonElement? = null): Moment = offCaller {
            val values = buildJsonObject {
                text?.let { put("text", jsonValue(it)) }
                media?.let { put("media", jsonValue(it)) }
            }
            read(DBlayer.database.create("Moment", values.jsonText(), "{}"))
        }
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
    val author get() = related("author", User)
    val vibes get() = related("vibes", Vibe)
    object Relations {
        val author = Association<Moment, User>("author", User)
        val vibes = Association<Moment, Vibe>("vibes", Vibe)
    }
}

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
        fun find(id: String) = Query("NestedSample", this).find(id)
        fun where(condition: Condition) = Query("NestedSample", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("NestedSample", this, ids)
        fun entity(id: String) = entityOf("NestedSample", this, id)
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
        fun find(id: String) = Query("NestedReference", this).find(id)
        fun where(condition: Condition) = Query("NestedReference", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("NestedReference", this, ids)
        fun entity(id: String) = entityOf("NestedReference", this, id)
        suspend fun create(values: List<NestedValue>? = null): NestedReference = offCaller {
            val values_ = buildJsonObject {
                values?.let { put("values", jsonValue(it)) }
            }
            read(DBlayer.database.create("NestedReference", values_.jsonText(), "{}"))
        }
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
        fun find(id: String) = Query("Notice", this).find(id)
        fun where(condition: Condition) = Query("Notice", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Notice", this, ids)
        fun entity(id: String) = entityOf("Notice", this, id)
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
        fun find(id: String) = Query("InteractionNotice", this).find(id)
        fun where(condition: Condition) = Query("InteractionNotice", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("InteractionNotice", this, ids)
        fun entity(id: String) = entityOf("InteractionNotice", this, id)
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
    }
    suspend fun update(userId: String? = null, room: String? = null): Presence = offCaller {
        val values = buildJsonObject {
            userId?.let { put("userId", jsonValue(it)) }
            room?.let { put("room", jsonValue(it)) }
        }
        Presence.read(DBlayer.database.update(modelName, id, values.jsonText(), "{}"))
    }
    val user get() = related("user", User)
    object Relations {
        val user = Association<Presence, User>("user", User)
    }
}

@Immutable
data class Record(
    override val id: String,
    val containerId: String,
    val authorId: String,
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
        override val layout = "6c634a59c604e66ca9f6ee45264c4b80afeb809a977aa165bddc0e3ba65aa767"
        override val modelName = "Record"
        override fun decode(row: WireRow): Record {
            val r = CellReader(row.cells)
            return Record(
                id = row.id,
                containerId = r.req(0, Cell::asId),
                authorId = r.req(1, Cell::asId),
                text = r.req(2, Cell::asText),
                createdAt = r.req(3, Cell::asText),
                sequenceNumber = r.opt(4, Cell::asInt),
                reactions = r.arr(5) { RecordReactionNode.fromCells(it) },
                attachment = r.objOpt(6) { RecordAttachment.fromCells(it) },
                media = r.objOpt(7) { RecordMedia.fromCells(it) },
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val containerId = Column<Record, String>("containerId")
        val authorId = Column<Record, String>("authorId")
        val text = Column<Record, String>("text")
        val createdAt = Column<Record, String>("createdAt")
        val sequenceNumber = Column<Record, Long?>("sequenceNumber")
        val reactions = Column<Record, List<RecordReactionNode>>("reactions")
        val attachment = Column<Record, RecordAttachmentKind>("attachment")
        val media = Column<Record, RecordMedia?>("media")
        val events: kotlinx.coroutines.flow.Flow<ModelEventItem<Record>> get() = modelEvents()
        fun find(id: String) = Query("Record", this).find(id)
        fun where(condition: Condition) = Query("Record", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Record", this, ids)
        fun entity(id: String) = entityOf("Record", this, id)
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
        suspend fun transcodeStatus(id: String, force: Boolean = false): Record? = offCaller { DBlayer.database.node("Record", "transcode_status", id, force)?.let { read(it) } }
        fun containerMedia(containerId: String): Query<Record> {
            var built = Query("Record", this, relation = "container_media", params = buildJsonObject { put("containerId", jsonValue(containerId)) })
            return built
        }
        val history: Query<Record> get() {
            var built = Query("Record", this, relation = "history", params = buildJsonObject {  })
            return built
        }
        suspend fun create(containerId: String, authorId: String, text: String, attachedUserId: String? = null, signedId: String? = null): Record = offCaller {
            val values = buildJsonObject {
                put("containerId", jsonValue(containerId))
                put("authorId", jsonValue(authorId))
                put("text", jsonValue(text))
            }
            val args = buildJsonObject {
                attachedUserId?.let { put("attachedUserId", jsonValue(it)) }
                signedId?.let { put("signedId", jsonValue(it)) }
            }
            read(DBlayer.database.create("Record", values.jsonText(), args.jsonText()))
        }
    }
    suspend fun update(containerId: String? = null, authorId: String? = null, text: String? = null, createdAt: String? = null, sequenceNumber: GraphQLNullable<Long> = GraphQLNullable.Omitted, reactions: List<RecordReactionNode>? = null, attachment: GraphQLNullable<RecordAttachment> = GraphQLNullable.Omitted, media: GraphQLNullable<RecordMedia> = GraphQLNullable.Omitted): Record = offCaller {
        val values = buildJsonObject {
            containerId?.let { put("containerId", jsonValue(it)) }
            authorId?.let { put("authorId", jsonValue(it)) }
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
    val container get() = related("container", Container)
    object Relations {
        val container = Association<Record, Container>("container", Container)
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
        fun find(id: String) = Query("CustomScreen", this).find(id)
        fun where(condition: Condition) = Query("CustomScreen", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("CustomScreen", this, ids)
        fun entity(id: String) = entityOf("CustomScreen", this, id)
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
        fun find(id: String) = Query("StorageRow", this).find(id)
        fun where(condition: Condition) = Query("StorageRow", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("StorageRow", this, ids)
        fun entity(id: String) = entityOf("StorageRow", this, id)
        suspend fun create(record: String): StorageRow = offCaller {
            val values = buildJsonObject {
                put("record", jsonValue(record))
            }
            read(DBlayer.database.create("StorageRow", values.jsonText(), "{}"))
        }
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
        fun find(id: String) = Query("Tail", this).find(id)
        fun where(condition: Condition) = Query("Tail", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Tail", this, ids)
        fun entity(id: String) = entityOf("Tail", this, id)
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
        fun find(id: String) = Query("UnionSample", this).find(id)
        fun where(condition: Condition) = Query("UnionSample", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("UnionSample", this, ids)
        fun entity(id: String) = entityOf("UnionSample", this, id)
        suspend fun create(label: String? = null, attachment: RecordAttachment? = null, props: MomentWidgetItemProps? = null): UnionSample = offCaller {
            val values = buildJsonObject {
                label?.let { put("label", jsonValue(it)) }
                attachment?.let { put("attachment", jsonValue(it)) }
                props?.let { put("props", jsonValue(it)) }
            }
            read(DBlayer.database.create("UnionSample", values.jsonText(), "{}"))
        }
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
        fun find(id: String) = Query("User", this).find(id)
        fun where(condition: Condition) = Query("User", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("User", this, ids)
        fun entity(id: String) = entityOf("User", this, id)
        val profile: Query<User> get() {
            var built = Query("User", this, relation = "profile", params = buildJsonObject {  })
            return built
        }
        fun containerMembers(containerId: String): Query<User> {
            var built = Query("User", this, relation = "container_members", params = buildJsonObject { put("containerId", jsonValue(containerId)) })
            return built
        }
        fun entity() = singletonEntityOf("User", this, profile)
        suspend fun create(name: String, birthday: String? = null): User = offCaller {
            val values = buildJsonObject {
                put("name", jsonValue(name))
                birthday?.let { put("birthday", jsonValue(it)) }
            }
            read(DBlayer.database.create("User", values.jsonText(), "{}"))
        }
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
        fun find(id: String) = Query("Vibe", this).find(id)
        fun where(condition: Condition) = Query("Vibe", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Vibe", this, ids)
        fun entity(id: String) = entityOf("Vibe", this, id)
        suspend fun create(momentId: String, userId: String, kind: VibeKind): Vibe = offCaller {
            val values = buildJsonObject {
                put("momentId", jsonValue(momentId))
                put("userId", jsonValue(userId))
                put("kind", jsonValue(kind))
            }
            read(DBlayer.database.create("Vibe", values.jsonText(), "{}"))
        }
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

val modelsJSON = """[{"key":"Asset","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"clientKey","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"size","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"position","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create_file","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"CreateAsset","upload":true,"variables":{"assetId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":["clientKey"],"ignore_response":false,"wrap":null,"root":"createAsset.asset","lands":[],"result":{}},{"batch":{"min":2,"max":10,"items":"items","single":"create_file","input":{"clientKey":"clientKey"},"renumber":{"position":"position","size":"size","position_from":0}},"client_mutation_id":false,"name":"create_files","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"CreateAssets","upload":true,"variables":{"assetId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":["clientKey"],"ignore_response":false,"wrap":null,"root":"createAssets.assets","lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"CloudDraft","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"pinned","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[],"cloud":["title","pinned"]},{"key":"CloudCursor","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"readUntil","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"scroll","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[],"cloud":["chatId","readUntil"]},{"key":"CloudNote","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tone","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":{"type":"local"}}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"cloud":["tone"]},{"key":"Container","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastRecordAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastRecordId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"shareUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"isPublic","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"shelves","kind":{"type":"list","element":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ContainerShelf","values":["PRIMARY","SECONDARY"],"merge":null}],"associations":[{"name":"records","kind":"has_many","target":"Record","foreign_key":"containerId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null},{"name":"lastRecord","kind":"belongs_to","target":"Record","foreign_key":"lastRecordId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ContainerNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"nodes","by_columns":[],"text_params":[],"remote":{"operation":"ContainersByIds","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"node":"many"},{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"lastRecordAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateContainer","upload":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"create_photo","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ContainerGroupCreateFile","upload":true,"variables":{"containerId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"groupContainerCreate.container","lands":[],"result":{}},{"client_mutation_id":false,"name":"update_photo","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ContainerGroupUpdateFile","upload":true,"variables":{"containerId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"groupContainerUpdate.container","lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"CONTAINER","relation":"node","parameter":null,"removes_on":["DELETED","ACCESS_REVOKED"]}],"change_feed":{"subscription":"RecordEvents","root":"recordEvents.entries","area":"topic","kind":"action","id":"recordId"}},{"key":"Document","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"bySlug","updates":[{"subscription":"documentUpdated","deleted_by":null,"set":{},"removes":false,"debounce":null,"root":null,"id":null}],"by_columns":["slug"],"text_params":["source"],"remote":{"operation":"documentBySlug","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":"documentBySlug"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Member","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"blocked","by_columns":[],"text_params":[],"remote":{"operation":"BlockedMembers","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":"viewer.blockedMembers"},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"roster","by_columns":[],"text_params":["groupId"],"remote":{"operation":"GroupRoster","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":"node.members"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"block","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"MemberBlock","upload":false,"variables":{"memberId":"id"},"optimistic":{},"effects":[{"kind":"add","model":"Member","scope":"blocked"}],"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":true,"wrap":"input","root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"unblock","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"MemberUnblock","upload":false,"variables":{"memberId":"id"},"optimistic":{},"effects":[{"kind":"remove","model":"Member","scope":"blocked"}],"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":true,"wrap":"input","root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"send_gift","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"MemberGift","upload":false,"variables":{"giftId":"giftId","memberId":"id"},"optimistic":{},"effects":[{"kind":"create","model":"Post","values":{"body":{"literal":"gift"},"groupId":{"arg":"groupId"}}},{"kind":"change","model":"Purse","values":{"balance":{"minus":"price"}}}],"refetch":[],"args":{"giftId":"string","groupId":"id","price":"int?"},"local":[],"correlate":[],"ignore_response":false,"wrap":"input","root":"memberGift.member","lands":[{"model":"Post","root":"memberGift.post"}],"result":{"successMessage":"string"}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Group","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"open","by_columns":[],"text_params":["folder"],"remote":{"operation":"OpenGroups","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":"viewer.groups"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"leave","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"GroupLeave","upload":false,"variables":{"groupId":"id"},"optimistic":{},"effects":[{"kind":"remove","model":"Group","scope":"open"}],"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":true,"wrap":"input","root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"add_members","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"GroupAddMembers","upload":false,"variables":{"addMemberIds":"memberIds","groupId":"id"},"optimistic":{},"effects":[{"kind":"add","model":"Member","scope":"roster","rows":"memberIds","with":{"groupId":{"owner":"id"}}}],"refetch":[],"args":{"memberIds":"ids"},"local":[],"correlate":[],"ignore_response":true,"wrap":"input","root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"remove_members","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"GroupRemoveMembers","upload":false,"variables":{"groupId":"id","removeMemberIds":"memberIds"},"optimistic":{},"effects":[{"kind":"remove","model":"Member","scope":"roster","rows":"memberIds","with":{"groupId":{"owner":"id"}}}],"refetch":[],"args":{"memberIds":"ids"},"local":[],"correlate":[],"ignore_response":true,"wrap":"input","root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Post","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"groupId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"thread","by_columns":["groupId"],"text_params":[],"remote":{"operation":"GroupThread","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":"node.posts"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[["groupId"]],"actions":[{"client_mutation_id":false,"name":"forward","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"PostForward","upload":false,"variables":{"groupId":"targetGroupId","postId":"id"},"optimistic":{},"effects":[{"kind":"create","model":"Post","values":{"body":{"owner":"body"},"groupId":{"arg":"targetGroupId"}}}],"refetch":[],"args":{"targetGroupId":"id"},"local":[],"correlate":[],"ignore_response":false,"wrap":"input","root":"postForward.post","lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Purse","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"balance","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"current","by_columns":[],"text_params":[],"remote":{"operation":"PurseQuery","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"InterfaceRecord","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"spec","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleSpec","merge":null,"fields":[{"name":"root","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"elements","kind":{"type":"union_array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleElement","merge":null,"fields":[{"name":"button","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleButton","merge":null,"fields":[{"name":"label","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextValue","merge":null,"fields":[{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null},{"name":"textLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextLiteral","merge":null}]},{"name":"disabled","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagValue","merge":null,"fields":[{"name":"flagLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagLiteral","merge":null},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null}]}]},{"name":"column","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleColumn","merge":null,"fields":[{"name":"gap","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberValue","merge":null,"fields":[{"name":"numberLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberLiteral","merge":null},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null}]},{"name":"alignItems","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"heading","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleHeading","merge":null,"fields":[{"name":"text","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextValue","merge":null},{"name":"level","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"pressable","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SamplePressable","merge":null},{"name":"spacer","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleSpacer","merge":null,"fields":[{"name":"length","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberValue","merge":null}]},{"name":"switch","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleSwitch","merge":null,"fields":[{"name":"checked","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagValue","merge":null},{"name":"label","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextValue","merge":null}]}],"shared":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"press","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleAction","merge":null,"fields":[{"name":"navigate","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNavigateAction","merge":null,"fields":[{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openRecord","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOpenRecordAction","merge":null,"fields":[{"name":"recordId","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openExternal","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOpenExternalAction","merge":null,"fields":[{"name":"url","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openProfile","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOpenProfileAction","merge":null,"fields":[{"name":"userId","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"visible","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibility","merge":null,"fields":[{"name":"condition","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleCondition","merge":null,"fields":[{"name":"comparison","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"negated","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"operand","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOperand","merge":null,"fields":[{"name":"flagLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagLiteral","merge":null,"fields":[{"name":"flag","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"numberLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberLiteral","merge":null,"fields":[{"name":"number","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"textLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextLiteral","merge":null,"fields":[{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"visibleAll","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibleAll","merge":null,"fields":[{"name":"conditions","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleCondition","merge":null}]},{"name":"visibleAny","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibleAny","merge":null,"fields":[{"name":"conditions","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleCondition","merge":null}]},{"name":"visibleFlag","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibleFlag","merge":null,"fields":[{"name":"flag","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}]},{"name":"state","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateEntry","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"union"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateValue","merge":null,"fields":[{"name":"flag","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagLiteral","merge":null},{"name":"number","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberLiteral","merge":null},{"name":"text","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextLiteral","merge":null}]}]}]}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"SampleNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"KeywordRecord","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"default","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"class","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"KeywordValue","merge":null,"fields":[{"name":"default","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"when","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"class","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"container","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"r","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"child","kind":{"type":"object"},"nullable":false,"optional":false,"default":{"class":"c","default":"d","when":"w"},"indexed":false,"id_from_parent":null,"from":null,"app_type":"KeywordChild","merge":null,"fields":[{"name":"default","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"when","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"class","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[{"name":"when","by_columns":["class"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"uuid","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"searchable":true},{"name":"likedByMe","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"media","kind":{"type":"raw"},"nullable":false,"optional":false,"default":{},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tone","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":{"type":"local"}},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"author","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":true,"sets":{},"newer":[],"from":null},{"name":"vibes","kind":"has_many","target":"Vibe","foreign_key":"momentId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"transcode_status","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"MomentTranscodeStatus","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"momentId":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"by_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"feed","updates":[{"subscription":"MomentEvents","deleted_by":"destroyed","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"impressionUuid","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"impressionScore","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"vibe_feed","by_columns":[],"text_params":["vibeId"],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"impressionUuid","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"score","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"impressionScore","app_type":null,"merge":null},{"name":"rank","kind":{"type":"enum","values":["top","normal"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"FeedRank","merge":null}]}],"indexes":[["uuid"],["userId","createdAt"]],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateMoment","upload":false,"variables":{"media":"media","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"update","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"UpdateMoment","upload":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"upload_clip","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"CreateMoment","upload":true,"variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"createMoment","lands":[],"result":{}},{"client_mutation_id":false,"name":"destroy","root_shape":"destroy","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"DeleteMoment","upload":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"like","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"LikeMoment","upload":false,"variables":{"momentId":"id"},"optimistic":{"likedByMe":true},"refetch":["feed"],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"share","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ShareMoment","upload":false,"variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"shareMoment.moment","lands":[],"result":{"copies":"int","successMessage":"string"}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"NestedSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"phase","kind":{"type":"enum","values":["draft","live"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedSamplePhase","merge":null},{"name":"value","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null,"fields":[{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"note","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"nullableDefault","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":"default","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ownerId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"owner_id","app_type":null,"merge":null},{"name":"count","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":2,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratio","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":1.5,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"enabled","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"state","kind":{"type":"enum","values":["ready","done"]},"nullable":false,"optional":false,"default":"ready","indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueState","merge":null},{"name":"labels","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"counts","kind":{"type":"list","element":"int"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratios","kind":{"type":"list","element":"num"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"flags","kind":{"type":"list","element":"bool"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"dates","kind":{"type":"list","element":"date"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"owners","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"payload","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"rawValue","kind":{"type":"raw"},"nullable":false,"optional":false,"default":{"free":[1,true,null]},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"preferences","kind":{"type":"object"},"nullable":false,"optional":false,"default":{"label":"base"},"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null,"fields":[{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":true,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"presets","kind":{"type":"array"},"nullable":false,"optional":false,"default":[{"label":"first"}],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null},{"name":"metadata","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"child","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"horizontal","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":"x","app_type":null,"merge":null}]},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null}]},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null},{"name":"tree","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null,"fields":[{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null}]},{"name":"replyTo","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordReplyNode","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordUserRef","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fullName","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"username","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatarUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedMedia","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":"","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"NestedReference","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"Notice","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"occurredAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"recent","updates":[{"subscription":"BulletinAdded","deleted_by":null,"set":{},"removes":false,"debounce":null,"root":null,"id":null}],"by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"occurredAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"InteractionNotice","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"recordId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"actorUserId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"occurredAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"recent","updates":[{"subscription":"RecordSignal","deleted_by":null,"set":{},"removes":false,"debounce":null,"root":null,"id":null}],"by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"occurredAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"Presence","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"room","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"user","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"for_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"online","updates":[{"subscription":"PresenceChanged","deleted_by":"left","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"by_columns":[],"text_params":[],"remote":{"operation":"PresenceOnline","shape":{"type":"list","coverage":"complete"},"stale_time":"30.seconds","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"preload":{}},{"name":"on_channel","by_columns":[],"text_params":["channel"],"remote":{"operation":"PresenceOnChannel","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"preload":{"channel":["lobby","stage"]}},{"name":"history","by_columns":[],"text_params":["presenceId"],"remote":{"operation":"PresenceHistory","shape":{"type":"list","coverage":"complete"},"stale_time":"12.hours","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"preload":{"presenceId":{"from":{"model":"Presence","relation":"online"}}}}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"Record","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"containerId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"authorId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"sequenceNumber","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"reactions","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordReactionNode","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"user.id","app_type":null,"merge":null},{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"link":{"model":"User","id":"userId"},"app_type":"RecordReactionUser","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"attachment","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordAttachment","merge":null,"fields":[{"name":"reward","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RewardAttachment","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tier","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"rewardTier","app_type":null,"merge":null},{"name":"status","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tags","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"moment","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentAttachment","merge":null,"fields":[{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"caption","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"profile","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ProfileAttachment","merge":null,"fields":[{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"AttachmentUser","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"reaction","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ReactionAttachment","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordMedia","merge":{"type":"keys","keys":{"fileUrl":{"type":"non_empty"}}},"fields":[{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"transcoded","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}],"associations":[{"name":"container","kind":"belongs_to","target":"Container","foreign_key":"containerId","dependent":null,"touches_parent":false,"sets":{"lastRecordAt":"createdAt","lastRecordId":"id"},"newer":[{"child":"createdAt","parent":"lastRecordAt"}],"from":null}],"relations":[{"name":"by_container","by_columns":["containerId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"asc"}]},"joins_events":false,"joins_created":false},{"name":"thread","by_columns":["containerId"],"text_params":[],"remote":{"operation":"ContainerThread","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"container.records"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc_nulls_last"},{"column":"createdAt","direction":"asc"}]},"joins_events":true,"joins_created":false,"order_stable":true},{"name":"transcode_status","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"RecordTranscodeStatus","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"container_media","by_columns":["containerId"],"text_params":[],"remote":{"operation":"ContainerMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"node.records"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"history","by_columns":[],"text_params":[],"remote":{"operation":"ContainerRecords","shape":{"type":"connection","direction":"backward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"1.minute","empty_stale_time":"10.seconds","selects_page_info":true,"page_size":30,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateRecord","upload":false,"variables":{"attachedUserId":"attachedUserId","containerId":"containerId","signedId":"signedId","text":"text"},"optimistic":{},"refetch":[],"args":{"attachedUserId":"id?","signedId":"string?"},"local":["authorId"],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"react","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"RecordReact","upload":false,"variables":{"recordId":"id"},"optimistic":{"reactions":{"${'$'}keyed":{"by":"userId","element":{"body":{"${'$'}arg":"reactionBody"},"userId":{"${'$'}arg":"userId"}}}}},"refetch":[],"args":{"reactionBody":"string","userId":"id"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"RECORD","relation":"observed","parameter":null,"observed":true}]},{"key":"CustomScreen","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"version","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"requires","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenRequires","merge":null,"fields":[{"name":"ios","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"android","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"spec","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenSpec","merge":null,"fields":[{"name":"root","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"state","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateEntry","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"raw"},"parse_json":true,"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"elements","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenElement","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"type","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slot","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"props","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"visible","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"press","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"change","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"padding","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"margin","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"background","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"radius","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"expand","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"align","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"accessibilityLabel","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ScreenNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"StorageRow","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"record","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Tail","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"since","by_columns":[],"text_params":["containerId","afterSequence","pinned"],"scalar_types":{"afterSequence":"int","pinned":"bool"},"remote":{"operation":"tailSince","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":50,"variables":{},"root":"tailSince"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"UnionSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"attachment","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordAttachment","merge":null},{"name":"props","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentWidgetItemProps","merge":null,"fields":[{"name":"emoji","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentEmojiProps","merge":null,"fields":[{"name":"emoji","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"label","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentLabelProps","merge":null,"fields":[{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"size","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"User","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatar","kind":{"type":"file"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"birthday","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"profile","updates":[{"subscription":"UserTyping","deleted_by":null,"set":{"typing":true},"removes":false,"debounce":null,"root":null,"id":null}],"by_columns":[],"text_params":[],"remote":{"operation":"UserProfile","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"container_members","by_columns":[],"text_params":["containerId"],"remote":{"operation":"ContainerMembers","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"node.members","total_from":"node.membersCount"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[{"name":"typing","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":"5.seconds"},{"name":"mood","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":null}],"storage":"disk","expire_after":null,"listens":[]},{"key":"Vibe","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"enum","values":["like","love","laugh"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"VibeKind","merge":null}],"associations":[],"relations":[],"indexes":[["userId"]],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]}]"""

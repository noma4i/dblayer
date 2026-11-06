package com.noma4i.dblayer.models

import androidx.compose.runtime.Immutable
import com.noma4i.dblayer.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*

@Immutable
data class Ledger(
    override val id: String,
    val scope: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Ledger"
    companion object : ModelDecoder<Ledger> {
        override val layout = "289c8b1976ed584f0fc8530e2c6a07072821088c740310c0619fbb37e245c33e"
        override val modelName = "Ledger"
        override fun decode(row: WireRow): Ledger {
            val r = CellReader(row.cells)
            return Ledger(
                id = row.id,
                scope = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val scope = Column<Ledger, String>("scope")
        fun where(condition: Condition) = Query("Ledger", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Ledger", this, ids)
        suspend fun send(account: AccountScope, scope: String): MutationReceipt<Ledger> {
            val values_ = buildJsonObject {
                this.put("scope", jsonValue(scope))
            }
            return account.send(this, values_)
        }
    }
    suspend fun update(scope: String? = null, account: AccountScope? = null): MutationReceipt<Ledger> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            scope?.let { this.put("scope", jsonValue(it)) }
        }
        MutationReceipt(database_, Ledger, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Ledger> = Ledger.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Ledger> = Ledger.destroy(id, account)
    suspend fun move(scope: String, account: AccountScope? = null): MutationReceipt<Ledger> = this.act("move", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf("scope" to jsonValue(scope))), Ledger, account)
}

@Immutable
data class LedgerFeed(
    override val id: String,
    val title: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "LedgerFeed"
    companion object : ModelDecoder<LedgerFeed> {
        override val layout = "e9226eea266088346d0e13a891650ac9945faf4f4ff01bfa2daed4a6ed5e8092"
        override val modelName = "LedgerFeed"
        override fun decode(row: WireRow): LedgerFeed {
            val r = CellReader(row.cells)
            return LedgerFeed(
                id = row.id,
                title = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val title = Column<LedgerFeed, String>("title")
        fun where(condition: Condition) = Query("LedgerFeed", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("LedgerFeed", this, ids)
        fun entries(scope: String): Query<LedgerFeed> {
            var built_ = Query("LedgerFeed", this, relation = "entries", params = buildJsonObject { this.put("scope", jsonValue(scope)) })
            return built_
        }
        suspend fun entriesEdge(scope: String, id: String, account: AccountScope? = null): LedgerFeedEntriesEdge? = LedgerFeed.entries(scope).edgeValues(id, account)?.let { LedgerFeedEntriesEdge(it) }
        suspend fun create(title: String, account: AccountScope? = null): MutationReceipt<LedgerFeed> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("title", jsonValue(title))
            }
            MutationReceipt(database_, LedgerFeed, database_.create(epoch_, "LedgerFeed", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(title: String? = null, account: AccountScope? = null): MutationReceipt<LedgerFeed> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            title?.let { this.put("title", jsonValue(it)) }
        }
        MutationReceipt(database_, LedgerFeed, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<LedgerFeed> = LedgerFeed.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<LedgerFeed> = LedgerFeed.destroy(id, account)
}

@Immutable
data class Shadow(
    override val id: String,
    val title: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Shadow"
    companion object : ModelDecoder<Shadow> {
        override val layout = "e9226eea266088346d0e13a891650ac9945faf4f4ff01bfa2daed4a6ed5e8092"
        override val modelName = "Shadow"
        override fun decode(row: WireRow): Shadow {
            val r = CellReader(row.cells)
            return Shadow(
                id = row.id,
                title = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val title = Column<Shadow, String>("title")
        fun where(condition: Condition) = Query("Shadow", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Shadow", this, ids)
        val scope: Query<Shadow> get() {
            var built_ = Query("Shadow", this, relation = "scope", params = buildJsonObject {  })
            return built_
        }
        suspend fun scopeEdge(id: String, account: AccountScope? = null): ShadowScopeEdge? = Shadow.scope.edgeValues(id, account)?.let { ShadowScopeEdge(it) }
        val account: Query<Shadow> get() {
            var built_ = Query("Shadow", this, relation = "account", params = buildJsonObject {  })
            return built_
        }
        suspend fun accountEdge(id: String, account: AccountScope? = null): ShadowAccountEdge? = Shadow.account.edgeValues(id, account)?.let { ShadowAccountEdge(it) }
        suspend fun send(account: AccountScope, title: String): MutationReceipt<Shadow> {
            val values_ = buildJsonObject {
                this.put("title", jsonValue(title))
            }
            return account.send(this, values_)
        }
    }
    suspend fun update(title: String? = null, account: AccountScope? = null): MutationReceipt<Shadow> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            title?.let { this.put("title", jsonValue(it)) }
        }
        MutationReceipt(database_, Shadow, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Shadow> = Shadow.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Shadow> = Shadow.destroy(id, account)
}

@Immutable
data class Holder(
    override val id: String,
    val label: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Holder"
    companion object : ModelDecoder<Holder> {
        override val layout = "cae70f3b4f7f7821a632570ad1a489bc597e5b848b1ccc6df664113173d98031"
        override val modelName = "Holder"
        override fun decode(row: WireRow): Holder {
            val r = CellReader(row.cells)
            return Holder(
                id = row.id,
                label = r.opt(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val label = Column<Holder, String?>("label")
        fun where(condition: Condition) = Query("Holder", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Holder", this, ids)
        val account: Query<Holder> get() {
            var built_ = Query("Holder", this, relation = "account", params = buildJsonObject {  })
            return built_
        }
        suspend fun create(label: String? = null, account: AccountScope? = null): MutationReceipt<Holder> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                label?.let { this.put("label", jsonValue(it)) }
            }
            MutationReceipt(database_, Holder, database_.create(epoch_, "Holder", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(label: GraphQLNullable<String> = GraphQLNullable.Omitted, account: AccountScope? = null): MutationReceipt<Holder> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            this.put("label", label)
        }
        MutationReceipt(database_, Holder, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Holder> = Holder.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Holder> = Holder.destroy(id, account)
}

@Immutable
data class Mailbox(
    override val id: String,
    val read: Boolean,
    val put: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Mailbox"
    companion object : ModelDecoder<Mailbox> {
        override val layout = "378773461bb198bd38dc75a714a52c5eb2749b320835a2bec565b972cc490775"
        override val modelName = "Mailbox"
        override fun decode(row: WireRow): Mailbox {
            val r = CellReader(row.cells)
            return Mailbox(
                id = row.id,
                read = r.req(0, Cell::asBool),
                put = r.opt(1, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val read = Column<Mailbox, Boolean>("read")
        val put = Column<Mailbox, String?>("put")
        fun where(condition: Condition) = Query("Mailbox", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Mailbox", this, ids)
        fun entries(act: String): Query<Mailbox> {
            var built_ = Query("Mailbox", this, relation = "entries", params = buildJsonObject { this.put("act", jsonValue(act)) })
            return built_
        }
        suspend fun entriesEdge(act: String, id: String, account: AccountScope? = null): MailboxEntriesEdge? = Mailbox.entries(act).edgeValues(id, account)?.let { MailboxEntriesEdge(it) }
        suspend fun create(read: Boolean, put: String? = null, account: AccountScope? = null): MutationReceipt<Mailbox> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("read", jsonValue(read))
                put?.let { this.put("put", jsonValue(it)) }
            }
            MutationReceipt(database_, Mailbox, database_.create(epoch_, "Mailbox", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(read: Boolean? = null, put: GraphQLNullable<String> = GraphQLNullable.Omitted, account: AccountScope? = null): MutationReceipt<Mailbox> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            read?.let { this.put("read", jsonValue(it)) }
            this.put("put", put)
        }
        MutationReceipt(database_, Mailbox, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Mailbox> = Mailbox.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Mailbox> = Mailbox.destroy(id, account)
    suspend fun mark(put: String, account: AccountScope? = null): MutationReceipt<Mailbox> = this.act("mark", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf("put" to jsonValue(put))), Mailbox, account)
}

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
        suspend fun create(clientKey: String, size: Long, position: Long? = null, account: AccountScope? = null): MutationReceipt<Asset> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("clientKey", jsonValue(clientKey))
                this.put("size", jsonValue(size))
                position?.let { this.put("position", jsonValue(it)) }
            }
            MutationReceipt(database_, Asset, database_.create(epoch_, "Asset", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(clientKey: String? = null, size: Long? = null, position: Long? = null, account: AccountScope? = null): MutationReceipt<Asset> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            clientKey?.let { this.put("clientKey", jsonValue(it)) }
            size?.let { this.put("size", jsonValue(it)) }
            position?.let { this.put("position", jsonValue(it)) }
        }
        MutationReceipt(database_, Asset, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Asset> = Asset.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Asset> = Asset.destroy(id, account)
    suspend fun createFile(account: AccountScope, request: UploadRequest): MutationReceipt<Asset> = account.upload(this, "create_file", request, Asset)
}

@Immutable
@Serializable
class AssetCreateFileResult

@Immutable
data class Club(
    override val id: String,
    val name: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Club"
    companion object : ModelDecoder<Club> {
        override val layout = "daaaa5a3ca91ab534db47731be1fca27d87f8dd345d457bfd43bf77767339316"
        override val modelName = "Club"
        override fun decode(row: WireRow): Club {
            val r = CellReader(row.cells)
            return Club(
                id = row.id,
                name = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val name = Column<Club, String>("name")
        fun where(condition: Condition) = Query("Club", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Club", this, ids)
        suspend fun create(name: String, account: AccountScope? = null): MutationReceipt<Club> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("name", jsonValue(name))
            }
            MutationReceipt(database_, Club, database_.create(epoch_, "Club", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(name: String? = null, account: AccountScope? = null): MutationReceipt<Club> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            name?.let { this.put("name", jsonValue(it)) }
        }
        MutationReceipt(database_, Club, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Club> = Club.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Club> = Club.destroy(id, account)
    suspend fun leave(account: AccountScope? = null): MutationReceipt<Club> = this.act("leave", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf()), Club, account)
    suspend fun addMember(userIds: List<String>, account: AccountScope? = null): MutationReceipt<Club> = this.act("add_member", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf("userIds" to jsonValue(userIds))), Club, account)
    suspend fun removeMember(userIds: List<String>, account: AccountScope? = null): MutationReceipt<Club> = this.act("remove_member", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf("userIds" to jsonValue(userIds))), Club, account)
}

@Immutable
data class Person(
    override val id: String,
    val name: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Person"
    companion object : ModelDecoder<Person> {
        override val layout = "daaaa5a3ca91ab534db47731be1fca27d87f8dd345d457bfd43bf77767339316"
        override val modelName = "Person"
        override fun decode(row: WireRow): Person {
            val r = CellReader(row.cells)
            return Person(
                id = row.id,
                name = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val name = Column<Person, String>("name")
        fun where(condition: Condition) = Query("Person", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Person", this, ids)
        fun clubMembers(clubId: String, order: String): Query<Person> {
            var built_ = Query("Person", this, relation = "club_members", params = buildJsonObject { this.put("clubId", jsonValue(clubId)); this.put("order", jsonValue(order)) })
            return built_
        }
        suspend fun create(name: String, account: AccountScope? = null): MutationReceipt<Person> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("name", jsonValue(name))
            }
            MutationReceipt(database_, Person, database_.create(epoch_, "Person", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(name: String? = null, account: AccountScope? = null): MutationReceipt<Person> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            name?.let { this.put("name", jsonValue(it)) }
        }
        MutationReceipt(database_, Person, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Person> = Person.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Person> = Person.destroy(id, account)
}

@Immutable
data class Letter(
    override val id: String,
    val chatId: String,
    val text: String,
    val at: Long,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "Letter"
    companion object : ModelDecoder<Letter> {
        override val layout = "c3472584e6f90c487fde5123abb7fd4d61c0787a5c6056261019759f11c496e7"
        override val modelName = "Letter"
        override fun decode(row: WireRow): Letter {
            val r = CellReader(row.cells)
            return Letter(
                id = row.id,
                chatId = r.req(0, Cell::asId),
                text = r.req(1, Cell::asText),
                at = r.req(2, Cell::asInt),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val chatId = Column<Letter, String>("chatId")
        val text = Column<Letter, String>("text")
        val at = Column<Letter, Long>("at")
        fun where(condition: Condition) = Query("Letter", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Letter", this, ids)
        fun thread(chatId: String): Query<Letter> {
            var built_ = Query("Letter", this, relation = "thread", params = buildJsonObject { this.put("chatId", jsonValue(chatId)) })
            built_ = built_.where(Condition.equal("chatId", jsonValue(chatId)))
            built_ = built_.order(Order("at", "asc"))
            return built_
        }
        suspend fun send(account: AccountScope, chatId: String, text: String, at: Long): MutationReceipt<Letter> {
            val values_ = buildJsonObject {
                this.put("chatId", jsonValue(chatId))
                this.put("text", jsonValue(text))
                this.put("at", jsonValue(at))
            }
            return account.send(this, values_)
        }
    }
    suspend fun update(chatId: String? = null, text: String? = null, at: Long? = null, account: AccountScope? = null): MutationReceipt<Letter> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            chatId?.let { this.put("chatId", jsonValue(it)) }
            text?.let { this.put("text", jsonValue(it)) }
            at?.let { this.put("at", jsonValue(it)) }
        }
        MutationReceipt(database_, Letter, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Letter> = Letter.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Letter> = Letter.destroy(id, account)
    suspend fun forward(targetChatId: String, account: AccountScope? = null): MutationReceipt<Letter> = this.act("forward", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf("targetChatId" to jsonValue(targetChatId))), Letter, account)
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
        fun where(condition: Condition) = Query("Container", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Container", this, ids)
        suspend fun node(id: String, force: Boolean = false, account: AccountScope? = null): Container? = onAccount(account) { database, epoch -> database.node(epoch, "Container", "node", id, force)?.let { Container.read(it) } }
        suspend fun nodes(ids: List<String>, account: AccountScope? = null): List<Container> = onAccount(account) { database, epoch -> database.nodes(epoch, "Container", "nodes", ids).map { Container.read(it) } }
        val recent: Query<Container> get() {
            var built_ = Query("Container", this, relation = "recent", params = buildJsonObject {  })
            built_ = built_.order(Order("lastRecordAt", "desc"))
            return built_
        }
        fun shelf(shelf: ContainerShelf): Query<Container> {
            var built_ = Query("Container", this, relation = "shelf", params = buildJsonObject { this.put("shelf", jsonValue(shelf)) })
            return built_
        }
        suspend fun send(account: AccountScope, title: String, lastRecordAt: String? = null, lastRecordId: String? = null, shareUrl: String? = null, isPublic: Boolean? = null, shelves: List<ContainerShelf>? = null): MutationReceipt<Container> {
            val values_ = buildJsonObject {
                this.put("title", jsonValue(title))
                lastRecordAt?.let { this.put("lastRecordAt", jsonValue(it)) }
                lastRecordId?.let { this.put("lastRecordId", jsonValue(it)) }
                shareUrl?.let { this.put("shareUrl", jsonValue(it)) }
                isPublic?.let { this.put("isPublic", jsonValue(it)) }
                shelves?.let { this.put("shelves", jsonValue(it)) }
            }
            return account.send(this, values_)
        }
    }
    suspend fun update(title: String? = null, lastRecordAt: GraphQLNullable<String> = GraphQLNullable.Omitted, lastRecordId: GraphQLNullable<String> = GraphQLNullable.Omitted, shareUrl: GraphQLNullable<String> = GraphQLNullable.Omitted, isPublic: GraphQLNullable<Boolean> = GraphQLNullable.Omitted, shelves: GraphQLNullable<List<ContainerShelf>> = GraphQLNullable.Omitted, account: AccountScope? = null): MutationReceipt<Container> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            title?.let { this.put("title", jsonValue(it)) }
            this.put("lastRecordAt", lastRecordAt)
            this.put("lastRecordId", lastRecordId)
            this.put("shareUrl", shareUrl)
            this.put("isPublic", isPublic)
            this.put("shelves", shelves)
        }
        MutationReceipt(database_, Container, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Container> = Container.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Container> = Container.destroy(id, account)
    suspend fun createPhoto(account: AccountScope, request: UploadRequest): MutationReceipt<Container> = account.upload(this, "create_photo", request, Container)
    suspend fun updatePhoto(account: AccountScope, request: UploadRequest): MutationReceipt<Container> = account.upload(this, "update_photo", request, Container)
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
            var built_ = Query("Document", this, relation = "bySlug", params = buildJsonObject { this.put("slug", jsonValue(slug)); this.put("source", jsonValue(source)) })
            return built_
        }
        suspend fun create(slug: String, account: AccountScope? = null): MutationReceipt<Document> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("slug", jsonValue(slug))
            }
            MutationReceipt(database_, Document, database_.create(epoch_, "Document", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(slug: String? = null, account: AccountScope? = null): MutationReceipt<Document> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            slug?.let { this.put("slug", jsonValue(it)) }
        }
        MutationReceipt(database_, Document, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Document> = Document.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Document> = Document.destroy(id, account)
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
        suspend fun node(id: String, force: Boolean = false, account: AccountScope? = null): Chat? = onAccount(account) { database, epoch -> database.node(epoch, "Chat", "node", id, force)?.let { Chat.read(it) } }
        fun list(list: String): Query<Chat> {
            var built_ = Query("Chat", this, relation = "list", params = buildJsonObject { this.put("list", jsonValue(list)) })
            return built_
        }
        suspend fun create(title: String, serverRevision: Long, lists: List<String>, account: AccountScope? = null): MutationReceipt<Chat> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("title", jsonValue(title))
                this.put("serverRevision", jsonValue(serverRevision))
                this.put("lists", jsonValue(lists))
            }
            MutationReceipt(database_, Chat, database_.create(epoch_, "Chat", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(title: String? = null, serverRevision: Long? = null, lists: List<String>? = null, account: AccountScope? = null): MutationReceipt<Chat> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            title?.let { this.put("title", jsonValue(it)) }
            serverRevision?.let { this.put("serverRevision", jsonValue(it)) }
            lists?.let { this.put("lists", jsonValue(it)) }
        }
        MutationReceipt(database_, Chat, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Chat> = Chat.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Chat> = Chat.destroy(id, account)
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
        suspend fun node(id: String, force: Boolean = false, account: AccountScope? = null): Message? = onAccount(account) { database, epoch -> database.node(epoch, "Message", "node", id, force)?.let { Message.read(it) } }
        suspend fun create(text: String, chatId: String, account: AccountScope? = null): MutationReceipt<Message> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("text", jsonValue(text))
                this.put("chatId", jsonValue(chatId))
            }
            MutationReceipt(database_, Message, database_.create(epoch_, "Message", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(text: String? = null, chatId: String? = null, account: AccountScope? = null): MutationReceipt<Message> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            text?.let { this.put("text", jsonValue(it)) }
            chatId?.let { this.put("chatId", jsonValue(it)) }
        }
        MutationReceipt(database_, Message, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Message> = Message.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Message> = Message.destroy(id, account)
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
        suspend fun node(id: String, force: Boolean = false, account: AccountScope? = null): InterfaceRecord? = onAccount(account) { database, epoch -> database.node(epoch, "InterfaceRecord", "node", id, force)?.let { InterfaceRecord.read(it) } }
        suspend fun create(slug: String, spec: SampleSpec? = null, account: AccountScope? = null): MutationReceipt<InterfaceRecord> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("slug", jsonValue(slug))
                spec?.let { this.put("spec", jsonValue(it)) }
            }
            MutationReceipt(database_, InterfaceRecord, database_.create(epoch_, "InterfaceRecord", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(slug: String? = null, spec: GraphQLNullable<SampleSpec> = GraphQLNullable.Omitted, account: AccountScope? = null): MutationReceipt<InterfaceRecord> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            slug?.let { this.put("slug", jsonValue(it)) }
            this.put("spec", spec)
        }
        MutationReceipt(database_, InterfaceRecord, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<InterfaceRecord> = InterfaceRecord.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<InterfaceRecord> = InterfaceRecord.destroy(id, account)
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
            var built_ = Query("KeywordRecord", this, relation = "when", params = buildJsonObject { this.put("class", jsonValue(`class`)) })
            built_ = built_.where(Condition.equal("class", jsonValue(`class`)))
            return built_
        }
        suspend fun create(default: String, `class`: String, value: KeywordValue? = null, account: AccountScope? = null): MutationReceipt<KeywordRecord> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("default", jsonValue(default))
                this.put("class", jsonValue(`class`))
                value?.let { this.put("value", jsonValue(it)) }
            }
            MutationReceipt(database_, KeywordRecord, database_.create(epoch_, "KeywordRecord", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(default: String? = null, `class`: String? = null, value: GraphQLNullable<KeywordValue> = GraphQLNullable.Omitted, account: AccountScope? = null): MutationReceipt<KeywordRecord> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            default?.let { this.put("default", jsonValue(it)) }
            `class`?.let { this.put("class", jsonValue(it)) }
            this.put("value", value)
        }
        MutationReceipt(database_, KeywordRecord, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<KeywordRecord> = KeywordRecord.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<KeywordRecord> = KeywordRecord.destroy(id, account)
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
            var built_ = Query("MediaMessage", this, relation = "visual_media", params = buildJsonObject { this.put("chatId", jsonValue(chatId)) })
            return built_
        }
        fun audioMedia(chatId: String): Query<MediaMessage> {
            var built_ = Query("MediaMessage", this, relation = "audio_media", params = buildJsonObject { this.put("chatId", jsonValue(chatId)) })
            return built_
        }
        fun giftMedia(chatId: String): Query<MediaMessage> {
            var built_ = Query("MediaMessage", this, relation = "gift_media", params = buildJsonObject { this.put("chatId", jsonValue(chatId)) })
            return built_
        }
        fun visualSince(chatId: String, afterSequence: Long): Query<MediaMessage> {
            var built_ = Query("MediaMessage", this, relation = "visual_since", params = buildJsonObject { this.put("chatId", jsonValue(chatId)); this.put("afterSequence", jsonValue(afterSequence)) })
            return built_
        }
        fun audioSince(chatId: String, afterSequence: Long): Query<MediaMessage> {
            var built_ = Query("MediaMessage", this, relation = "audio_since", params = buildJsonObject { this.put("chatId", jsonValue(chatId)); this.put("afterSequence", jsonValue(afterSequence)) })
            return built_
        }
        fun giftsSince(chatId: String, afterSequence: Long): Query<MediaMessage> {
            var built_ = Query("MediaMessage", this, relation = "gifts_since", params = buildJsonObject { this.put("chatId", jsonValue(chatId)); this.put("afterSequence", jsonValue(afterSequence)) })
            return built_
        }
        suspend fun create(chatId: String, kind: MediaMessageKind, mediaKind: MediaKindValue? = null, sequenceNumber: Long? = null, account: AccountScope? = null): MutationReceipt<MediaMessage> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("chatId", jsonValue(chatId))
                this.put("kind", jsonValue(kind))
                mediaKind?.let { this.put("mediaKind", jsonValue(it)) }
                sequenceNumber?.let { this.put("sequenceNumber", jsonValue(it)) }
            }
            MutationReceipt(database_, MediaMessage, database_.create(epoch_, "MediaMessage", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(chatId: String? = null, kind: MediaMessageKind? = null, mediaKind: GraphQLNullable<MediaKindValue> = GraphQLNullable.Omitted, sequenceNumber: GraphQLNullable<Long> = GraphQLNullable.Omitted, account: AccountScope? = null): MutationReceipt<MediaMessage> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            chatId?.let { this.put("chatId", jsonValue(it)) }
            kind?.let { this.put("kind", jsonValue(it)) }
            this.put("mediaKind", mediaKind)
            this.put("sequenceNumber", sequenceNumber)
        }
        MutationReceipt(database_, MediaMessage, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<MediaMessage> = MediaMessage.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<MediaMessage> = MediaMessage.destroy(id, account)
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
        suspend fun transcodeStatus(id: String, force: Boolean = false, account: AccountScope? = null): Moment? = onAccount(account) { database, epoch -> database.node(epoch, "Moment", "transcode_status", id, force)?.let { Moment.read(it) } }
        val recent: Query<Moment> get() {
            var built_ = Query("Moment", this, relation = "recent", params = buildJsonObject {  })
            built_ = built_.order(Order("createdAt", "desc"))
            return built_
        }
        fun byUser(userId: String?): Query<Moment> {
            var built_ = Query("Moment", this, relation = "by_user", params = buildJsonObject { this.put("userId", jsonValue(userId)) })
            built_ = built_.where(Condition.equal("userId", jsonValue(userId)))
            built_ = built_.order(Order("createdAt", "desc"))
            return built_
        }
        val feed: Query<Moment> get() {
            var built_ = Query("Moment", this, relation = "feed", params = buildJsonObject {  })
            return built_
        }
        suspend fun feedEdge(id: String, account: AccountScope? = null): MomentFeedEdge? = Moment.feed.edgeValues(id, account)?.let { MomentFeedEdge(it) }
        fun vibeFeed(vibeId: String): Query<Moment> {
            var built_ = Query("Moment", this, relation = "vibe_feed", params = buildJsonObject { this.put("vibeId", jsonValue(vibeId)) })
            return built_
        }
        suspend fun vibeFeedEdge(vibeId: String, id: String, account: AccountScope? = null): MomentVibeFeedEdge? = Moment.vibeFeed(vibeId).edgeValues(id, account)?.let { MomentVibeFeedEdge(it) }
        suspend fun send(account: AccountScope, text: String? = null, media: JsonElement? = null): MutationReceipt<Moment> {
            val values_ = buildJsonObject {
                text?.let { this.put("text", jsonValue(it)) }
                media?.let { this.put("media", jsonValue(it)) }
            }
            return account.send(this, values_)
        }
    }
    suspend fun update(uuid: String? = null, userId: GraphQLNullable<String> = GraphQLNullable.Omitted, text: GraphQLNullable<String> = GraphQLNullable.Omitted, likedByMe: Boolean? = null, media: JsonElement? = null, createdAt: String? = null, account: AccountScope? = null): MutationReceipt<Moment> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            uuid?.let { this.put("uuid", jsonValue(it)) }
            this.put("userId", userId)
            this.put("text", text)
            likedByMe?.let { this.put("likedByMe", jsonValue(it)) }
            media?.let { this.put("media", jsonValue(it)) }
            createdAt?.let { this.put("createdAt", jsonValue(it)) }
        }
        MutationReceipt(database_, Moment, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Moment> = Moment.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Moment> = Moment.destroy(id, account)
    suspend fun like(account: AccountScope? = null): MutationReceipt<Moment> = this.act("like", JsonObject(parseJsonObject("""{"likedByMe":true}""") + mapOf()), JsonObject(mapOf()), Moment, account)
    suspend fun share(account: AccountScope? = null): MutationReceipt<Moment> = this.act("share", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf()), Moment, account)
    suspend fun uploadClip(account: AccountScope, request: UploadRequest): MutationReceipt<Moment> = account.upload(this, "upload_clip", request, Moment)
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
        suspend fun create(phase: NestedSamplePhase? = null, value: NestedValue? = null, values: List<NestedValue>? = null, tree: NestedTree? = null, replyTo: RecordReplyNode? = null, account: AccountScope? = null): MutationReceipt<NestedSample> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                phase?.let { this.put("phase", jsonValue(it)) }
                value?.let { this.put("value", jsonValue(it)) }
                values?.let { this.put("values", jsonValue(it)) }
                tree?.let { this.put("tree", jsonValue(it)) }
                replyTo?.let { this.put("replyTo", jsonValue(it)) }
            }
            MutationReceipt(database_, NestedSample, database_.create(epoch_, "NestedSample", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(phase: GraphQLNullable<NestedSamplePhase> = GraphQLNullable.Omitted, value: GraphQLNullable<NestedValue> = GraphQLNullable.Omitted, values: List<NestedValue>? = null, tree: GraphQLNullable<NestedTree> = GraphQLNullable.Omitted, replyTo: GraphQLNullable<RecordReplyNode> = GraphQLNullable.Omitted, account: AccountScope? = null): MutationReceipt<NestedSample> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            this.put("phase", phase)
            this.put("value", value)
            values?.let { this.put("values", jsonValue(it)) }
            this.put("tree", tree)
            this.put("replyTo", replyTo)
        }
        MutationReceipt(database_, NestedSample, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<NestedSample> = NestedSample.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<NestedSample> = NestedSample.destroy(id, account)
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
        suspend fun create(values: List<NestedValue>? = null, account: AccountScope? = null): MutationReceipt<NestedReference> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                values?.let { this.put("values", jsonValue(it)) }
            }
            MutationReceipt(database_, NestedReference, database_.create(epoch_, "NestedReference", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(values: List<NestedValue>? = null, account: AccountScope? = null): MutationReceipt<NestedReference> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            values?.let { this.put("values", jsonValue(it)) }
        }
        MutationReceipt(database_, NestedReference, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<NestedReference> = NestedReference.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<NestedReference> = NestedReference.destroy(id, account)
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
            var built_ = Query("Notice", this, relation = "recent", params = buildJsonObject {  })
            built_ = built_.order(Order("occurredAt", "desc"))
            return built_
        }
        suspend fun create(body: String, occurredAt: String, account: AccountScope? = null): MutationReceipt<Notice> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("body", jsonValue(body))
                this.put("occurredAt", jsonValue(occurredAt))
            }
            MutationReceipt(database_, Notice, database_.create(epoch_, "Notice", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(body: String? = null, occurredAt: String? = null, account: AccountScope? = null): MutationReceipt<Notice> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            body?.let { this.put("body", jsonValue(it)) }
            occurredAt?.let { this.put("occurredAt", jsonValue(it)) }
        }
        MutationReceipt(database_, Notice, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Notice> = Notice.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Notice> = Notice.destroy(id, account)
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
            var built_ = Query("InteractionNotice", this, relation = "recent", params = buildJsonObject {  })
            built_ = built_.order(Order("occurredAt", "desc"))
            return built_
        }
        suspend fun create(recordId: String, actorUserId: String, occurredAt: String, account: AccountScope? = null): MutationReceipt<InteractionNotice> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("recordId", jsonValue(recordId))
                this.put("actorUserId", jsonValue(actorUserId))
                this.put("occurredAt", jsonValue(occurredAt))
            }
            MutationReceipt(database_, InteractionNotice, database_.create(epoch_, "InteractionNotice", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(recordId: String? = null, actorUserId: String? = null, occurredAt: String? = null, account: AccountScope? = null): MutationReceipt<InteractionNotice> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            recordId?.let { this.put("recordId", jsonValue(it)) }
            actorUserId?.let { this.put("actorUserId", jsonValue(it)) }
            occurredAt?.let { this.put("occurredAt", jsonValue(it)) }
        }
        MutationReceipt(database_, InteractionNotice, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<InteractionNotice> = InteractionNotice.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<InteractionNotice> = InteractionNotice.destroy(id, account)
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
            var built_ = Query("ChatAction", this, relation = "by_chat", params = buildJsonObject { this.put("chatId", jsonValue(chatId)) })
            built_ = built_.where(Condition.equal("chatId", jsonValue(chatId)))
            return built_
        }
        suspend fun create(chatId: String, userId: String, kind: String, active: Boolean, account: AccountScope? = null): MutationReceipt<ChatAction> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("chatId", jsonValue(chatId))
                this.put("userId", jsonValue(userId))
                this.put("kind", jsonValue(kind))
                this.put("active", jsonValue(active))
            }
            MutationReceipt(database_, ChatAction, database_.create(epoch_, "ChatAction", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(chatId: String? = null, userId: String? = null, kind: String? = null, active: Boolean? = null, account: AccountScope? = null): MutationReceipt<ChatAction> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            chatId?.let { this.put("chatId", jsonValue(it)) }
            userId?.let { this.put("userId", jsonValue(it)) }
            kind?.let { this.put("kind", jsonValue(it)) }
            active?.let { this.put("active", jsonValue(it)) }
        }
        MutationReceipt(database_, ChatAction, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<ChatAction> = ChatAction.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<ChatAction> = ChatAction.destroy(id, account)
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
            var built_ = Query("ChatActivityNotice", this, relation = "recent", params = buildJsonObject {  })
            built_ = built_.order(Order("kind", "asc"))
            return built_
        }
        suspend fun create(chatId: String, userId: String, kind: String, active: Boolean, account: AccountScope? = null): MutationReceipt<ChatActivityNotice> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("chatId", jsonValue(chatId))
                this.put("userId", jsonValue(userId))
                this.put("kind", jsonValue(kind))
                this.put("active", jsonValue(active))
            }
            MutationReceipt(database_, ChatActivityNotice, database_.create(epoch_, "ChatActivityNotice", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(chatId: String? = null, userId: String? = null, kind: String? = null, active: Boolean? = null, account: AccountScope? = null): MutationReceipt<ChatActivityNotice> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            chatId?.let { this.put("chatId", jsonValue(it)) }
            userId?.let { this.put("userId", jsonValue(it)) }
            kind?.let { this.put("kind", jsonValue(it)) }
            active?.let { this.put("active", jsonValue(it)) }
        }
        MutationReceipt(database_, ChatActivityNotice, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<ChatActivityNotice> = ChatActivityNotice.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<ChatActivityNotice> = ChatActivityNotice.destroy(id, account)
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
            var built_ = Query("Presence", this, relation = "for_user", params = buildJsonObject { this.put("userId", jsonValue(userId)) })
            built_ = built_.where(Condition.equal("userId", jsonValue(userId)))
            return built_
        }
        val online: Query<Presence> get() {
            var built_ = Query("Presence", this, relation = "online", params = buildJsonObject {  })
            return built_
        }
        fun onChannel(channel: String): Query<Presence> {
            var built_ = Query("Presence", this, relation = "on_channel", params = buildJsonObject { this.put("channel", jsonValue(channel)) })
            return built_
        }
        fun history(presenceId: String): Query<Presence> {
            var built_ = Query("Presence", this, relation = "history", params = buildJsonObject { this.put("presenceId", jsonValue(presenceId)) })
            return built_
        }
        suspend fun create(userId: String, room: String, account: AccountScope? = null): MutationReceipt<Presence> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("userId", jsonValue(userId))
                this.put("room", jsonValue(room))
            }
            MutationReceipt(database_, Presence, database_.create(epoch_, "Presence", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(userId: String? = null, room: String? = null, account: AccountScope? = null): MutationReceipt<Presence> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            userId?.let { this.put("userId", jsonValue(it)) }
            room?.let { this.put("room", jsonValue(it)) }
        }
        MutationReceipt(database_, Presence, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Presence> = Presence.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Presence> = Presence.destroy(id, account)
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
            var built_ = Query("Record", this, relation = "by_container", params = buildJsonObject { this.put("containerId", jsonValue(containerId)) })
            built_ = built_.where(Condition.equal("containerId", jsonValue(containerId)))
            built_ = built_.order(Order("createdAt", "asc"))
            return built_
        }
        fun thread(containerId: String): Query<Record> {
            var built_ = Query("Record", this, relation = "thread", params = buildJsonObject { this.put("containerId", jsonValue(containerId)) })
            return built_
        }
        suspend fun node(id: String, force: Boolean = false, account: AccountScope? = null): Record? = onAccount(account) { database, epoch -> database.node(epoch, "Record", "node", id, force)?.let { Record.read(it) } }
        suspend fun transcodeStatus(id: String, force: Boolean = false, account: AccountScope? = null): Record? = onAccount(account) { database, epoch -> database.node(epoch, "Record", "transcode_status", id, force)?.let { Record.read(it) } }
        fun containerMedia(containerId: String): Query<Record> {
            var built_ = Query("Record", this, relation = "container_media", params = buildJsonObject { this.put("containerId", jsonValue(containerId)) })
            return built_
        }
        val history: Query<Record> get() {
            var built_ = Query("Record", this, relation = "history", params = buildJsonObject {  })
            return built_
        }
        suspend fun send(account: AccountScope, containerId: String, authorId: String? = null, regionId: String? = null, text: String, attachedUserId: String? = null, signedId: String? = null): MutationReceipt<Record> {
            val values_ = buildJsonObject {
                this.put("containerId", jsonValue(containerId))
                authorId?.let { this.put("authorId", jsonValue(it)) }
                regionId?.let { this.put("regionId", jsonValue(it)) }
                this.put("text", jsonValue(text))
            }
            val args_ = buildJsonObject {
                attachedUserId?.let { this.put("attachedUserId", jsonValue(it)) }
                signedId?.let { this.put("signedId", jsonValue(it)) }
            }
            return account.send(this, values_, args_)
        }
    }
    suspend fun update(containerId: String? = null, authorId: String? = null, regionId: GraphQLNullable<String> = GraphQLNullable.Omitted, text: String? = null, createdAt: String? = null, sequenceNumber: GraphQLNullable<Long> = GraphQLNullable.Omitted, reactions: List<RecordReactionNode>? = null, attachment: GraphQLNullable<RecordAttachment> = GraphQLNullable.Omitted, media: GraphQLNullable<RecordMedia> = GraphQLNullable.Omitted, account: AccountScope? = null): MutationReceipt<Record> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            containerId?.let { this.put("containerId", jsonValue(it)) }
            authorId?.let { this.put("authorId", jsonValue(it)) }
            this.put("regionId", regionId)
            text?.let { this.put("text", jsonValue(it)) }
            createdAt?.let { this.put("createdAt", jsonValue(it)) }
            this.put("sequenceNumber", sequenceNumber)
            reactions?.let { this.put("reactions", jsonValue(it)) }
            this.put("attachment", attachment)
            this.put("media", media)
        }
        MutationReceipt(database_, Record, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Record> = Record.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Record> = Record.destroy(id, account)
    suspend fun react(reactionBody: String, userId: String, account: AccountScope? = null): MutationReceipt<Record> = this.act("react", JsonObject(parseJsonObject("""{"reactions":{"${'$'}keyed":{"by":"userId","element":{"body":{"${'$'}arg":"reactionBody"},"userId":{"${'$'}arg":"userId"}}}}}""") + mapOf()), JsonObject(mapOf("reactionBody" to jsonValue(reactionBody), "userId" to jsonValue(userId))), Record, account)
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
            var built_ = Query("CurrentUser", this, relation = "current", params = buildJsonObject {  })
            return built_
        }
        suspend fun create(name: String, account: AccountScope? = null): MutationReceipt<CurrentUser> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("name", jsonValue(name))
            }
            MutationReceipt(database_, CurrentUser, database_.create(epoch_, "CurrentUser", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(name: String? = null, account: AccountScope? = null): MutationReceipt<CurrentUser> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            name?.let { this.put("name", jsonValue(it)) }
        }
        MutationReceipt(database_, CurrentUser, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<CurrentUser> = CurrentUser.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<CurrentUser> = CurrentUser.destroy(id, account)
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
            var built_ = Query("CurrentRegion", this, relation = "current", params = buildJsonObject {  })
            return built_
        }
        suspend fun create(name: String, account: AccountScope? = null): MutationReceipt<CurrentRegion> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("name", jsonValue(name))
            }
            MutationReceipt(database_, CurrentRegion, database_.create(epoch_, "CurrentRegion", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(name: String? = null, account: AccountScope? = null): MutationReceipt<CurrentRegion> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            name?.let { this.put("name", jsonValue(it)) }
        }
        MutationReceipt(database_, CurrentRegion, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<CurrentRegion> = CurrentRegion.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<CurrentRegion> = CurrentRegion.destroy(id, account)
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
        suspend fun node(id: String, force: Boolean = false, account: AccountScope? = null): CustomScreen? = onAccount(account) { database, epoch -> database.node(epoch, "CustomScreen", "node", id, force)?.let { CustomScreen.read(it) } }
        suspend fun create(slug: String, version: Long, requires: ScreenRequires, spec: ScreenSpec? = null, account: AccountScope? = null): MutationReceipt<CustomScreen> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("slug", jsonValue(slug))
                this.put("version", jsonValue(version))
                this.put("requires", jsonValue(requires))
                spec?.let { this.put("spec", jsonValue(it)) }
            }
            MutationReceipt(database_, CustomScreen, database_.create(epoch_, "CustomScreen", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(slug: String? = null, version: Long? = null, requires: ScreenRequires? = null, spec: GraphQLNullable<ScreenSpec> = GraphQLNullable.Omitted, account: AccountScope? = null): MutationReceipt<CustomScreen> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            slug?.let { this.put("slug", jsonValue(it)) }
            version?.let { this.put("version", jsonValue(it)) }
            requires?.let { this.put("requires", jsonValue(it)) }
            this.put("spec", spec)
        }
        MutationReceipt(database_, CustomScreen, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<CustomScreen> = CustomScreen.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<CustomScreen> = CustomScreen.destroy(id, account)
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
        suspend fun create(record: String, account: AccountScope? = null): MutationReceipt<StorageRow> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("record", jsonValue(record))
            }
            MutationReceipt(database_, StorageRow, database_.create(epoch_, "StorageRow", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(record: String? = null, account: AccountScope? = null): MutationReceipt<StorageRow> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            record?.let { this.put("record", jsonValue(it)) }
        }
        MutationReceipt(database_, StorageRow, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<StorageRow> = StorageRow.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<StorageRow> = StorageRow.destroy(id, account)
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
            var built_ = Query("Tail", this, relation = "since", params = buildJsonObject { this.put("containerId", jsonValue(containerId)); this.put("afterSequence", jsonValue(afterSequence)); this.put("pinned", jsonValue(pinned)) })
            return built_
        }
        suspend fun create(text: String, account: AccountScope? = null): MutationReceipt<Tail> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("text", jsonValue(text))
            }
            MutationReceipt(database_, Tail, database_.create(epoch_, "Tail", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(text: String? = null, account: AccountScope? = null): MutationReceipt<Tail> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            text?.let { this.put("text", jsonValue(it)) }
        }
        MutationReceipt(database_, Tail, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Tail> = Tail.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Tail> = Tail.destroy(id, account)
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
        suspend fun create(label: String? = null, attachment: RecordAttachment? = null, props: MomentWidgetItemProps? = null, account: AccountScope? = null): MutationReceipt<UnionSample> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                label?.let { this.put("label", jsonValue(it)) }
                attachment?.let { this.put("attachment", jsonValue(it)) }
                props?.let { this.put("props", jsonValue(it)) }
            }
            MutationReceipt(database_, UnionSample, database_.create(epoch_, "UnionSample", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(label: GraphQLNullable<String> = GraphQLNullable.Omitted, attachment: GraphQLNullable<RecordAttachment> = GraphQLNullable.Omitted, props: GraphQLNullable<MomentWidgetItemProps> = GraphQLNullable.Omitted, account: AccountScope? = null): MutationReceipt<UnionSample> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            this.put("label", label)
            this.put("attachment", attachment)
            this.put("props", props)
        }
        MutationReceipt(database_, UnionSample, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<UnionSample> = UnionSample.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<UnionSample> = UnionSample.destroy(id, account)
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
            var built_ = Query("User", this, relation = "profile", params = buildJsonObject {  })
            return built_
        }
        fun containerMembers(containerId: String): Query<User> {
            var built_ = Query("User", this, relation = "container_members", params = buildJsonObject { this.put("containerId", jsonValue(containerId)) })
            return built_
        }
        fun completeMembers(containerId: String): Query<User> {
            var built_ = Query("User", this, relation = "complete_members", params = buildJsonObject { this.put("containerId", jsonValue(containerId)) })
            return built_
        }
        suspend fun create(name: String, birthday: String? = null, account: AccountScope? = null): MutationReceipt<User> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("name", jsonValue(name))
                birthday?.let { this.put("birthday", jsonValue(it)) }
            }
            MutationReceipt(database_, User, database_.create(epoch_, "User", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(name: String? = null, birthday: GraphQLNullable<String> = GraphQLNullable.Omitted, account: AccountScope? = null): MutationReceipt<User> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            name?.let { this.put("name", jsonValue(it)) }
            this.put("birthday", birthday)
        }
        MutationReceipt(database_, User, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<User> = User.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<User> = User.destroy(id, account)
    suspend fun set(typing: Boolean? = null, mood: GraphQLNullable<String> = GraphQLNullable.Omitted, account: AccountScope? = null) {
        onAccount(account) { database_, epoch_ -> database_.setEphemeral(epoch_, this.modelName, this.id, buildJsonObject {
            typing?.let { this.put("typing", jsonValue(it)) }
            this.put("mood", mood)
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
        suspend fun create(momentId: String, userId: String, kind: VibeKind, account: AccountScope? = null): MutationReceipt<Vibe> = onAccount(account) { database_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("momentId", jsonValue(momentId))
                this.put("userId", jsonValue(userId))
                this.put("kind", jsonValue(kind))
            }
            MutationReceipt(database_, Vibe, database_.create(epoch_, "Vibe", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(momentId: String? = null, userId: String? = null, kind: VibeKind? = null, account: AccountScope? = null): MutationReceipt<Vibe> = onAccount(account) { database_, epoch_ ->
        val values_ = buildJsonObject {
            momentId?.let { this.put("momentId", jsonValue(it)) }
            userId?.let { this.put("userId", jsonValue(it)) }
            kind?.let { this.put("kind", jsonValue(it)) }
        }
        MutationReceipt(database_, Vibe, database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadRequest? = null, account: AccountScope? = null): MutationReceipt<Vibe> = Vibe.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Vibe> = Vibe.destroy(id, account)
}

@Immutable
data class LedgerFeedEntriesEdge(
    val impressionUuid: String? = null
) {
    internal constructor(values: JsonObject) : this((values["impressionUuid"] as? JsonPrimitive)?.contentOrNull)
}

@Immutable
data class ShadowScopeEdge(
    val score: Long? = null
) {
    internal constructor(values: JsonObject) : this((values["score"] as? JsonPrimitive)?.longOrNull)
}

@Immutable
data class ShadowAccountEdge(
    val rank: Long? = null
) {
    internal constructor(values: JsonObject) : this((values["rank"] as? JsonPrimitive)?.longOrNull)
}

@Immutable
data class MailboxEntriesEdge(
    val score: Long? = null
) {
    internal constructor(values: JsonObject) : this((values["score"] as? JsonPrimitive)?.longOrNull)
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

val modelsJSON = """[{"key":"Ledger","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"scope","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[["scope"]],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateMoment","upload":false,"durable":true,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"update","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"UpdateMoment","upload":false,"durable":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"move","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"LikeMoment","upload":false,"durable":false,"variables":{"scope":"scope"},"optimistic":{},"refetch":[],"args":{"scope":"string"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"LedgerFeed","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"entries","by_columns":[],"text_params":["scope"],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"impressionUuid","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Shadow","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"scope","by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"score","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"account","by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"rank","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateMoment","upload":false,"durable":true,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Holder","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"account","by_columns":[],"text_params":[],"remote":{"operation":"AccountProfile","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Mailbox","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"read","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"put","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"entries","by_columns":[],"text_params":["act"],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"score","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"mark","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"LikeMoment","upload":false,"durable":false,"variables":{"put":"put"},"optimistic":{},"refetch":[],"args":{"put":"string"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Asset","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"clientKey","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"size","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"position","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create_file","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"CreateAsset","upload":true,"durable":false,"variables":{"assetId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":["clientKey"],"ignore_response":false,"wrap":null,"root":"createAsset.asset","lands":[],"result":{}},{"batch":{"min":2,"max":10,"items":"items","single":"create_file","input":{"clientKey":"clientKey"},"renumber":{"position":"position","size":"size","position_from":0}},"client_mutation_id":false,"name":"create_files","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"CreateAssets","upload":true,"durable":false,"variables":{"assetId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":["clientKey"],"ignore_response":false,"wrap":null,"root":"createAssets.assets","lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"upload_steps":{"create_blob":{"name":"CreateBlob","root_path":"blobCreate"},"multipart":{"initiate":{"name":"InitMultipart"},"presign":{"name":"PresignParts"},"complete":{"name":"CompleteMultipart"},"abort":{"name":"AbortMultipart"}}}},{"key":"Club","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateClub","upload":false,"durable":false,"variables":{"name":"name"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"leave","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"LeaveClub","upload":false,"durable":false,"variables":{"clubId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":true,"wrap":null,"root":null,"lands":[],"result":{},"removes_receiver":true},{"client_mutation_id":false,"name":"add_member","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"AddClubMember","upload":false,"durable":false,"variables":{"clubId":"id","userIds":"userIds"},"optimistic":{},"refetch":["Person.club_members"],"args":{"userIds":"ids"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"addClubMember","lands":[],"result":{},"scope_effects":[{"kind":"join","model":"Person","scope":"club_members","rows":"userIds","with":{"clubId":"id"},"order_param":"order","orders":["JOINED","OWNER_FIRST"]}]},{"client_mutation_id":false,"name":"remove_member","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"RemoveClubMember","upload":false,"durable":false,"variables":{"clubId":"id","userIds":"userIds"},"optimistic":{},"refetch":["Person.club_members"],"args":{"userIds":"ids"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"removeClubMember","lands":[],"result":{},"scope_effects":[{"kind":"leave","model":"Person","scope":"club_members","rows":"userIds","with":{"clubId":"id"},"order_param":"order","orders":["JOINED","OWNER_FIRST"]}]}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Person","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"club_members","by_columns":[],"text_params":["clubId","order"],"remote":{"operation":"ClubMembers","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"node.members"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Letter","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"at","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"thread","by_columns":["chatId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"at","direction":"asc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"SendLetter","upload":false,"durable":true,"variables":{"at":"at","chatId":"chatId","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"forward","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ForwardLetter","upload":false,"durable":false,"variables":{"chatId":"targetChatId","letterId":"id"},"optimistic":{},"refetch":[],"args":{"targetChatId":"id"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"forwardLetter.letter","lands":[],"result":{},"copies_receiver_to":{"arg":"targetChatId","field":"chatId"}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Container","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastRecordAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastRecordId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"shareUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"isPublic","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"shelves","kind":{"type":"list","element":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ContainerShelf","values":["PRIMARY","SECONDARY"],"merge":null}],"associations":[{"name":"records","kind":"has_many","target":"Record","foreign_key":"containerId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null},{"name":"lastRecord","kind":"belongs_to","target":"Record","foreign_key":"lastRecordId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ContainerNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"nodes","by_columns":[],"text_params":[],"remote":{"operation":"ContainersByIds","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"node":"many"},{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"lastRecordAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"shelf","membership":{"shelf":{"contains":"shelves"}},"joins_node":true,"by_columns":[],"text_params":["shelf"],"text_types":{"shelf":"ContainerShelf"},"remote":{"operation":"ContainerShelfQuery","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":"containers"},"is_global":false,"sort":"server_order","joins_events":true,"joins_created":false}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateContainer","upload":false,"durable":true,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"create_photo","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ContainerGroupCreateFile","upload":true,"durable":false,"variables":{"containerId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"groupContainerCreate.container","lands":[],"result":{}},{"client_mutation_id":false,"name":"update_photo","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ContainerGroupUpdateFile","upload":true,"durable":false,"variables":{"containerId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"groupContainerUpdate.container","lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"CONTAINER","relation":"node","parameter":null,"removes_on":["DELETED","ACCESS_REVOKED"]}],"change_feed":{"subscription":"RecordEvents","root":"recordEvents.entries","area":"topic","kind":"action","id":"recordId"},"upload_steps":{"create_blob":{"name":"CreateBlob","root_path":"blobCreate"},"multipart":{"initiate":{"name":"InitMultipart"},"presign":{"name":"PresignParts"},"complete":{"name":"CompleteMultipart"},"abort":{"name":"AbortMultipart"}}}},{"key":"Document","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"bySlug","updates":[{"subscription":"documentUpdated","deleted_by":null,"set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":["slug"],"text_params":["source"],"remote":{"operation":"documentBySlug","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":"documentBySlug"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Chat","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"serverRevision","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"revision","app_type":null,"merge":null},{"name":"lists","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ChatNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"list","membership":{"list":{"contains":"lists"}},"joins_node":true,"by_columns":[],"text_params":["list"],"remote":{"operation":"ChatList","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":"chats"},"is_global":false,"sort":"server_order","joins_events":true,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"MESSAGE","relation":"node","parameter":null,"payload":"chat"},{"area":"CHAT","relation":"node","parameter":null,"removes_on":["DELETED","ACCESS_REVOKED"]}],"server_revision":"serverRevision"},{"key":"Message","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"MessageNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"MESSAGE","relation":"node","parameter":null,"removes_on":["DELETED","ACCESS_REVOKED"],"payload":"message"}]},{"key":"InterfaceRecord","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"spec","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleSpec","merge":null,"fields":[{"name":"root","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"elements","kind":{"type":"union_array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleElement","merge":null,"fields":[{"name":"button","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleButton","merge":null,"fields":[{"name":"label","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextValue","merge":null,"fields":[{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null},{"name":"textLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextLiteral","merge":null}]},{"name":"disabled","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagValue","merge":null,"fields":[{"name":"flagLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagLiteral","merge":null},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null}]}]},{"name":"column","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleColumn","merge":null,"fields":[{"name":"gap","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberValue","merge":null,"fields":[{"name":"numberLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberLiteral","merge":null},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null}]},{"name":"alignItems","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"heading","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleHeading","merge":null,"fields":[{"name":"text","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextValue","merge":null},{"name":"level","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"pressable","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SamplePressable","merge":null},{"name":"spacer","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleSpacer","merge":null,"fields":[{"name":"length","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberValue","merge":null}]},{"name":"switch","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleSwitch","merge":null,"fields":[{"name":"checked","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagValue","merge":null},{"name":"label","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextValue","merge":null}]}],"shared":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"press","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleAction","merge":null,"fields":[{"name":"navigate","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNavigateAction","merge":null,"fields":[{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openRecord","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOpenRecordAction","merge":null,"fields":[{"name":"recordId","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openExternal","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOpenExternalAction","merge":null,"fields":[{"name":"url","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openProfile","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOpenProfileAction","merge":null,"fields":[{"name":"userId","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"visible","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibility","merge":null,"fields":[{"name":"condition","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleCondition","merge":null,"fields":[{"name":"comparison","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"negated","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"operand","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOperand","merge":null,"fields":[{"name":"flagLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagLiteral","merge":null,"fields":[{"name":"flag","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"numberLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberLiteral","merge":null,"fields":[{"name":"number","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"textLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextLiteral","merge":null,"fields":[{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"visibleAll","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibleAll","merge":null,"fields":[{"name":"conditions","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleCondition","merge":null}]},{"name":"visibleAny","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibleAny","merge":null,"fields":[{"name":"conditions","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleCondition","merge":null}]},{"name":"visibleFlag","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibleFlag","merge":null,"fields":[{"name":"flag","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}]},{"name":"state","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateEntry","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"union"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateValue","merge":null,"fields":[{"name":"flag","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagLiteral","merge":null},{"name":"number","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberLiteral","merge":null},{"name":"text","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextLiteral","merge":null}]}]}]}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"SampleNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"KeywordRecord","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"default","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"class","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"KeywordValue","merge":null,"fields":[{"name":"default","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"when","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"class","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"container","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"r","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"child","kind":{"type":"object"},"nullable":false,"optional":false,"default":{"class":"c","default":"d","when":"w"},"indexed":false,"id_from_parent":null,"from":null,"app_type":"KeywordChild","merge":null,"fields":[{"name":"default","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"when","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"class","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[{"name":"when","by_columns":["class"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"MediaMessage","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"enum","values":["TEXT","GIFT"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MediaMessageKind","merge":null},{"name":"mediaKind","kind":{"type":"enum","values":["AUDIO","IMAGE","VIDEO"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MediaKindValue","merge":null},{"name":"sequenceNumber","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"visual_media","row_rules":[{"field":"mediaKind","test":"in","values":["IMAGE","VIDEO"]}],"joins_node":true,"by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{"includeGifts":{"from":"constant","value":false},"mediaKinds":{"from":"constant","value":["IMAGE","VIDEO"]}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"audio_media","row_rules":[{"field":"mediaKind","test":"equals","value":"AUDIO"}],"joins_node":true,"by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{"includeGifts":{"from":"constant","value":false},"mediaKinds":{"from":"constant","value":["AUDIO"]}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"gift_media","row_rules":[{"field":"kind","test":"equals","value":"GIFT"}],"joins_node":true,"by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{"includeGifts":{"from":"constant","value":true}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"visual_since","row_rules":[{"field":"mediaKind","test":"in","values":["IMAGE","VIDEO"]},{"field":"sequenceNumber","test":"greater_than","parameter":"afterSequence"}],"joins_node":true,"by_columns":["chatId"],"text_params":["afterSequence"],"scalar_types":{"afterSequence":"int"},"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":50,"variables":{"includeGifts":{"from":"constant","value":false},"mediaKinds":{"from":"constant","value":["IMAGE","VIDEO"]}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"audio_since","row_rules":[{"field":"mediaKind","test":"equals","value":"AUDIO"},{"field":"sequenceNumber","test":"greater_than","parameter":"afterSequence"}],"joins_node":true,"by_columns":["chatId"],"text_params":["afterSequence"],"scalar_types":{"afterSequence":"int"},"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":50,"variables":{"includeGifts":{"from":"constant","value":false},"mediaKinds":{"from":"constant","value":["AUDIO"]}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"gifts_since","row_rules":[{"field":"kind","test":"equals","value":"GIFT"},{"field":"sequenceNumber","test":"greater_than","parameter":"afterSequence"}],"joins_node":true,"by_columns":["chatId"],"text_params":["afterSequence"],"scalar_types":{"afterSequence":"int"},"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":50,"variables":{"includeGifts":{"from":"constant","value":true}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false}],"indexes":[["chatId","sequenceNumber"]],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"uuid","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"searchable":true},{"name":"likedByMe","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"media","kind":{"type":"raw"},"nullable":false,"optional":false,"default":{},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tone","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":{"type":"local"}},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"author","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":true,"sets":{},"newer":[],"from":null},{"name":"vibes","kind":"has_many","target":"Vibe","foreign_key":"momentId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"transcode_status","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"MomentTranscodeStatus","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"momentId":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"by_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"feed","updates":[{"subscription":"MomentEvents","deleted_by":"destroyed","set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"impressionUuid","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"impressionScore","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"vibe_feed","by_columns":[],"text_params":["vibeId"],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"impressionUuid","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"score","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"impressionScore","app_type":null,"merge":null},{"name":"rank","kind":{"type":"enum","values":["top","normal"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"FeedRank","merge":null}]}],"indexes":[["uuid"],["userId","createdAt"]],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateMoment","upload":false,"durable":true,"variables":{"media":"media","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"update","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"UpdateMoment","upload":false,"durable":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"upload_clip","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"CreateMoment","upload":true,"durable":false,"variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"createMoment","lands":[],"result":{"successMessage":"string"}},{"client_mutation_id":false,"name":"destroy","root_shape":"destroy","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"DeleteMoment","upload":false,"durable":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"like","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"LikeMoment","upload":false,"durable":false,"variables":{"momentId":"id"},"optimistic":{"likedByMe":true},"refetch":["feed"],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"share","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ShareMoment","upload":false,"durable":false,"variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"shareMoment.moment","lands":[],"result":{"copies":"int","successMessage":"string"}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"upload_steps":{"create_blob":{"name":"CreateBlob","root_path":"blobCreate"},"multipart":{"initiate":{"name":"InitMultipart"},"presign":{"name":"PresignParts"},"complete":{"name":"CompleteMultipart"},"abort":{"name":"AbortMultipart"}}}},{"key":"NestedSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"phase","kind":{"type":"enum","values":["draft","live"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedSamplePhase","merge":null},{"name":"value","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null,"fields":[{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"note","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"nullableDefault","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":"default","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ownerId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"owner_id","app_type":null,"merge":null},{"name":"count","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":2,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratio","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":1.5,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"enabled","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"state","kind":{"type":"enum","values":["ready","done"]},"nullable":false,"optional":false,"default":"ready","indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueState","merge":null},{"name":"labels","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"counts","kind":{"type":"list","element":"int"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratios","kind":{"type":"list","element":"num"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"flags","kind":{"type":"list","element":"bool"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"dates","kind":{"type":"list","element":"date"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"owners","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"payload","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"rawValue","kind":{"type":"raw"},"nullable":false,"optional":false,"default":{"free":[1,true,null]},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"preferences","kind":{"type":"object"},"nullable":false,"optional":false,"default":{"label":"base"},"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null,"fields":[{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":true,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"presets","kind":{"type":"array"},"nullable":false,"optional":false,"default":[{"label":"first"}],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null},{"name":"metadata","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"child","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"horizontal","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":"x","app_type":null,"merge":null}]},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null}]},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null},{"name":"tree","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null,"fields":[{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null}]},{"name":"replyTo","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordReplyNode","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordUserRef","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fullName","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"username","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatarUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedMedia","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":"","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"NestedReference","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"Notice","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"occurredAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"recent","updates":[{"subscription":"BulletinAdded","deleted_by":null,"set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"occurredAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"InteractionNotice","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"recordId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"actorUserId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"occurredAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"recent","updates":[{"subscription":"RecordSignal","deleted_by":null,"set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"occurredAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"ChatAction","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"by_chat","updates":[{"subscription":"ChatActionSignal","deleted_by":null,"deleted_unless":"active","set":{},"removes":false,"debounce":null,"root":null,"id":["chatId","userId"],"apply":{"effect":"upsert","matches":{"kind":["RECORDING_AUDIO","RECORDING_VIDEO","TYPING","UPLOADING_PHOTO","UPLOADING_VIDEO"]}}}],"by_columns":["chatId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"8.seconds","listens":[]},{"key":"ChatActivityNotice","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"kind","direction":"asc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[{"subscription":"ChatActionSignal","deleted_by":null,"deleted_unless":"active","set":{},"removes":false,"debounce":null,"root":null,"id":["chatId","userId"],"apply":{"effect":"upsert","matches":{"kind":["SCREENSHOT"]}}}]},{"key":"Presence","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"room","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"user","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"for_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"online","updates":[{"subscription":"PresenceChanged","deleted_by":"left","set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":{"operation":"PresenceOnline","shape":{"type":"list","coverage":"complete"},"stale_time":"30.seconds","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"preload":{}},{"name":"on_channel","by_columns":[],"text_params":["channel"],"remote":{"operation":"PresenceOnChannel","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"preload":{"channel":["lobby","stage"]},"preload_order":["channel"]},{"name":"history","by_columns":[],"text_params":["presenceId"],"remote":{"operation":"PresenceHistory","shape":{"type":"list","coverage":"complete"},"stale_time":"12.hours","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"preload":{"presenceId":{"from":{"model":"Presence","relation":"online"}}},"preload_limit":40,"preload_order":["presenceId"]}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"Record","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"containerId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"authorId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"regionId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"sequenceNumber","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"reactions","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordReactionNode","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"user.id","app_type":null,"merge":null},{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"link":{"model":"User","id":"userId"},"app_type":"RecordReactionUser","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"attachment","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordAttachment","merge":null,"fields":[{"name":"reward","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RewardAttachment","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tier","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"rewardTier","app_type":null,"merge":null},{"name":"status","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tags","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"moment","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentAttachment","merge":null,"fields":[{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"caption","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"profile","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ProfileAttachment","merge":null,"fields":[{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"AttachmentUser","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"reaction","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ReactionAttachment","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordMedia","merge":{"type":"keys","keys":{"fileUrl":{"type":"non_empty"}}},"fields":[{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"transcoded","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}],"associations":[{"name":"container","kind":"belongs_to","target":"Container","foreign_key":"containerId","dependent":null,"touches_parent":false,"sets":{"lastRecordAt":"createdAt","lastRecordId":"id"},"newer":[{"child":"createdAt","parent":"lastRecordAt"}],"from":null}],"relations":[{"name":"by_container","by_columns":["containerId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"asc"}]},"joins_events":false,"joins_created":false},{"name":"thread","by_columns":["containerId"],"text_params":[],"remote":{"operation":"ContainerThread","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"container.records"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc_nulls_last"},{"column":"createdAt","direction":"asc"}]},"joins_events":true,"joins_created":false,"order_stable":true},{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"RecordNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"transcode_status","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"RecordTranscodeStatus","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"container_media","by_columns":["containerId"],"text_params":[],"remote":{"operation":"ContainerMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"node.records"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"history","by_columns":[],"text_params":[],"remote":{"operation":"ContainerRecords","shape":{"type":"connection","direction":"backward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"1.minute","empty_stale_time":"10.seconds","selects_page_info":true,"page_size":30,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateRecord","upload":false,"durable":true,"variables":{"attachedUserId":"attachedUserId","containerId":"containerId","signedId":"signedId","text":"text"},"optimistic":{},"refetch":[],"args":{"attachedUserId":"id?","signedId":"string?"},"local":["authorId","regionId"],"local_from":{"authorId":{"row":{"model":"CurrentUser","field":"id"}},"regionId":{"row":{"model":"CurrentRegion","field":"id"}}},"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"react","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"RecordReact","upload":false,"durable":false,"variables":{"recordId":"id"},"optimistic":{"reactions":{"${'$'}keyed":{"by":"userId","element":{"body":{"${'$'}arg":"reactionBody"},"userId":{"${'$'}arg":"userId"}}}}},"refetch":[],"args":{"reactionBody":"string","userId":"id"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"RECORD","relation":"observed","parameter":null,"observed":true}]},{"key":"CurrentUser","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"current","by_columns":[],"text_params":[],"remote":{"operation":"CurrentUserQuery","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"CurrentRegion","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"current","by_columns":[],"text_params":[],"remote":{"operation":"CurrentRegionQuery","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"CustomScreen","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"version","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"requires","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenRequires","merge":null,"fields":[{"name":"ios","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"android","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"spec","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenSpec","merge":null,"fields":[{"name":"root","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"state","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateEntry","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"raw"},"parse_json":true,"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"elements","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenElement","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"type","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slot","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"props","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"visible","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"press","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"change","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"padding","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"margin","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"background","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"radius","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"expand","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"align","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"accessibilityLabel","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ScreenNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"StorageRow","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"record","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Tail","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"since","by_columns":[],"text_params":["containerId","afterSequence","pinned"],"scalar_types":{"afterSequence":"int","pinned":"bool"},"remote":{"operation":"tailSince","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":50,"variables":{},"root":"tailSince"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"UnionSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"attachment","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordAttachment","merge":null},{"name":"props","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentWidgetItemProps","merge":null,"fields":[{"name":"emoji","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentEmojiProps","merge":null,"fields":[{"name":"emoji","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"label","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentLabelProps","merge":null,"fields":[{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"size","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"User","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatar","kind":{"type":"file"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"birthday","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"profile","updates":[{"subscription":"UserTyping","deleted_by":null,"set":{"typing":true},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":{"operation":"UserProfile","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"container_members","by_columns":[],"text_params":["containerId"],"remote":{"operation":"ContainerMembers","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"node.members","total_from":"node.membersCount"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"complete_members","by_columns":[],"text_params":["containerId"],"remote":{"operation":"ContainerMembers","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":100,"variables":{},"root":"node.members","total_from":"node.membersCount"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"members_complete":true}],"indexes":[],"actions":[],"ephemeral":[{"name":"typing","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":"5.seconds"},{"name":"mood","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":null}],"storage":"disk","expire_after":null,"listens":[]},{"key":"Vibe","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"enum","values":["like","love","laugh"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"VibeKind","merge":null}],"associations":[],"relations":[],"indexes":[["userId"]],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]}]"""

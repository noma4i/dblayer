package com.noma4i.dblayer.models

import androidx.compose.runtime.Immutable
import com.noma4i.dblayer.*
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*

@Immutable
data class Clip(
    override val id: String,
    val title: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap(),
    override val rowRevision: Long = 0,
    override val estimatedBytes: ULong = 0u
) : DBModel {
    override val modelName get() = "Clip"
    companion object : ModelDecoder<Clip> {
        override val layout = "e9226eea266088346d0e13a891650ac9945faf4f4ff01bfa2daed4a6ed5e8092"
        override val modelName = "Clip"
        override fun decode(row: WireRow): Clip {
            val r = CellReader(row.cells)
            return Clip(
                id = row.id,
                title = r.opt(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
                rowRevision = row.revision,
                estimatedBytes = row.estimatedBytes,
            )
        }

        val title = Column<Clip, String?>("title")
        fun where(condition: Condition) = Query("Clip", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Clip", this, ids)
        fun createFileVariables(widgets: List<StickerWidgetInput>? = null): JsonObject = buildJsonObject {
            if (widgets != null) this.put("widgets", jsonValue(widgets))
        }
        suspend fun createFile(account: AccountScope, id: String, source: String, widgets: List<StickerWidgetInput>? = null, prepare: Prepare? = null, maxBytes: ULong? = null): MutationReceipt<Clip> =
            account.upload(this, id, "create_file", UploadRequest(source, variables = buildJsonObject { if (widgets != null) this.put("widgets", jsonValue(widgets)) }, prepare = prepare, maxBytes = maxBytes))
        suspend fun stageCreateFile(account: AccountScope, source: String, prepare: Prepare? = null, maxBytes: ULong? = null): StagedUpload<Clip> =
            account.stage(Clip, "create_file", UploadRequest(source, prepare = prepare, maxBytes = maxBytes))
        suspend fun create(title: String? = null, account: AccountScope? = null): MutationReceipt<Clip> = onAccountOf(account) { database_, ordered_, epoch_ ->
            val values_ = buildJsonObject {
                title?.let { this.put("title", jsonValue(it)) }
            }
            MutationReceipt(database_, Clip, ordered_.create(epoch_, "Clip", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(title: GraphQLNullable<String> = GraphQLNullable.Omitted, account: AccountScope? = null): MutationReceipt<Clip> = onAccountOf(account) { database_, ordered_, epoch_ ->
        val values_ = buildJsonObject {
            this.put("title", title)
        }
        MutationReceipt(database_, Clip, ordered_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadSource? = null, account: AccountScope? = null): MutationReceipt<Clip> = Clip.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Clip> = Clip.destroy(id, account)
    suspend fun createFile(account: AccountScope, request: UploadRequest): MutationReceipt<Clip> = account.upload(Clip, id, "create_file", request)
}

@Immutable
@Serializable
class ClipCreateFileResult

@Immutable
data class Inbox(
    override val id: String,
    val title: String,
    val stance: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap(),
    override val rowRevision: Long = 0,
    override val estimatedBytes: ULong = 0u
) : DBModel {
    override val modelName get() = "Inbox"
    companion object : ModelDecoder<Inbox> {
        override val layout = "2069e3ab4783e460fc6cb6b14859d746510bc8544c6342cb14c9ff5a89f93b0c"
        override val modelName = "Inbox"
        override fun decode(row: WireRow): Inbox {
            val r = CellReader(row.cells)
            return Inbox(
                id = row.id,
                title = r.req(0, Cell::asText),
                stance = r.opt(1, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
                rowRevision = row.revision,
                estimatedBytes = row.estimatedBytes,
            )
        }

        val title = Column<Inbox, String>("title")
        val stance = Column<Inbox, String?>("stance")
        fun where(condition: Condition) = Query("Inbox", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Inbox", this, ids)
        fun shelf(list: ContainerShelfKind): Query<Inbox> {
            var built_ = Query("Inbox", this, relation = "shelf", params = buildJsonObject { this.put("list", jsonValue(list)) })
            return built_
        }
        suspend fun create(title: String, stance: String? = null, account: AccountScope? = null): MutationReceipt<Inbox> = onAccountOf(account) { database_, ordered_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("title", jsonValue(title))
                stance?.let { this.put("stance", jsonValue(it)) }
            }
            MutationReceipt(database_, Inbox, ordered_.create(epoch_, "Inbox", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(title: String? = null, stance: GraphQLNullable<String> = GraphQLNullable.Omitted, account: AccountScope? = null): MutationReceipt<Inbox> = onAccountOf(account) { database_, ordered_, epoch_ ->
        val values_ = buildJsonObject {
            title?.let { this.put("title", jsonValue(it)) }
            this.put("stance", stance)
        }
        MutationReceipt(database_, Inbox, ordered_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadSource? = null, account: AccountScope? = null): MutationReceipt<Inbox> = Inbox.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Inbox> = Inbox.destroy(id, account)
    suspend fun move(list: ContainerShelfKind, account: AccountScope? = null): MutationReceipt<Inbox> = this.act("move", JsonObject(parseJsonObject("""{"stance":{"${'$'}map":{"from":"list","map":{"PRIMARY":"main","SECONDARY":"other"}}}}""") + mapOf()), JsonObject(mapOf("list" to jsonValue(list))), Inbox, account)
}

@Immutable
data class MediaEntry(
    override val id: String,
    val text: String? = null,
    val sequenceNumber: Long,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap(),
    override val rowRevision: Long = 0,
    override val estimatedBytes: ULong = 0u
) : DBModel {
    override val modelName get() = "MediaEntry"
    companion object : ModelDecoder<MediaEntry> {
        override val layout = "3d888c449b3a4c344c639a31a082e38e5a5856b16d4b932bfd615e243785441d"
        override val modelName = "MediaEntry"
        override fun decode(row: WireRow): MediaEntry {
            val r = CellReader(row.cells)
            return MediaEntry(
                id = row.id,
                text = r.opt(0, Cell::asText),
                sequenceNumber = r.req(1, Cell::asInt),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
                rowRevision = row.revision,
                estimatedBytes = row.estimatedBytes,
            )
        }

        val text = Column<MediaEntry, String?>("text")
        val sequenceNumber = Column<MediaEntry, Long>("sequenceNumber")
        fun where(condition: Condition) = Query("MediaEntry", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("MediaEntry", this, ids)
        fun media(chatId: String): Query<MediaEntry> {
            var built_ = Query("MediaEntry", this, relation = "media", params = buildJsonObject { this.put("chatId", jsonValue(chatId)) })
            return built_
        }
        suspend fun create(text: String? = null, sequenceNumber: Long, account: AccountScope? = null): MutationReceipt<MediaEntry> = onAccountOf(account) { database_, ordered_, epoch_ ->
            val values_ = buildJsonObject {
                text?.let { this.put("text", jsonValue(it)) }
                this.put("sequenceNumber", jsonValue(sequenceNumber))
            }
            MutationReceipt(database_, MediaEntry, ordered_.create(epoch_, "MediaEntry", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(text: GraphQLNullable<String> = GraphQLNullable.Omitted, sequenceNumber: Long? = null, account: AccountScope? = null): MutationReceipt<MediaEntry> = onAccountOf(account) { database_, ordered_, epoch_ ->
        val values_ = buildJsonObject {
            this.put("text", text)
            sequenceNumber?.let { this.put("sequenceNumber", jsonValue(it)) }
        }
        MutationReceipt(database_, MediaEntry, ordered_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadSource? = null, account: AccountScope? = null): MutationReceipt<MediaEntry> = MediaEntry.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<MediaEntry> = MediaEntry.destroy(id, account)
}

@Immutable
data class Roster(
    override val id: String,
    val title: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap(),
    override val rowRevision: Long = 0,
    override val estimatedBytes: ULong = 0u
) : DBModel {
    override val modelName get() = "Roster"
    companion object : ModelDecoder<Roster> {
        override val layout = "e9226eea266088346d0e13a891650ac9945faf4f4ff01bfa2daed4a6ed5e8092"
        override val modelName = "Roster"
        override fun decode(row: WireRow): Roster {
            val r = CellReader(row.cells)
            return Roster(
                id = row.id,
                title = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
                rowRevision = row.revision,
                estimatedBytes = row.estimatedBytes,
            )
        }

        val title = Column<Roster, String>("title")
        fun where(condition: Condition) = Query("Roster", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Roster", this, ids)
        fun containerMembers(containerId: String, order: ContainerMemberOrder = ContainerMemberOrder.Joined): Query<Roster> {
            var built_ = Query("Roster", this, relation = "container_members", params = buildJsonObject { this.put("containerId", jsonValue(containerId)); this.put("order", jsonValue(order)) })
            return built_
        }
        fun completeMembers(containerId: String, order: ContainerMemberOrder = ContainerMemberOrder.Joined): Query<Roster> {
            var built_ = Query("Roster", this, relation = "complete_members", params = buildJsonObject { this.put("containerId", jsonValue(containerId)); this.put("order", jsonValue(order)) })
            return built_
        }
        suspend fun create(title: String, account: AccountScope? = null): MutationReceipt<Roster> = onAccountOf(account) { database_, ordered_, epoch_ ->
            val values_ = buildJsonObject {
                this.put("title", jsonValue(title))
            }
            MutationReceipt(database_, Roster, ordered_.create(epoch_, "Roster", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(title: String? = null, account: AccountScope? = null): MutationReceipt<Roster> = onAccountOf(account) { database_, ordered_, epoch_ ->
        val values_ = buildJsonObject {
            title?.let { this.put("title", jsonValue(it)) }
        }
        MutationReceipt(database_, Roster, ordered_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun retry(action: String?, source: UploadSource? = null, account: AccountScope? = null): MutationReceipt<Roster> = Roster.retry(id, action, source, account)
    suspend fun destroy(account: AccountScope? = null): MutationReceipt<Roster> = Roster.destroy(id, account)
}

@Immutable
@Serializable(with = ContainerShelfKindSerializer::class)
sealed class ContainerShelfKind(override val raw: String) : RawEnum {
    data object Primary : ContainerShelfKind("PRIMARY")
    data object Secondary : ContainerShelfKind("SECONDARY")
    @Immutable
    data class Unknown(val value: String) : ContainerShelfKind(value)

    companion object {
        fun of(raw: String): ContainerShelfKind = when (raw) {
            "PRIMARY" -> Primary
            "SECONDARY" -> Secondary
            else -> Unknown(raw)
        }
    }
}

object ContainerShelfKindSerializer : RawEnumSerializer<ContainerShelfKind>("ContainerShelfKind", { ContainerShelfKind.of(it) })

@Immutable
@Serializable(with = ContainerMemberOrderSerializer::class)
sealed class ContainerMemberOrder(override val raw: String) : RawEnum {
    data object Joined : ContainerMemberOrder("JOINED")
    data object OwnerFirst : ContainerMemberOrder("OWNER_FIRST")
    @Immutable
    data class Unknown(val value: String) : ContainerMemberOrder(value)

    companion object {
        fun of(raw: String): ContainerMemberOrder = when (raw) {
            "JOINED" -> Joined
            "OWNER_FIRST" -> OwnerFirst
            else -> Unknown(raw)
        }
    }
}

object ContainerMemberOrderSerializer : RawEnumSerializer<ContainerMemberOrder>("ContainerMemberOrder", { ContainerMemberOrder.of(it) })

@Immutable
@Serializable(with = WidgetKindSerializer::class)
sealed class WidgetKind(override val raw: String) : RawEnum {
    data object Emoji : WidgetKind("EMOJI")
    data object Gif : WidgetKind("GIF")
    @Immutable
    data class Unknown(val value: String) : WidgetKind(value)

    companion object {
        fun of(raw: String): WidgetKind = when (raw) {
            "EMOJI" -> Emoji
            "GIF" -> Gif
            else -> Unknown(raw)
        }
    }
}

object WidgetKindSerializer : RawEnumSerializer<WidgetKind>("WidgetKind", { WidgetKind.of(it) })

@Immutable
@Serializable(with = MoodSerializer::class)
sealed class Mood(override val raw: String) : RawEnum {
    data object Happy : Mood("HAPPY")
    data object Sad : Mood("SAD")
    @Immutable
    data class Unknown(val value: String) : Mood(value)

    companion object {
        fun of(raw: String): Mood = when (raw) {
            "HAPPY" -> Happy
            "SAD" -> Sad
            else -> Unknown(raw)
        }
    }
}

object MoodSerializer : RawEnumSerializer<Mood>("Mood", { Mood.of(it) })

@Immutable
@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class StickerWidgetInput(val kind: WidgetKind, val props: StickerPropsInput, @EncodeDefault(EncodeDefault.Mode.NEVER) val extras: GraphQLNullable<List<StickerPropsInput>> = GraphQLNullable.Omitted)

@Immutable
@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class StickerInput(val name: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val mood: GraphQLNullable<Mood> = GraphQLNullable.Omitted, val tags: List<String>, @EncodeDefault(EncodeDefault.Mode.NEVER) val notes: GraphQLNullable<List<String?>> = GraphQLNullable.Omitted, @EncodeDefault(EncodeDefault.Mode.NEVER) val size: GraphQLNullable<Long> = GraphQLNullable.Omitted, val pinned: Boolean, val owner: OwnerInput, @EncodeDefault(EncodeDefault.Mode.NEVER) val helpers: GraphQLNullable<List<OwnerInput>> = GraphQLNullable.Omitted, @EncodeDefault(EncodeDefault.Mode.NEVER) val recursive: GraphQLNullable<RecursiveInput> = GraphQLNullable.Omitted, @EncodeDefault(EncodeDefault.Mode.NEVER) val keywords: GraphQLNullable<KeywordInput> = GraphQLNullable.Omitted)

@Immutable
@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class MomentInput(val file: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val widgets: GraphQLNullable<List<WidgetInput>> = GraphQLNullable.Omitted)

@Immutable
@Serializable(with = StickerPropsInputSerializer::class)
sealed interface StickerPropsInput {
    @Immutable
    data class Text(val value: StickerTextInput) : StickerPropsInput
    @Immutable
    data class Emoji(val value: StickerEmojiInput) : StickerPropsInput
    @Immutable
    data class Tags(val value: List<String>) : StickerPropsInput
}

object StickerPropsInputSerializer : KSerializer<StickerPropsInput> {
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("StickerPropsInput")

    override fun serialize(encoder: Encoder, value: StickerPropsInput) {
        val (key, element) = when (value) {
            is StickerPropsInput.Text -> "text" to jsonValue(value.value)
            is StickerPropsInput.Emoji -> "emoji" to jsonValue(value.value)
            is StickerPropsInput.Tags -> "tags" to jsonValue(value.value)
        }
        (encoder as JsonEncoder).encodeJsonElement(JsonObject(mapOf(key to element)))
    }

    override fun deserialize(decoder: Decoder): StickerPropsInput = throw SerializationException("StickerPropsInput is only sent")
}

@Immutable
@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class OwnerInput(val id: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val nickname: GraphQLNullable<String> = GraphQLNullable.Omitted)

@Immutable
@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class RecursiveInput(@EncodeDefault(EncodeDefault.Mode.NEVER) val next: GraphQLNullable<RecursiveInput> = GraphQLNullable.Omitted, @EncodeDefault(EncodeDefault.Mode.NEVER) val choice: GraphQLNullable<RecursiveChoice> = GraphQLNullable.Omitted)

@Immutable
@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class KeywordInput(val default: String, val `when`: String, val `class`: String, val container: String, val encoder: String, val choices: List<KeywordChoice>)

@Immutable
@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class WidgetInput(val kind: WidgetKind, val settings: SettingsInput)

@Immutable
@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class StickerTextInput(val body: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val sizes: GraphQLNullable<List<Long>> = GraphQLNullable.Omitted)

@Immutable
@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class StickerEmojiInput(val code: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val kind: GraphQLNullable<WidgetKind> = GraphQLNullable.Omitted)

@Immutable
@Serializable(with = RecursiveChoiceSerializer::class)
sealed interface RecursiveChoice {
    @Immutable
    data class Next(val value: RecursiveChoice) : RecursiveChoice
    @Immutable
    data class Value(val value: String) : RecursiveChoice
}

object RecursiveChoiceSerializer : KSerializer<RecursiveChoice> {
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("RecursiveChoice")

    override fun serialize(encoder: Encoder, value: RecursiveChoice) {
        val (key, element) = when (value) {
            is RecursiveChoice.Next -> "next" to jsonValue(value.value)
            is RecursiveChoice.Value -> "value" to jsonValue(value.value)
        }
        (encoder as JsonEncoder).encodeJsonElement(JsonObject(mapOf(key to element)))
    }

    override fun deserialize(decoder: Decoder): RecursiveChoice = throw SerializationException("RecursiveChoice is only sent")
}

@Immutable
@Serializable(with = KeywordChoiceSerializer::class)
sealed interface KeywordChoice {
    @Immutable
    data class Default(val value: String) : KeywordChoice
    @Immutable
    data class When(val value: String) : KeywordChoice
}

object KeywordChoiceSerializer : KSerializer<KeywordChoice> {
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("KeywordChoice")

    override fun serialize(encoder: Encoder, value: KeywordChoice) {
        val (key, element) = when (value) {
            is KeywordChoice.Default -> "default" to jsonValue(value.value)
            is KeywordChoice.When -> "when" to jsonValue(value.value)
        }
        (encoder as JsonEncoder).encodeJsonElement(JsonObject(mapOf(key to element)))
    }

    override fun deserialize(decoder: Decoder): KeywordChoice = throw SerializationException("KeywordChoice is only sent")
}

@Immutable
@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class SettingsInput(val itemProps: JsonElement, @EncodeDefault(EncodeDefault.Mode.NEVER) val rotate: GraphQLNullable<Double> = GraphQLNullable.Omitted)

@Immutable
@Serializable
data class WhenResult(
    val default: String? = null,
    val `when`: String? = null,
    val container: String? = null,
    val r: String? = null,
)

@Immutable
@Serializable
data class QrCodeCreateResult(
    val url: String? = null,
)

@Immutable
@Serializable
data class StickerSendResult(
    val id: String? = null,
)

@Immutable
@Serializable
data class PricingLoadResult(
    val rewards: List<PricingLoadResultRewards>? = null,
    val videoIntro: PricingLoadResultVideoIntro? = null,
    val tags: List<String>? = null,
    val spec: JsonElement? = null,
)

@Immutable
@Serializable
data class PricingLoadResultRewards(
    val id: String? = null,
    val name: String? = null,
    val price: Long? = null,
)

@Immutable
@Serializable
data class PricingLoadResultVideoIntro(
    val enabled: Boolean? = null,
)

@Immutable
@Serializable
data class MomentCreateResult(
    val moment: MomentCreateResultMoment? = null,
)

@Immutable
@Serializable
data class MomentCreateResultMoment(
    val id: String? = null,
)

@Immutable
@Serializable
data class GiphyLoadResult(
    val id: String? = null,
    val url: String? = null,
    val isSticker: Boolean? = null,
)

@Immutable
@Serializable
data class WidgetsSendResult(
    val id: String? = null,
)

@Immutable
@Serializable
data class ContainerMediaLoadResult(
    val visualCount: Long? = null,
    val audioCount: Long? = null,
    val rewardCount: Long? = null,
    val latestSequence: Long? = null,
)

object Operations {
    suspend fun `when`(`class`: String, account: AccountScope? = null): WhenResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        variables_["class"] = jsonValue(`class`)
        return DBlayer.call("KeywordCall", JsonObject(variables_), WhenResult.serializer(), account)
    }
    suspend fun qrCodeCreate(kind: String, size: Long? = null, account: AccountScope? = null): QrCodeCreateResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        variables_["kind"] = jsonValue(kind)
        if (size != null) variables_["size"] = jsonValue(size)
        return DBlayer.call("QrCodeCreate", JsonObject(mapOf("input" to JsonObject(variables_))), QrCodeCreateResult.serializer(), account)
    }
    suspend fun signOut(account: AccountScope? = null) {
        val variables_ = mutableMapOf<String, JsonElement>()
        DBlayer.call("SignOut", JsonObject(variables_), account)
    }
    suspend fun stickerSend(input: StickerInput, account: AccountScope? = null): StickerSendResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        variables_["input"] = jsonValue(input)
        return DBlayer.call("StickerSend", JsonObject(variables_), StickerSendResult.serializer(), account)
    }
    suspend fun pricingLoad(account: AccountScope? = null): PricingLoadResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        return DBlayer.call("Pricing", JsonObject(variables_), PricingLoadResult.serializer(), account)
    }
    suspend fun momentCreate(input: MomentInput, account: AccountScope? = null): MomentCreateResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        variables_["input"] = jsonValue(input)
        return DBlayer.call("MomentCreate", JsonObject(variables_), MomentCreateResult.serializer(), account)
    }
    suspend fun giphyLoad(type: String, limit: Long? = null, account: AccountScope? = null): List<GiphyLoadResult> {
        val variables_ = mutableMapOf<String, JsonElement>()
        variables_["type"] = jsonValue(type)
        if (limit != null) variables_["limit"] = jsonValue(limit)
        return DBlayer.call("Giphy", JsonObject(variables_), ListSerializer(GiphyLoadResult.serializer()), account)
    }
    suspend fun widgetsSend(widgets: List<StickerWidgetInput>? = null, account: AccountScope? = null): WidgetsSendResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        if (widgets != null) variables_["widgets"] = jsonValue(widgets)
        return DBlayer.call("WidgetsSend", JsonObject(variables_), WidgetsSendResult.serializer(), account)
    }
    suspend fun containerMediaLoad(containerId: String, upToSequence: Long? = null, account: AccountScope? = null): ContainerMediaLoadResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        variables_["containerId"] = jsonValue(containerId)
        if (upToSequence != null) variables_["upToSequence"] = jsonValue(upToSequence)
        return DBlayer.call("containerMedia", JsonObject(variables_), ContainerMediaLoadResult.serializer(), account)
    }
}

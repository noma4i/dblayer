package com.noma4i.dblayer.models

import androidx.compose.runtime.Immutable
import com.noma4i.dblayer.*
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
data class BlobProfile(
    override val id: String,
    val label: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
) : DBModel {
    override val modelName get() = "BlobProfile"
    companion object : ModelDecoder<BlobProfile> {
        override val layout = "cae70f3b4f7f7821a632570ad1a489bc597e5b848b1ccc6df664113173d98031"
        override val modelName = "BlobProfile"
        override fun decode(row: WireRow): BlobProfile {
            val r = CellReader(row.cells)
            return BlobProfile(
                id = row.id,
                label = r.opt(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                uploads = row.uploads.associate { it.action to UploadInfo.of(it.upload) },
            )
        }

        val label = Column<BlobProfile, String?>("label")
        fun where(condition: Condition) = Query("BlobProfile", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("BlobProfile", this, ids)
        val current: Query<BlobProfile> get() {
            var built_ = Query("BlobProfile", this, relation = "current", params = buildJsonObject {  })
            return built_
        }
        suspend fun setVideo(video: Blob, account_: AccountScope? = null) = requireNotNull(BlobProfile.current.read(account_).firstOrNull()) { "singleton is missing" }.setVideo(video, account_)
        suspend fun create(label: String? = null, account_: AccountScope? = null): BlobProfile = onAccount(account_) { database_, epoch_ ->
            val values_ = buildJsonObject {
                label?.let { put("label", jsonValue(it)) }
            }
            read(database_.create(epoch_, "BlobProfile", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(label: GraphQLNullable<String> = GraphQLNullable.Omitted, account_: AccountScope? = null): BlobProfile = onAccount(account_) { database_, epoch_ ->
        val values_ = buildJsonObject {
            put("label", label)
        }
        BlobProfile.read(database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun setVideo(video: Blob, account_: AccountScope? = null): BlobProfile = act("set_video", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf("video" to jsonValue(video))), BlobProfile, account_)
}

@Immutable
data class Clip(
    override val id: String,
    val title: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
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
            )
        }

        val title = Column<Clip, String?>("title")
        fun where(condition: Condition) = Query("Clip", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Clip", this, ids)
        fun createFileVariables(widgets: List<StickerWidgetInput>? = null): JsonObject = buildJsonObject {
            if (widgets != null) put("widgets", jsonValue(widgets))
        }
        suspend fun create(title: String? = null, account_: AccountScope? = null): Clip = onAccount(account_) { database_, epoch_ ->
            val values_ = buildJsonObject {
                title?.let { put("title", jsonValue(it)) }
            }
            read(database_.create(epoch_, "Clip", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(title: GraphQLNullable<String> = GraphQLNullable.Omitted, account_: AccountScope? = null): Clip = onAccount(account_) { database_, epoch_ ->
        val values_ = buildJsonObject {
            put("title", title)
        }
        Clip.read(database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun createFile(account_: AccountScope, request: UploadRequest): ActionAnswer<Clip, ClipCreateFileResult> = account_.upload(this, "create_file", request, ClipCreateFileResult.serializer())
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
    override val uploads: Map<String, UploadInfo> = emptyMap()
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
            )
        }

        val title = Column<Inbox, String>("title")
        val stance = Column<Inbox, String?>("stance")
        fun where(condition: Condition) = Query("Inbox", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Inbox", this, ids)
        fun shelf(list: ContainerShelfKind): Query<Inbox> {
            var built_ = Query("Inbox", this, relation = "shelf", params = buildJsonObject { put("list", jsonValue(list)) })
            return built_
        }
        suspend fun create(title: String, stance: String? = null, account_: AccountScope? = null): Inbox = onAccount(account_) { database_, epoch_ ->
            val values_ = buildJsonObject {
                put("title", jsonValue(title))
                stance?.let { put("stance", jsonValue(it)) }
            }
            read(database_.create(epoch_, "Inbox", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(title: String? = null, stance: GraphQLNullable<String> = GraphQLNullable.Omitted, account_: AccountScope? = null): Inbox = onAccount(account_) { database_, epoch_ ->
        val values_ = buildJsonObject {
            title?.let { put("title", jsonValue(it)) }
            put("stance", stance)
        }
        Inbox.read(database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
    suspend fun move(list: ContainerShelfKind, account_: AccountScope? = null): Inbox = act("move", JsonObject(parseJsonObject("""{"stance":{"${'$'}map":{"from":"list","map":{"PRIMARY":"main","SECONDARY":"other"}}}}""") + mapOf()), JsonObject(mapOf("list" to jsonValue(list))), Inbox, account_)
}

@Immutable
data class MediaEntry(
    override val id: String,
    val text: String? = null,
    val sequenceNumber: Long,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
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
            )
        }

        val text = Column<MediaEntry, String?>("text")
        val sequenceNumber = Column<MediaEntry, Long>("sequenceNumber")
        fun where(condition: Condition) = Query("MediaEntry", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("MediaEntry", this, ids)
        fun media(chatId: String): Query<MediaEntry> {
            var built_ = Query("MediaEntry", this, relation = "media", params = buildJsonObject { put("chatId", jsonValue(chatId)) })
            return built_
        }
        suspend fun create(text: String? = null, sequenceNumber: Long, account_: AccountScope? = null): MediaEntry = onAccount(account_) { database_, epoch_ ->
            val values_ = buildJsonObject {
                text?.let { put("text", jsonValue(it)) }
                put("sequenceNumber", jsonValue(sequenceNumber))
            }
            read(database_.create(epoch_, "MediaEntry", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(text: GraphQLNullable<String> = GraphQLNullable.Omitted, sequenceNumber: Long? = null, account_: AccountScope? = null): MediaEntry = onAccount(account_) { database_, epoch_ ->
        val values_ = buildJsonObject {
            put("text", text)
            sequenceNumber?.let { put("sequenceNumber", jsonValue(it)) }
        }
        MediaEntry.read(database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
}

@Immutable
data class Roster(
    override val id: String,
    val title: String,
    override val sync: SyncInfo = SyncInfo(),
    override val uploads: Map<String, UploadInfo> = emptyMap()
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
            )
        }

        val title = Column<Roster, String>("title")
        fun where(condition: Condition) = Query("Roster", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Roster", this, ids)
        fun containerMembers(containerId: String, order: ContainerMemberOrder = ContainerMemberOrder.Joined): Query<Roster> {
            var built_ = Query("Roster", this, relation = "container_members", params = buildJsonObject { put("containerId", jsonValue(containerId)); put("order", jsonValue(order)) })
            return built_
        }
        fun completeMembers(containerId: String, order: ContainerMemberOrder = ContainerMemberOrder.Joined): Query<Roster> {
            var built_ = Query("Roster", this, relation = "complete_members", params = buildJsonObject { put("containerId", jsonValue(containerId)); put("order", jsonValue(order)) })
            return built_
        }
        suspend fun create(title: String, account_: AccountScope? = null): Roster = onAccount(account_) { database_, epoch_ ->
            val values_ = buildJsonObject {
                put("title", jsonValue(title))
            }
            read(database_.create(epoch_, "Roster", values_.jsonText(), "{}"))
        }
    }
    suspend fun update(title: String? = null, account_: AccountScope? = null): Roster = onAccount(account_) { database_, epoch_ ->
        val values_ = buildJsonObject {
            title?.let { put("title", jsonValue(it)) }
        }
        Roster.read(database_.update(epoch_, this.modelName, this.id, values_.jsonText(), "{}"))
    }
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
    @Serializable(with = IDText::class) val id: String? = null,
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
    @Serializable(with = IDText::class) val id: String? = null,
)

@Immutable
@Serializable
data class GiphyLoadResult(
    @Serializable(with = IDText::class) val id: String? = null,
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
    suspend fun `when`(`class`: String, account_: AccountScope? = null): WhenResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        variables_["class"] = jsonValue(`class`)
        return DBlayer.run("KeywordCall", JsonObject(variables_), WhenResult.serializer(), account_)
    }
    suspend fun qrCodeCreate(kind: String, size: Long? = null, account_: AccountScope? = null): QrCodeCreateResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        variables_["kind"] = jsonValue(kind)
        if (size != null) variables_["size"] = jsonValue(size)
        return DBlayer.run("QrCodeCreate", JsonObject(mapOf("input" to JsonObject(variables_))), QrCodeCreateResult.serializer(), account_)
    }
    suspend fun accountNames(scope: String, account: String, account_: AccountScope? = null) {
        val variables_ = mutableMapOf<String, JsonElement>()
        variables_["scope"] = jsonValue(scope)
        variables_["account"] = jsonValue(account)
        DBlayer.run("AccountNames", JsonObject(variables_), account_)
    }
    suspend fun signOut(account_: AccountScope? = null) {
        val variables_ = mutableMapOf<String, JsonElement>()
        DBlayer.run("SignOut", JsonObject(variables_), account_)
    }
    suspend fun stickerSend(input: StickerInput, account_: AccountScope? = null): StickerSendResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        variables_["input"] = jsonValue(input)
        return DBlayer.run("StickerSend", JsonObject(variables_), StickerSendResult.serializer(), account_)
    }
    suspend fun pricingLoad(account_: AccountScope? = null): PricingLoadResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        return DBlayer.run("Pricing", JsonObject(variables_), PricingLoadResult.serializer(), account_)
    }
    suspend fun momentCreate(input: MomentInput, account_: AccountScope? = null): MomentCreateResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        variables_["input"] = jsonValue(input)
        return DBlayer.run("MomentCreate", JsonObject(variables_), MomentCreateResult.serializer(), account_)
    }
    suspend fun giphyLoad(type: String, limit: Long? = null, account_: AccountScope? = null): List<GiphyLoadResult> {
        val variables_ = mutableMapOf<String, JsonElement>()
        variables_["type"] = jsonValue(type)
        if (limit != null) variables_["limit"] = jsonValue(limit)
        return DBlayer.run("Giphy", JsonObject(variables_), kotlinx.serialization.builtins.ListSerializer(GiphyLoadResult.serializer()), account_)
    }
    suspend fun widgetsSend(widgets: List<StickerWidgetInput>? = null, account_: AccountScope? = null): WidgetsSendResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        if (widgets != null) variables_["widgets"] = jsonValue(widgets)
        return DBlayer.run("WidgetsSend", JsonObject(variables_), WidgetsSendResult.serializer(), account_)
    }
    suspend fun containerMediaLoad(containerId: String, upToSequence: Long? = null, account_: AccountScope? = null): ContainerMediaLoadResult {
        val variables_ = mutableMapOf<String, JsonElement>()
        variables_["containerId"] = jsonValue(containerId)
        if (upToSequence != null) variables_["upToSequence"] = jsonValue(upToSequence)
        return DBlayer.run("containerMedia", JsonObject(variables_), ContainerMediaLoadResult.serializer(), account_)
    }
}


val BlobUpload.Companion.VideoIntro: BlobUpload get() = BlobUpload("video_intro", "BlobCreate")

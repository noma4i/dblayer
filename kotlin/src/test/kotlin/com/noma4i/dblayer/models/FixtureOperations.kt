package com.noma4i.dblayer.models

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

data class Clip(
    override val id: String,
    val title: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
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
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val title = Column<Clip, String?>("title")
        fun find(id: String) = Query("Clip", this).find(id)
        fun where(condition: Condition) = Query("Clip", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Clip", this, ids)
        fun entity(id: String) = entityOf("Clip", this, id)
        fun createFileVariables(widgets: List<StickerWidgetInput>? = null): JsonObject = buildJsonObject {
            if (widgets != null) put("widgets", jsonValue(widgets))
        }
        suspend fun create(title: String? = null): Clip {
            val values = buildJsonObject {
                title?.let { put("title", jsonValue(it)) }
            }
            return read(DBlayer.database.create("Clip", values.toString(), "{}"))
        }
    }
    suspend fun update(title: String? = null): Clip {
        val values = buildJsonObject {
            title?.let { put("title", jsonValue(it)) }
        }
        return Clip.read(DBlayer.database.update(modelName, id, values.toString(), "{}"))
    }
}

data class Inbox(
    override val id: String,
    val title: String,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "Inbox"
    companion object : ModelDecoder<Inbox> {
        override val layout = "e9226eea266088346d0e13a891650ac9945faf4f4ff01bfa2daed4a6ed5e8092"
        override val modelName = "Inbox"
        override fun decode(row: WireRow): Inbox {
            val r = CellReader(row.cells)
            return Inbox(
                id = row.id,
                title = r.req(0, Cell::asText),
                sync = SyncInfo.of(row.sync),
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val title = Column<Inbox, String>("title")
        fun find(id: String) = Query("Inbox", this).find(id)
        fun where(condition: Condition) = Query("Inbox", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Inbox", this, ids)
        fun entity(id: String) = entityOf("Inbox", this, id)
        fun shelf(list: ChatShelfKind): Query<Inbox> {
            var built = Query("Inbox", this, relation = "shelf", params = buildJsonObject { put("list", jsonValue(list)) })
            return built
        }
        suspend fun create(title: String): Inbox {
            val values = buildJsonObject {
                put("title", jsonValue(title))
            }
            return read(DBlayer.database.create("Inbox", values.toString(), "{}"))
        }
    }
    suspend fun update(title: String? = null): Inbox {
        val values = buildJsonObject {
            title?.let { put("title", jsonValue(it)) }
        }
        return Inbox.read(DBlayer.database.update(modelName, id, values.toString(), "{}"))
    }
}

data class Roster(
    override val id: String,
    val title: String,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
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
                upload = row.upload?.let { UploadInfo.of(it) },
            )
        }

        val title = Column<Roster, String>("title")
        fun find(id: String) = Query("Roster", this).find(id)
        fun where(condition: Condition) = Query("Roster", this).where(condition)
        fun ids(ids: List<String>) = idsQuery("Roster", this, ids)
        fun entity(id: String) = entityOf("Roster", this, id)
        fun chatMembers(chatId: String, order: ChatMemberOrder = ChatMemberOrder.Joined): Query<Roster> {
            var built = Query("Roster", this, relation = "chat_members", params = buildJsonObject { put("chatId", jsonValue(chatId)); put("order", jsonValue(order)) })
            return built
        }
        suspend fun create(title: String): Roster {
            val values = buildJsonObject {
                put("title", jsonValue(title))
            }
            return read(DBlayer.database.create("Roster", values.toString(), "{}"))
        }
    }
    suspend fun update(title: String? = null): Roster {
        val values = buildJsonObject {
            title?.let { put("title", jsonValue(it)) }
        }
        return Roster.read(DBlayer.database.update(modelName, id, values.toString(), "{}"))
    }
}

@Serializable(with = ChatShelfKindSerializer::class)
sealed class ChatShelfKind(override val raw: String) : RawEnum {
    data object Primary : ChatShelfKind("PRIMARY")
    data object Secondary : ChatShelfKind("SECONDARY")
    data class Unknown(val value: String) : ChatShelfKind(value)

    companion object {
        fun of(raw: String): ChatShelfKind = when (raw) {
            "PRIMARY" -> Primary
            "SECONDARY" -> Secondary
            else -> Unknown(raw)
        }
    }
}

object ChatShelfKindSerializer : RawEnumSerializer<ChatShelfKind>("ChatShelfKind", { ChatShelfKind.of(it) })

@Serializable(with = ChatMemberOrderSerializer::class)
sealed class ChatMemberOrder(override val raw: String) : RawEnum {
    data object Joined : ChatMemberOrder("JOINED")
    data object OwnerFirst : ChatMemberOrder("OWNER_FIRST")
    data class Unknown(val value: String) : ChatMemberOrder(value)

    companion object {
        fun of(raw: String): ChatMemberOrder = when (raw) {
            "JOINED" -> Joined
            "OWNER_FIRST" -> OwnerFirst
            else -> Unknown(raw)
        }
    }
}

object ChatMemberOrderSerializer : RawEnumSerializer<ChatMemberOrder>("ChatMemberOrder", { ChatMemberOrder.of(it) })

@Serializable(with = WidgetKindSerializer::class)
sealed class WidgetKind(override val raw: String) : RawEnum {
    data object Emoji : WidgetKind("EMOJI")
    data object Gif : WidgetKind("GIF")
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

@Serializable(with = MoodSerializer::class)
sealed class Mood(override val raw: String) : RawEnum {
    data object Happy : Mood("HAPPY")
    data object Sad : Mood("SAD")
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

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class StickerWidgetInput(val kind: WidgetKind, val props: StickerPropsInput, @EncodeDefault(EncodeDefault.Mode.NEVER) val extras: GraphQLNullable<List<StickerPropsInput>> = GraphQLNullable.Omitted)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class StickerInput(val name: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val mood: GraphQLNullable<Mood> = GraphQLNullable.Omitted, val tags: List<String>, @EncodeDefault(EncodeDefault.Mode.NEVER) val notes: GraphQLNullable<List<String?>> = GraphQLNullable.Omitted, @EncodeDefault(EncodeDefault.Mode.NEVER) val size: GraphQLNullable<Int> = GraphQLNullable.Omitted, val pinned: Boolean, val owner: OwnerInput, @EncodeDefault(EncodeDefault.Mode.NEVER) val helpers: GraphQLNullable<List<OwnerInput>> = GraphQLNullable.Omitted)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class MomentInput(val file: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val widgets: GraphQLNullable<List<WidgetInput>> = GraphQLNullable.Omitted)

@Serializable(with = StickerPropsInputSerializer::class)
sealed interface StickerPropsInput {
    data class Text(val value: StickerTextInput) : StickerPropsInput
    data class Emoji(val value: StickerEmojiInput) : StickerPropsInput
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

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class OwnerInput(val id: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val nickname: GraphQLNullable<String> = GraphQLNullable.Omitted)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class WidgetInput(val kind: WidgetKind, val settings: SettingsInput)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class StickerTextInput(val body: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val sizes: GraphQLNullable<List<Int>> = GraphQLNullable.Omitted)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class StickerEmojiInput(val code: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val kind: GraphQLNullable<WidgetKind> = GraphQLNullable.Omitted)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class SettingsInput(val itemProps: JsonElement, @EncodeDefault(EncodeDefault.Mode.NEVER) val rotate: GraphQLNullable<Double> = GraphQLNullable.Omitted)

@Serializable
data class QrCodeCreateResult(
    val url: String? = null,
)

@Serializable
data class StickerSendResult(
    val id: String? = null,
)

@Serializable
data class PricingLoadResult(
    val gifts: List<PricingLoadResultGifts>? = null,
    val videoIntro: PricingLoadResultVideoIntro? = null,
    val tags: List<String>? = null,
    val spec: JsonElement? = null,
)

@Serializable
data class PricingLoadResultGifts(
    @Serializable(with = IDText::class) val id: String? = null,
    val name: String? = null,
    val price: Long? = null,
)

@Serializable
data class PricingLoadResultVideoIntro(
    val enabled: Boolean? = null,
)

@Serializable
data class MomentCreateResult(
    val moment: MomentCreateResultMoment? = null,
)

@Serializable
data class MomentCreateResultMoment(
    @Serializable(with = IDText::class) val id: String? = null,
)

@Serializable
data class GiphyLoadResult(
    @Serializable(with = IDText::class) val id: String? = null,
    val url: String? = null,
    val isSticker: Boolean? = null,
)

@Serializable
data class WidgetsSendResult(
    val id: String? = null,
)

object Operations {
    suspend fun qrCodeCreate(kind: String, size: Int? = null): QrCodeCreateResult {
        val variables = mutableMapOf<String, JsonElement>()
        variables["kind"] = jsonValue(kind)
        if (size != null) variables["size"] = jsonValue(size)
        return DBlayer.run("QrCodeCreate", JsonObject(mapOf("input" to JsonObject(variables))), QrCodeCreateResult.serializer())
    }
    suspend fun signOut() {
        val variables = mutableMapOf<String, JsonElement>()
        DBlayer.run("SignOut", JsonObject(variables))
    }
    suspend fun stickerSend(input: StickerInput): StickerSendResult {
        val variables = mutableMapOf<String, JsonElement>()
        variables["input"] = jsonValue(input)
        return DBlayer.run("StickerSend", JsonObject(variables), StickerSendResult.serializer())
    }
    suspend fun pricingLoad(): PricingLoadResult {
        val variables = mutableMapOf<String, JsonElement>()
        return DBlayer.run("Pricing", JsonObject(variables), PricingLoadResult.serializer())
    }
    suspend fun momentCreate(input: MomentInput): MomentCreateResult {
        val variables = mutableMapOf<String, JsonElement>()
        variables["input"] = jsonValue(input)
        return DBlayer.run("MomentCreate", JsonObject(variables), MomentCreateResult.serializer())
    }
    suspend fun giphyLoad(type: String, limit: Int? = null): List<GiphyLoadResult> {
        val variables = mutableMapOf<String, JsonElement>()
        variables["type"] = jsonValue(type)
        if (limit != null) variables["limit"] = jsonValue(limit)
        return DBlayer.run("Giphy", JsonObject(variables), kotlinx.serialization.builtins.ListSerializer(GiphyLoadResult.serializer()))
    }
    suspend fun widgetsSend(widgets: List<StickerWidgetInput>? = null): WidgetsSendResult {
        val variables = mutableMapOf<String, JsonElement>()
        if (widgets != null) variables["widgets"] = jsonValue(widgets)
        return DBlayer.run("WidgetsSend", JsonObject(variables), WidgetsSendResult.serializer())
    }
}

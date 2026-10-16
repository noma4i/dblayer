package com.noma4i.dblayer.models

import com.noma4i.dblayer.*
import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*

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

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class StickerInput(val name: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val mood: GraphQLNullable<Mood> = GraphQLNullable.Omitted, val tags: List<String>, @EncodeDefault(EncodeDefault.Mode.NEVER) val notes: GraphQLNullable<List<String?>> = GraphQLNullable.Omitted, @EncodeDefault(EncodeDefault.Mode.NEVER) val size: GraphQLNullable<Int> = GraphQLNullable.Omitted, val pinned: Boolean, val owner: OwnerInput, @EncodeDefault(EncodeDefault.Mode.NEVER) val helpers: GraphQLNullable<List<OwnerInput>> = GraphQLNullable.Omitted)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class MomentInput(val file: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val widgets: GraphQLNullable<List<WidgetInput>> = GraphQLNullable.Omitted)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class OwnerInput(val id: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val nickname: GraphQLNullable<String> = GraphQLNullable.Omitted)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class WidgetInput(val kind: WidgetKind, val settings: SettingsInput)

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
}

package com.noma4i.dblayer.models

import com.noma4i.dblayer.*
import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class StickerInput(val name: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val mood: String? = null, val tags: List<String>, @EncodeDefault(EncodeDefault.Mode.NEVER) val notes: List<String?>? = null, @EncodeDefault(EncodeDefault.Mode.NEVER) val size: Int? = null, val pinned: Boolean, val owner: OwnerInput, @EncodeDefault(EncodeDefault.Mode.NEVER) val helpers: List<OwnerInput>? = null)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class MomentInput(val file: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val widgets: List<WidgetInput>? = null)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class OwnerInput(val id: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val nickname: String? = null)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class WidgetInput(val kind: String, val settings: SettingsInput)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class SettingsInput(val itemProps: JsonElement, @EncodeDefault(EncodeDefault.Mode.NEVER) val rotate: Double? = null)

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
}

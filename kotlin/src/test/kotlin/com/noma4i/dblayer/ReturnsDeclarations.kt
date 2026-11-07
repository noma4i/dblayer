package com.noma4i.dblayer

import com.noma4i.dblayer.models.Operations
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

val fixtureReturns: Map<String, String> = mapOf(
    "KeywordCall" to Operations.whenReturns,
    "QrCodeCreate" to Operations.qrCodeCreateReturns,
    "SignOut" to Operations.signOutReturns,
    "StickerSend" to Operations.stickerSendReturns,
    "Pricing" to Operations.pricingLoadReturns,
    "MomentCreate" to Operations.momentCreateReturns,
    "Giphy" to Operations.giphyLoadReturns,
    "WidgetsSend" to Operations.widgetsSendReturns,
    "containerMedia" to Operations.containerMediaLoadReturns,
)

const val RAW_RETURNS = """{"name":"raw","kind":{"type":"raw"}}"""

fun declaringReturns(defs: String): String = JsonArray((parseJson(defs) as JsonArray).map { entry ->
    val def = entry.jsonObject
    if ("returns" in def) def else JsonObject(def + ("returns" to JsonPrimitive(fixtureReturns[def.getValue("name").jsonPrimitive.content] ?: RAW_RETURNS)))
}).toString()

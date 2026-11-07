package com.noma4i.dblayer

import com.noma4i.dblayer.models.operationReturns
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

const val RAW_RETURNS = """{"name":"raw","kind":{"type":"raw"}}"""

fun declaringReturns(defs: String): String = JsonArray((parseJson(defs) as JsonArray).map { entry ->
    val def = entry.jsonObject
    if ("returns" in def) def else JsonObject(def + ("returns" to JsonPrimitive(operationReturns[def.getValue("name").jsonPrimitive.content] ?: RAW_RETURNS)))
}).toString()

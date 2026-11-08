package com.noma4i.dblayer

const val EMPTY_SPECS = """{"models":[]}"""

fun specs(models: String): String = """{"models":$models}"""

val modelsArrayJSON: String = (kotlinx.serialization.json.Json.parseToJsonElement(com.noma4i.dblayer.models.modelsJSON) as kotlinx.serialization.json.JsonObject).getValue("models").toString()

/** The schema with one server subscription: a schema holds one subscription, so a test of a model-level subscription runs on a schema that declares only that one. */
fun schema(onlySubscription: String, source: String = com.noma4i.dblayer.models.modelsJSON): String {
    fun keep(entries: kotlinx.serialization.json.JsonElement?) = entries?.let { all -> kotlinx.serialization.json.JsonArray((all as kotlinx.serialization.json.JsonArray).filter { ((it as kotlinx.serialization.json.JsonObject)["subscription"] as? kotlinx.serialization.json.JsonPrimitive)?.content == onlySubscription }) }
    return specs(kotlinx.serialization.json.JsonArray((kotlinx.serialization.json.Json.parseToJsonElement(source).let { (it as kotlinx.serialization.json.JsonObject).getValue("models") } as kotlinx.serialization.json.JsonArray).map { model ->
        val fields = (model as kotlinx.serialization.json.JsonObject).filterKeys { it != "change_feed" }.toMutableMap()
        keep(fields["listens"])?.let { fields["listens"] = it }
        (fields["relations"] as? kotlinx.serialization.json.JsonArray)?.let { relations ->
            fields["relations"] = kotlinx.serialization.json.JsonArray(relations.map { relation ->
                val parts = (relation as kotlinx.serialization.json.JsonObject).toMutableMap()
                keep(parts["updates"])?.let { parts["updates"] = it }
                kotlinx.serialization.json.JsonObject(parts)
            })
        }
        kotlinx.serialization.json.JsonObject(fields)
    }).toString())
}

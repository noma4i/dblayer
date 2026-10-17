package com.noma4i.dblayer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

private const val TYPENAME = "__typename"

abstract class UnionSerializer<Value : Any>(name: String) : KSerializer<Value> {
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor(name)

    abstract fun read(typename: String, body: JsonObject): Value

    abstract fun write(value: Value): Pair<String, JsonElement>

    override fun deserialize(decoder: Decoder): Value {
        val body = (decoder as JsonDecoder).decodeJsonElement().jsonObject
        val typename = body[TYPENAME]?.jsonPrimitive?.contentOrNull ?: throw SerializationException("the union value has no $TYPENAME")
        return read(typename, body)
    }

    override fun serialize(encoder: Encoder, value: Value) {
        val (typename, body) = write(value)
        (encoder as JsonEncoder).encodeJsonElement(JsonObject(mapOf(TYPENAME to JsonPrimitive(typename)) + body.jsonObject))
    }
}

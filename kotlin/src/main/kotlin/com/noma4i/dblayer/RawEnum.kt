package com.noma4i.dblayer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

interface RawEnum {
    val raw: String
}

open class RawEnumSerializer<Value : RawEnum>(name: String, private val parse: (String) -> Value) : KSerializer<Value> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor(name, PrimitiveKind.STRING)

    override fun deserialize(decoder: Decoder): Value = parse(decoder.decodeString())

    override fun serialize(encoder: Encoder, value: Value) = encoder.encodeString(value.raw)
}

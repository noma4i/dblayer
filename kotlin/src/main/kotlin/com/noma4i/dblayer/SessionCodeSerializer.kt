package com.noma4i.dblayer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

object SessionCodeSerializer : KSerializer<SessionCode> {
    override val descriptor = PrimitiveSerialDescriptor("SessionCode", PrimitiveKind.STRING)

    override fun deserialize(decoder: Decoder): SessionCode = SessionCode.valueOf(decoder.decodeString())

    override fun serialize(encoder: Encoder, value: SessionCode) = encoder.encodeString(value.name)
}

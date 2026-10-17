package com.noma4i.dblayer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

@Serializable
private data class EncodedGraphqlExtensions(val code: String?, val transactionDisposition: String?)

object GraphqlExtensionsSerializer : KSerializer<GraphqlExtensions> {
    override val descriptor = EncodedGraphqlExtensions.serializer().descriptor

    override fun deserialize(decoder: Decoder): GraphqlExtensions {
        val fields = EncodedGraphqlExtensions.serializer().deserialize(decoder)
        return GraphqlExtensions(fields.code, fields.transactionDisposition?.let(::transactionDisposition))
    }

    override fun serialize(encoder: Encoder, value: GraphqlExtensions) {
        EncodedGraphqlExtensions.serializer().serialize(encoder, EncodedGraphqlExtensions(
            value.code, value.transactionDisposition?.let(::transactionDispositionText),
        ))
    }
}

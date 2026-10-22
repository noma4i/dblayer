package com.noma4i.dblayer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObjectBuilder

interface GraphQLOperation {
    val operationName: String
    val document: String
}

interface GraphQLQuery<Data, Variables> : GraphQLOperation {
    val dataSerializer: KSerializer<Data>
    val variablesSerializer: KSerializer<Variables>
}

interface GraphQLMutation<Data, Variables> : GraphQLOperation {
    val dataSerializer: KSerializer<Data>
    val variablesSerializer: KSerializer<Variables>
}

interface GraphQLSubscription<Data, Variables> : GraphQLOperation {
    val dataSerializer: KSerializer<Data>
    val variablesSerializer: KSerializer<Variables>
}

@Serializable(with = GraphQLNullableSerializer::class)
sealed interface GraphQLNullable<out Wrapped> {
    data object Omitted : GraphQLNullable<Nothing>
    data object Null : GraphQLNullable<Nothing>
    data class Value<Wrapped>(val value: Wrapped) : GraphQLNullable<Wrapped>
}

class GraphQLNullableSerializer<Wrapped>(private val valueSerializer: KSerializer<Wrapped>) : KSerializer<GraphQLNullable<Wrapped>> {
    override val descriptor: SerialDescriptor = valueSerializer.descriptor

    override fun serialize(encoder: Encoder, value: GraphQLNullable<Wrapped>) {
        val json = encoder as JsonEncoder
        when (value) {
            GraphQLNullable.Omitted -> throw SerializationException("An omitted input must be excluded from its object")
            GraphQLNullable.Null -> json.encodeJsonElement(JsonNull)
            is GraphQLNullable.Value -> json.encodeJsonElement(json.json.encodeToJsonElement(valueSerializer, value.value))
        }
    }

    override fun deserialize(decoder: Decoder): GraphQLNullable<Wrapped> {
        val json = decoder as JsonDecoder
        val value = json.decodeJsonElement()
        return if (value == JsonNull) GraphQLNullable.Null else GraphQLNullable.Value(json.json.decodeFromJsonElement(valueSerializer, value))
    }
}
inline fun <reified Wrapped> JsonObjectBuilder.put(key: String, value: GraphQLNullable<Wrapped>) {
    when (value) {
        GraphQLNullable.Omitted -> Unit
        GraphQLNullable.Null -> put(key, JsonNull)
        is GraphQLNullable.Value -> put(key, jsonValue(value.value))
    }
}

package com.noma4i.dblayer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

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

data class GraphQLError(val message: String, val path: List<String>? = null, val extensions: Map<String, JsonElement>? = null) {
    val code: String? get() = (extensions?.get("code") as? JsonPrimitive)?.contentOrNull

    companion object {
        fun parse(element: JsonElement): GraphQLError? {
            val fields = runCatching { element.jsonObject }.getOrNull() ?: return null
            val message = fields["message"]?.jsonPrimitive?.contentOrNull ?: return null
            val path = (fields["path"] as? kotlinx.serialization.json.JsonArray)?.map { it.jsonPrimitive.content }
            val extensions = (fields["extensions"] as? kotlinx.serialization.json.JsonObject)?.toMap()
            return GraphQLError(message, path, extensions)
        }
    }
}

data class Response<Data>(val data: Data?, val errors: List<GraphQLError> = emptyList())

enum class SubscriptionFailureKind(val wire: String) { Transport("transport"), Graphql("graphql"), Internal("internal") }

data class SubscriptionFailure(val error: DbException) {
    val kind: SubscriptionFailureKind get() = when (error) {
        is DbException.Graphql -> SubscriptionFailureKind.Graphql
        is DbException.Network, is DbException.Http -> SubscriptionFailureKind.Transport
        else -> SubscriptionFailureKind.Internal
    }
    val message: String get() = when (error) {
        is DbException.Graphql -> error.messages.joinToString("\n")
        else -> error.toString()
    }
}

class SubscriptionHandle(private val onCancel: () -> Unit) {
    private val cancelled = java.util.concurrent.atomic.AtomicBoolean(false)
    fun cancel() {
        if (cancelled.compareAndSet(false, true)) onCancel()
    }
}

interface GraphQLTransport {
    suspend fun <Data, Variables> query(operation: GraphQLQuery<Data, Variables>, variables: Variables): Response<Data>
    suspend fun <Data, Variables> mutate(operation: GraphQLMutation<Data, Variables>, variables: Variables): Response<Data>
    fun <Data, Variables> subscribe(
        operation: GraphQLSubscription<Data, Variables>,
        variables: Variables,
        onReady: () -> Unit,
        onNext: (Response<Data>) -> Unit,
        onError: (SubscriptionFailure) -> Unit,
    ): SubscriptionHandle
}

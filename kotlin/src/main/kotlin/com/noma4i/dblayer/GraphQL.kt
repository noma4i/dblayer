package com.noma4i.dblayer

import kotlinx.serialization.KSerializer
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

sealed interface GraphQLNullable<out Wrapped> {
    data object Omitted : GraphQLNullable<Nothing>
    data object Null : GraphQLNullable<Nothing>
    data class Value<Wrapped>(val value: Wrapped) : GraphQLNullable<Wrapped>
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

data class SubscriptionFailure(val kind: SubscriptionFailureKind, val message: String, val details: String? = null)

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

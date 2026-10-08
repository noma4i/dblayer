package com.noma4i.dblayer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

class Condition(val json: JsonObject) {
    infix fun and(other: Condition) = Condition(buildJsonObject {
        put("type", "all_of")
        put("clauses", JsonArray(listOf(json, other.json)))
    })

    infix fun or(other: Condition) = Condition(buildJsonObject {
        put("type", "any_of")
        put("clauses", JsonArray(listOf(json, other.json)))
    })

    operator fun not() = Condition(buildJsonObject {
        put("type", "not")
        put("clause", json)
    })

    companion object {
        fun equal(field: String, value: JsonElement) = make("equal", field, "value" to value)
        fun notEqual(field: String, value: JsonElement) = make("not_equal", field, "value" to value)
        fun compare(field: String, op: String, value: JsonElement) = make("compare", field, "op" to JsonPrimitive(op), "value" to value)
        fun inList(field: String, values: List<JsonElement>) = make("in_list", field, "values" to JsonArray(values))
        fun isNull(field: String) = make("is_null", field)

        private fun make(type: String, field: String, vararg rest: Pair<String, JsonElement>) = Condition(buildJsonObject {
            put("type", type)
            put("field", field)
            rest.forEach { (key, value) -> put(key, value) }
        })
    }
}

data class Order(val field: String, val direction: String)

class Column<Model : DBModel, Value>(val name: String) {
    val asc get() = Order(name, "asc")
    val desc get() = Order(name, "desc")
    val isNull get() = Condition.isNull(name)
}

inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.eq(value: Value) = Condition.equal(name, jsonValue(value))
inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.neq(value: Value) = Condition.notEqual(name, jsonValue(value))
inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.lt(value: Value) = Condition.compare(name, "less", jsonValue(value))
inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.lte(value: Value) = Condition.compare(name, "less_or_equal", jsonValue(value))
inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.gt(value: Value) = Condition.compare(name, "greater", jsonValue(value))
inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.gte(value: Value) = Condition.compare(name, "greater_or_equal", jsonValue(value))
inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.within(values: List<Value>) = Condition.inList(name, values.map { jsonValue(it) })

class Query<Model : DBModel>(
    private val modelName: String,
    private val serializer: KSerializer<Model>,
    private val condition: Condition? = null,
    private val orders: List<Order> = emptyList(),
    private val limitCount: Int? = null,
    private val relation: String? = null,
    private val params: JsonObject = JsonObject(emptyMap()),
) {
    private fun copy(condition: Condition? = this.condition, orders: List<Order> = this.orders, limitCount: Int? = this.limitCount) =
        Query(modelName, serializer, condition, orders, limitCount, relation, params)

    fun where(next: Condition) = copy(condition = condition?.and(next) ?: next)
    fun order(vararg next: Order) = copy(orders = orders + next)
    fun limit(count: Int) = copy(limitCount = count)

    internal val json: String
        get() = buildJsonObject {
            put("model", modelName)
            condition?.let { put("where", it.json) }
            if (orders.isNotEmpty()) put("order", JsonArray(orders.map { buildJsonObject { put("field", it.field); put("direction", it.direction) } }))
            limitCount?.let { put("limit", it) }
            relation?.let { put("relation", it); put("params", params) }
        }.toString()

    val live: LiveSource<List<Model>>
        get() = LiveSource(emptyList(), { listener -> DBlayer.database.liveQuery(json, listener) }) { change -> change.decode(serializer) }

    val count: LiveSource<Long>
        get() = LiveSource(0L, { listener -> DBlayer.database.liveCount(json, listener) }) { change -> change.count ?: 0L }

    fun find(id: String): LiveSource<Model?> =
        LiveSource<Model?>(null, { listener -> DBlayer.database.liveFind(modelName, id, listener) }) { change -> change.decode(serializer).firstOrNull() }

    suspend fun refresh() = DBlayer.database.refresh(modelName, relation.orEmpty(), params.toString())
    suspend fun loadMore(): Boolean = DBlayer.database.loadMore(modelName, relation.orEmpty(), params.toString())
}

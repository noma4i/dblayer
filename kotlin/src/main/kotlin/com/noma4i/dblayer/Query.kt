package com.noma4i.dblayer

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
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
        fun search(text: String, wholeWords: Boolean = false) = Condition(buildJsonObject {
            put("type", "search")
            put("text", text)
            put("whole_words", wholeWords)
        })

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
    val ascNullsLast get() = Order(name, "asc_nulls_last")
    val descNullsFirst get() = Order(name, "desc_nulls_first")
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
    internal val decoder: ModelDecoder<Model>,
    private val condition: Condition? = null,
    private val orders: List<Order> = emptyList(),
    private val limitCount: Int? = null,
    private val relation: String? = null,
    private val params: JsonObject = JsonObject(emptyMap()),
) {
    private fun copy(condition: Condition? = this.condition, orders: List<Order> = this.orders, limitCount: Int? = this.limitCount) =
        Query(modelName, decoder, condition, orders, limitCount, relation, params)

    fun where(next: Condition) = copy(condition = condition?.and(next) ?: next)
    fun search(text: String, wholeWords: Boolean = false) = where(Condition.search(text, wholeWords))
    fun order(vararg next: Order) = copy(orders = orders + next)
    fun limit(count: Int) = copy(limitCount = count)
    fun window(tail: Int, max: Int? = null) = Window(this, tail, max)

    internal val json: String
        get() = json(false)

    private fun json(refreshOnConnect: Boolean) = buildJsonObject {
        put("model", modelName)
        if (refreshOnConnect) put("refreshOnConnect", true)
        condition?.let { put("where", it.json) }
        if (orders.isNotEmpty()) put("order", JsonArray(orders.map { buildJsonObject { put("field", it.field); put("direction", it.direction) } }))
        limitCount?.let { put("limit", it) }
        relation?.let { put("relation", it); put("params", params) }
    }.jsonText()

    val live: LiveSource<List<Model>>
        get() = listSource(emptyList(), decoder, open = { listener -> DBlayer.database.liveQuery(json, listener) }) { applied, _ -> applied.rows }

    val snapshots: LiveSource<Snapshot<Model>>
        get() = snapshots(false) {}

    internal fun snapshots(refreshOnConnect: Boolean, connected: (Result<LiveSubscription>) -> Unit): LiveSource<Snapshot<Model>> =
        listSource(Snapshot(emptyList(), emptyMap()), decoder, { older, newer -> Snapshot.merging(older, newer) }, { listener ->
            val result = runCatching { DBlayer.database.liveQuery(json(refreshOnConnect), listener) }
            connected(result)
            result.getOrThrow()
        }) { applied, change ->
            Snapshot.of(applied, change)
        }

    fun <Target : DBModel> related(association: Association<Model, Target>) = RelatedQuery(association.name, association.decoder) { json }

    val count: LiveSource<Long>
        get() = LiveSource(0L, { listener -> DBlayer.database.liveCount(json, listener) }) { change -> change.count ?: 0L }

    fun find(id: String): LiveSource<Model?> =
        listSource<Model, Model?>(null, decoder, open = { listener -> DBlayer.database.liveFind(modelName, id, listener) }) { applied, _ -> applied.rows.firstOrNull() }

    suspend fun cached(): Cached<List<Model>>? =
        offCaller { DBlayer.database.peekQuery(json)?.let { change -> Cached(change.rows.map(decoder::read), change.revision) } }

    suspend fun cachedRow(id: String): Model? =
        offCaller { DBlayer.database.peekRow(modelName, id)?.let(decoder::read) }

    suspend fun warm() = offCaller { DBlayer.database.warm(json) }

    suspend fun edgeValues(id: String): JsonObject? =
        offCaller { (Json.parseToJsonElement(DBlayer.database.edgeValues(modelName, relation.orEmpty(), params.jsonText(), listOf(id))) as JsonObject)[id] as? JsonObject }

    suspend fun load() = offCaller { DBlayer.database.load(modelName, relation.orEmpty(), params.jsonText()) }
    suspend fun refresh() = offCaller { DBlayer.database.refresh(modelName, relation.orEmpty(), params.jsonText()) }
    /** Reads every page of the connection of this relation to its confirmed end, so that `snapshots.current()` holds all rows afterwards. `force = false` uses the current freshness and the pages that are stored, `force = true` refreshes the first page and then reads the rest. A call that runs together with another one for the same relation shares its page requests. It throws when a page fails, when `max_pages` stops it before the end, or when the query has no remote relation. It does not make the pages atomic: rows of earlier pages stay after a failure. */
    suspend fun loadAll(force: Boolean = false) = offCaller { DBlayer.database.loadAll(modelName, relation.orEmpty(), params.jsonText(), force) }
    suspend fun loadMore(): LoadMoreResult = offCaller { DBlayer.database.loadMore(modelName, relation.orEmpty(), params.jsonText()) }
}

data class Cached<Value>(val value: Value, val revision: Long)

fun <Model : DBModel> idsQuery(modelName: String, decoder: ModelDecoder<Model>, ids: List<String>): Query<Model> =
    Query(modelName, decoder).where(Condition.inList("id", ids.map { JsonPrimitive(it) }))

class Association<Owner : DBModel, Target : DBModel>(val name: String, internal val decoder: ModelDecoder<Target>)

class RelatedQuery<Target : DBModel> internal constructor(
    private val name: String,
    private val decoder: ModelDecoder<Target>,
    private val sourceJson: () -> String,
) {
    internal fun sourceJsonText() = sourceJson()

    val live: LiveSource<List<Target>>
        get() = listSource(emptyList(), decoder, open = { listener -> DBlayer.database.liveRelated(sourceJson(), name, listener) }) { applied, _ -> applied.rows }

    val snapshots: LiveSource<Snapshot<Target>>
        get() = listSource(Snapshot(emptyList(), emptyMap()), decoder, { older, newer -> Snapshot.merging(older, newer) }, { listener ->
            DBlayer.database.liveRelated(sourceJson(), name, listener)
        }) { applied, change -> Snapshot.of(applied, change) }
}

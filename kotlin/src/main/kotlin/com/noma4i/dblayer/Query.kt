package com.noma4i.dblayer

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.longOrNull

class Condition(internal val tree: QueryCondition) {
    infix fun and(other: Condition) = Condition(QueryCondition.AllOf(listOf(tree, other.tree)))

    infix fun or(other: Condition) = Condition(QueryCondition.AnyOf(listOf(tree, other.tree)))

    operator fun not() = Condition(QueryCondition.Not(tree))

    companion object {
        fun equal(field: String, value: JsonElement) = Condition(QueryCondition.Equal(field, value.cell()))
        fun notEqual(field: String, value: JsonElement) = Condition(QueryCondition.NotEqual(field, value.cell()))
        fun compare(field: String, op: CompareOp, value: JsonElement) = Condition(QueryCondition.Compare(field, op, value.cell()))
        fun inList(field: String, values: List<JsonElement>) = Condition(QueryCondition.InList(field, values.map { it.cell() }))
        fun isNull(field: String) = Condition(QueryCondition.IsNull(field))
        fun search(text: String, wholeWords: Boolean = false) = Condition(QueryCondition.Search(text, wholeWords))
    }
}

typealias Order = QueryOrder

class Column<Model : DBModel, Value>(val name: String) {
    val asc get() = Order(name, SortDirection.ASC)
    val desc get() = Order(name, SortDirection.DESC)
    val ascNullsLast get() = Order(name, SortDirection.ASC_NULLS_LAST)
    val descNullsFirst get() = Order(name, SortDirection.DESC_NULLS_FIRST)
    val isNull get() = Condition.isNull(name)
}

inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.eq(value: Value) = Condition.equal(name, jsonValue(value))
inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.neq(value: Value) = Condition.notEqual(name, jsonValue(value))
inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.lt(value: Value) = Condition.compare(name, CompareOp.LESS, jsonValue(value))
inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.lte(value: Value) = Condition.compare(name, CompareOp.LESS_OR_EQUAL, jsonValue(value))
inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.gt(value: Value) = Condition.compare(name, CompareOp.GREATER, jsonValue(value))
inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.gte(value: Value) = Condition.compare(name, CompareOp.GREATER_OR_EQUAL, jsonValue(value))
inline infix fun <Model : DBModel, reified Value> Column<Model, Value>.within(values: List<Value>) = Condition.inList(name, values.map { jsonValue(it) })

internal fun JsonElement.cell(): Cell = when (this) {
    JsonNull -> Cell.Null
    is JsonArray -> Cell.Seq(map { it.cell() })
    is JsonObject -> Cell.Keyed(map { (key, value) -> CellEntry(key, value.cell()) })
    is JsonPrimitive -> when {
        isString -> Cell.Text(content)
        booleanOrNull != null -> Cell.Bool(content.toBoolean())
        longOrNull != null -> Cell.Int(content.toLong())
        else -> Cell.Real(content.toDouble())
    }
}

class Query<Model : DBModel>(
    private val modelName: String,
    internal val decoder: ModelDecoder<Model>,
    private val conditions: List<Condition> = emptyList(),
    private val orders: List<Order> = emptyList(),
    private val limitCount: Int? = null,
    private val relation: String? = null,
    private val params: JsonObject = JsonObject(emptyMap()),
    private val offsetCount: Int? = null,
) {
    private fun copy(conditions: List<Condition> = this.conditions, orders: List<Order> = this.orders, limitCount: Int? = this.limitCount, offsetCount: Int? = this.offsetCount) =
        Query(modelName, decoder, conditions, orders, limitCount, relation, params, offsetCount)

    fun where(next: Condition) = copy(conditions = conditions + next)
    fun search(text: String, wholeWords: Boolean = false) = where(Condition.search(text, wholeWords))
    fun order(vararg next: Order) = copy(orders = orders + next)
    fun limit(count: Int) = copy(limitCount = count)
    fun offset(count: Int) = copy(offsetCount = count)

    internal fun spec(refreshOnConnect: Boolean = false) =
        QuerySpec(modelName, relation, params.jsonText(), conditions.map { it.tree }, orders, window(limitCount, "limit"), window(offsetCount, "offset"), refreshOnConnect)

    private fun window(count: Int?, name: String): UInt? {
        if (count != null && count < 0) throw DbException.Invalid("$name $count is outside 0..${UInt.MAX_VALUE}")
        return count?.toUInt()
    }

    /** The rows of this query read from storage once. It sends no request and keeps no stream. */
    suspend fun read(account: AccountScope? = null): List<Model> = onAccountRead(account) { database, epoch -> database.readQuery(epoch, spec()).map(decoder::read) }

    /** One row read from storage, or null when the row does not exist. */
    suspend fun readRow(id: String, account: AccountScope? = null): Model? = onAccountRead(account) { database, epoch -> database.readRow(epoch, modelName, id)?.let(decoder::read) }

    suspend fun cachedRow(id: String, account: AccountScope? = null): Model? =
        onAccountRead(account) { database, epoch -> database.peekRow(epoch, modelName, id)?.let(decoder::read) }

    suspend fun edgeValues(id: String, account: AccountScope? = null): JsonObject? =
        onAccountRead(account) { database, epoch -> database.edgeValues(epoch, modelName, relation, params.jsonText(), listOf(id)).firstOrNull()?.let { Cell.Keyed(it.values).asJson() as JsonObject } }

    /** Starts loading the relation and returns this caller's interest at once. Callers of the same scope share one request, and the request stops when every caller cancelled its interest. */
    fun beginLoad(account: AccountScope? = null): LoadRequest = admit(account).let { admission -> admission.database.beginLoad(admission.epoch, modelName, relation, params.jsonText()) }

    /** Loads the relation. Cancelling the coroutine gives up its interest in the shared request. */
    suspend fun load(account: AccountScope? = null) {
        offCaller {
            val request = beginLoad(account)
            try {
                request.result()
            } finally {
                request.cancel()
            }
        }
    }
    suspend fun refresh(account: AccountScope? = null) = onAccountOf(account) { _, database, epoch -> database.refresh(epoch, modelName, relation, params.jsonText()) }
    /** Reads every page of the connection of this relation to its confirmed end, so that a stream opened afterwards holds all rows. `force = false` uses the current freshness and the pages that are stored, `force = true` refreshes the first page and then reads the rest. A call that runs together with another one for the same relation joins it. */
    suspend fun loadAll(force: Boolean = false, account: AccountScope? = null) {
        val admission = admit(account)
        offCaller {
            val request = admission.database.beginLoadAll(admission.epoch, modelName, relation, params.jsonText(), force)
            try {
                request.result()
            } finally {
                request.cancel()
            }
        }
    }
    suspend fun loadMore(account: AccountScope? = null): LoadMoreResult = onAccountOf(account) { _, database, epoch -> database.loadMore(epoch, modelName, relation, params.jsonText()) }
}

fun <Model : DBModel> idsQuery(modelName: String, decoder: ModelDecoder<Model>, ids: List<String>): Query<Model> =
    Query(modelName, decoder).where(Condition.inList("id", ids.map { JsonPrimitive(it) }))

class Association<Owner : DBModel, Target : DBModel>(val name: String, internal val decoder: ModelDecoder<Target>)

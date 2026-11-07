package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class QuerySpecTest {
    private val number = Column<Moment, Long>("n")

    @Test
    fun theQueryBuilderFillsTheRecordTheCoreCompiles() {
        val three = Cell.Int(3)
        val conditions = listOf(
            (number eq 3L) to QueryCondition.Equal("n", three),
            (number neq 3L) to QueryCondition.NotEqual("n", three),
            (number lt 3L) to QueryCondition.Compare("n", CompareOp.LESS, three),
            (number lte 3L) to QueryCondition.Compare("n", CompareOp.LESS_OR_EQUAL, three),
            (number gt 3L) to QueryCondition.Compare("n", CompareOp.GREATER, three),
            (number gte 3L) to QueryCondition.Compare("n", CompareOp.GREATER_OR_EQUAL, three),
            (number within listOf(1L, 2L)) to QueryCondition.InList("n", listOf(Cell.Int(1), Cell.Int(2))),
            number.isNull to QueryCondition.IsNull("n"),
            Condition.search("hi", true) to QueryCondition.Search("hi", true),
            (Condition.equal("t", JsonNull) or Condition.equal("t", JsonPrimitive("x"))) to QueryCondition.AnyOf(listOf(QueryCondition.Equal("t", Cell.Null), QueryCondition.Equal("t", Cell.Text("x")))),
            (!Condition.isNull("t")) to QueryCondition.Not(QueryCondition.IsNull("t")),
        )
        for ((condition, expected) in conditions) assertEquals(expected, condition.tree)

        val orders = listOf(number.asc to SortDirection.ASC, number.desc to SortDirection.DESC, number.ascNullsLast to SortDirection.ASC_NULLS_LAST, number.descNullsFirst to SortDirection.DESC_NULLS_FIRST)
        for ((order, direction) in orders) assertEquals(QueryOrder("n", direction), order)

        val spec = Query("Moment", Moment, relation = "feed", params = kotlinx.serialization.json.JsonObject(mapOf("k" to JsonPrimitive("v"))))
            .where(number gt 1L).where(Condition.isNull("t")).order(number.asc).limit(5).offset(2).spec(true)
        assertEquals(
            QuerySpec("Moment", "feed", """{"k":"v"}""", listOf(QueryCondition.Compare("n", CompareOp.GREATER, Cell.Int(1)), QueryCondition.IsNull("t")), listOf(number.asc), 5u, 2u, true),
            spec,
        )
    }

    @Test
    fun aStreamWithoutARelationRefusesToLoad() = bounded {
        reopenSigned(Config(null, null), modelsJSON)
        val stream = AccountScope.current().stream(Query("Moment", Moment))
        assertThrows(DbException.Invalid::class.java) { kotlinx.coroutines.runBlocking { stream.refresh() } }
        assertThrows(DbException.Invalid::class.java) { kotlinx.coroutines.runBlocking { stream.loadMore() } }
        stream.cancel()
    }
}

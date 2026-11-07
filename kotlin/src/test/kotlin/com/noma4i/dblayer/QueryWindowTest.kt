package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Test

class QueryWindowTest {
    @Test
    fun aQueryReadsLimitRowsAfterOffsetRowsInOrder() = bounded {
        reopenSigned(Config(null, null, 8u), modelsJSON)
        for (text in listOf("a", "b", "c", "d")) Moment.send(AccountScope.current(), text = text).optimisticValue!!
        val window = Query("Moment", Moment).order(Order("text", "asc")).limit(2).offset(1)
        assertEquals(listOf<String?>("b", "c"), window.live.current().map { it.text })
    }
}

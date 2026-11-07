package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Test

class ListEditTest {
    @Test
    fun aRowThatEntersTheMiddleOfTheListLandsAtItsIndex() {
        val rows = mutableListOf("n1", "n3")
        rows.replay(listOf(ListEdit.Insert(1, "n2")))
        assertEquals(listOf("n1", "n2", "n3"), rows)
        rows.replay(listOf(ListEdit.Insert(0, "n0")))
        assertEquals(listOf("n0", "n1", "n2", "n3"), rows)
    }
}

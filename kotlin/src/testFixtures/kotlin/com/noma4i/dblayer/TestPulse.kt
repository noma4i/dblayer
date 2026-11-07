package com.noma4i.dblayer

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update

/** The common source of change events of the tests: the doubles call `pulse()` at each change a test can observe, a waiter resumes at the next pulse. */
object TestPulse {
    private val generation = MutableStateFlow(0)

    init {
    }

    val current: Int get() = generation.value

    fun pulse() = generation.update { it + 1 }

    suspend fun next(after: Int) {
        generation.first { it != after }
    }
}

package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
const val HANG_GUARD_MILLIS = 60_000L
fun <T> bounded(timeoutMillis: Long = HANG_GUARD_MILLIS * 2, body: suspend CoroutineScope.() -> T): T = runBlocking {
    try {
        withTimeout(timeoutMillis) { body() }
    } catch (error: TimeoutCancellationException) {
        throw AssertionError("the test body did not finish within $timeoutMillis ms", error)
    }
}
suspend fun eventually(what: String, timeoutMillis: Long = HANG_GUARD_MILLIS, condition: suspend () -> Boolean) {
    val deadline = System.nanoTime() + timeoutMillis * 1_000_000
    while (!condition()) {
        if (System.nanoTime() > deadline) throw AssertionError("timed out after $timeoutMillis ms waiting for $what")
        delay(5)
    }
}
fun eventuallyBlocking(what: String, timeoutMillis: Long = HANG_GUARD_MILLIS, condition: () -> Boolean) = bounded(timeoutMillis + HANG_GUARD_MILLIS) { eventually(what, timeoutMillis) { condition() } }
suspend fun <T> awaitWithin(what: String, block: suspend () -> T): T = try {
    withTimeout(HANG_GUARD_MILLIS) { block() }
} catch (error: TimeoutCancellationException) {
    throw AssertionError("$what did not happen within $HANG_GUARD_MILLIS ms\n${threadStacks()}", error)
}
private fun threadStacks(): String = Thread.getAllStackTraces().entries
    .sortedBy { it.key.name }
    .joinToString("\n") { (thread, frames) -> "\"${thread.name}\" ${thread.state}\n" + frames.joinToString("\n") { "    at $it" } }

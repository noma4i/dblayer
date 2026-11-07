package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import kotlinx.serialization.decodeFromString
const val HANG_GUARD_MILLIS = 18_000L
const val TEST_DEADLINE_MILLIS = 20_000L
fun <T> bounded(timeoutMillis: Long = TEST_DEADLINE_MILLIS, body: suspend CoroutineScope.() -> T): T = runBlocking {
    try {
        withTimeout(timeoutMillis) { body() }
    } catch (error: TimeoutCancellationException) {
        throw AssertionError("the test body did not finish within $timeoutMillis ms\n${threadStacks()}", error)
    }
}
suspend fun eventually(what: String, timeoutMillis: Long = HANG_GUARD_MILLIS, detail: () -> String = { "" }, condition: suspend () -> Boolean) {
    val deadline = System.nanoTime() + timeoutMillis * 1_000_000
    while (!condition()) {
        if (System.nanoTime() > deadline) throw AssertionError("timed out after $timeoutMillis ms waiting for $what\n${detail()}")
        yield()
        Thread.onSpinWait()
    }
}
suspend fun fencedUntil(what: String, fence: suspend () -> Unit, ready: () -> Boolean) {
    val deadline = System.nanoTime() + 8_000_000_000L
    while (!ready()) {
        if (System.nanoTime() > deadline) throw AssertionError("timed out waiting for $what")
        fence()
        yield()
    }
}
suspend fun <State> quiesce(what: String, rounds: Int = 3, fence: suspend () -> Unit, state: suspend () -> State) {
    val deadline = System.nanoTime() + HANG_GUARD_MILLIS * 1_000_000
    var last = state()
    var stable = 0
    while (stable < rounds) {
        if (System.nanoTime() > deadline) throw AssertionError("$what: the state kept changing for $HANG_GUARD_MILLIS ms")
        fence()
        yield()
        val now = state()
        stable = if (now == last) stable + 1 else 0
        last = now
    }
}
fun eventuallyBlocking(what: String, timeoutMillis: Long = HANG_GUARD_MILLIS, condition: () -> Boolean) = bounded(timeoutMillis + 2_000) { eventually(what, timeoutMillis) { condition() } }
suspend fun <T> awaitWithin(what: String, detail: () -> String = { "" }, block: suspend () -> T): T = try {
    withTimeout(HANG_GUARD_MILLIS) { block() }
} catch (error: TimeoutCancellationException) {
    throw AssertionError("$what did not happen within $HANG_GUARD_MILLIS ms\n${detail()}\n${threadStacks()}", error)
}
private fun threadStacks(): String = Thread.getAllStackTraces().entries
    .sortedBy { it.key.name }
    .joinToString("\n") { (thread, frames) -> "\"${thread.name}\" ${thread.state}\n" + frames.joinToString("\n") { "    at $it" } }

suspend fun <Model : DBModel> MutationReceipt<Model>.confirmed(): Model =
    requireNotNull(outcome().confirmedValue) { "the operation was not confirmed" }

suspend inline fun <reified Answer> MutationReceipt<*>.answer(): Answer {
    val confirmed = outcome() as? OperationOutcome.Confirmed<*> ?: error("the operation was not confirmed")
    return dbJson.decodeFromString<Answer>(requireNotNull(confirmed.answerJson) { "the operation has no answer" })
}

suspend fun MutationReceipt<*>.refusal(): WireFailure = (outcome() as? OperationOutcome.Rejected)?.failure ?: error("the operation was not refused")

/** The network goes away while the held request is at the server: the load waits (Loading x Online(false) is Waiting) until the test sets the network online again. */
suspend fun networkDrops(during: Hold) {
    during.entered()
    DBlayer.database.setOnline(false)
}

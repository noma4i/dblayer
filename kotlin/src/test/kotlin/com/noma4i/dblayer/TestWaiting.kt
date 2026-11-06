package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
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
suspend fun eventually(what: String, timeoutMillis: Long = HANG_GUARD_MILLIS, condition: suspend () -> Boolean) {
    val deadline = System.nanoTime() + timeoutMillis * 1_000_000
    while (!condition()) {
        if (System.nanoTime() > deadline) throw AssertionError("timed out after $timeoutMillis ms waiting for $what")
        delay(5)
    }
}
fun eventuallyBlocking(what: String, timeoutMillis: Long = HANG_GUARD_MILLIS, condition: () -> Boolean) = bounded(timeoutMillis + 2_000) { eventually(what, timeoutMillis) { condition() } }
suspend fun <T> awaitWithin(what: String, block: suspend () -> T): T = try {
    withTimeout(HANG_GUARD_MILLIS) { block() }
} catch (error: TimeoutCancellationException) {
    throw AssertionError("$what did not happen within $HANG_GUARD_MILLIS ms\n${threadStacks()}", error)
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

/** The network goes away while the held request is at the server: the load ends with the network error at once. The test clock never moves by itself, so a network error the server returns would wait for a retry without end. */
suspend fun networkDrops(during: Hold) {
    during.entered()
    DBlayer.database.setOnline(false)
}

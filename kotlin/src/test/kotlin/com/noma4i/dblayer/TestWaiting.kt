package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
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
    while (true) {
        val seen = TestPulse.current
        val held = condition()
        val remainingMillis = (deadline - System.nanoTime()) / 1_000_000
        if (held) {
            if (remainingMillis <= 0) throw AssertionError("$what held only at the deadline: a state change without a pulse\n${detail()}")
            return
        }
        if (remainingMillis <= 0) throw AssertionError("timed out after $timeoutMillis ms waiting for $what\n${detail()}")
        untilChange(seen, remainingMillis)
    }
}
/** Returns at the next pulse of the doubles and of the screen models, or at the deadline. It never calls the core: a core that waits for the test must not hold the wait. */
suspend fun untilChange(seen: Int, remainingMillis: Long) {
    if (TestPulse.current != seen) return
    withTimeoutOrNull(remainingMillis) { TestPulse.next(seen) }
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

suspend inline fun <reified Answer> MutationReceipt<*>.answer(): Answer {
    return dbJson.decodeFromString<Answer>(requireNotNull(confirmed().answerJson) { "the operation has no answer" })
}

suspend fun MutationReceipt<*>.rejection(): DbException.Rejected = try {
    confirmed()
    error("the operation was not refused")
} catch (rejected: DbException.Rejected) {
    rejected
}

suspend fun MutationReceipt<*>.refusal(): WireFailure = rejection().failure

suspend fun MutationReceipt<*>.isRefused(): Boolean = try {
    confirmed()
    false
} catch (_: DbException.Rejected) {
    true
}

suspend fun MutationReceipt<*>.endedWithSession(): DbException.NoSession = try {
    confirmed()
    error("the operation was not ended by the session")
} catch (ended: DbException.NoSession) {
    ended
}

/** The network goes away while the held request is at the server: the load waits (Loading x Online(false) is Waiting) until the test sets the network online again. */
suspend fun networkDrops(during: Hold) {
    during.entered()
    DBlayer.database.setOnline(false)
}

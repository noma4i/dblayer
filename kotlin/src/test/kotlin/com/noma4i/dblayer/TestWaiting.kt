package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

/** The one bound of every wait: it only stops a hang, no wait is expected to come near it. */
const val HANG_GUARD_MILLIS = 60_000L

/** The only blocking entry of a test: the whole body is bounded, a hang fails the test instead of the build. */
fun <T> bounded(timeoutMillis: Long = HANG_GUARD_MILLIS * 2, body: suspend CoroutineScope.() -> T): T = runBlocking {
    try {
        withTimeout(timeoutMillis) { body() }
    } catch (error: TimeoutCancellationException) {
        throw AssertionError("the test body did not finish within $timeoutMillis ms", error)
    }
}

/** The only way a test waits for a condition: bounded, and the failure names the condition. */
suspend fun eventually(what: String, timeoutMillis: Long = HANG_GUARD_MILLIS, condition: suspend () -> Boolean) {
    val deadline = System.nanoTime() + timeoutMillis * 1_000_000
    while (!condition()) {
        if (System.nanoTime() > deadline) throw AssertionError("timed out after $timeoutMillis ms waiting for $what")
        delay(5)
    }
}

/** The same wait for a test that is not a coroutine. */
fun eventuallyBlocking(what: String, timeoutMillis: Long = HANG_GUARD_MILLIS, condition: () -> Boolean) = bounded(timeoutMillis + HANG_GUARD_MILLIS) { eventually(what, timeoutMillis) { condition() } }

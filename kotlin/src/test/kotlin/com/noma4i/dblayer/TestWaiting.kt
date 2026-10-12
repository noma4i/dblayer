package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

/** The only blocking entry of a test: the whole body is bounded, a hang fails the test instead of the build. */
fun <T> bounded(timeoutMillis: Long = 60_000, body: suspend CoroutineScope.() -> T): T = runBlocking {
    try {
        withTimeout(timeoutMillis) { body() }
    } catch (error: TimeoutCancellationException) {
        throw AssertionError("the test body did not finish within $timeoutMillis ms", error)
    }
}

/** The only way a test waits for a condition: bounded, and the failure names the condition. */
suspend fun eventually(what: String, timeoutMillis: Long = 20_000, condition: suspend () -> Boolean) {
    val deadline = System.nanoTime() + timeoutMillis * 1_000_000
    while (!condition()) {
        if (System.nanoTime() > deadline) throw AssertionError("timed out after $timeoutMillis ms waiting for $what")
        delay(5)
    }
}

/** The same wait for a test that is not a coroutine. */
fun eventuallyBlocking(what: String, timeoutMillis: Long = 20_000, condition: () -> Boolean) = bounded(timeoutMillis + 5_000) { eventually(what, timeoutMillis) { condition() } }

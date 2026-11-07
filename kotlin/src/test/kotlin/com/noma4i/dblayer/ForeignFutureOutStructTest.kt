package com.noma4i.dblayer

import com.sun.jna.CallbackReference
import com.sun.jna.Function
import com.sun.jna.Memory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ForeignFutureOutStructTest {
    private val rounds = 400
    private val threads = 8
    private val sentinel: Byte = 0x5A

    private class Instant : ScriptedTransport {
        override suspend fun send(request: HttpRequest): RawOutcome = RawOutcome.Response(200u, null, "{}")
    }

    @Test
    fun theOutStructIsNotWrittenAfterTheFutureCompleted() {
        uniffiEnsureInitialized()
        val function = Function.getFunction(CallbackReference.getFunctionPointer(uniffiCallbackInterfaceTransport.send))
        val handle = FfiConverterTypeTransport.handleMap.insert(Instant())
        val clobbered = AtomicInteger()
        val unanswered = AtomicInteger()
        val done = CountDownLatch(threads)
        repeat(threads) {
            Thread {
                for (round in 0 until rounds) {
                    val owned = Memory(16).also { memory -> memory.setMemory(0, 16, 0) }
                    val completed = CountDownLatch(1)
                    val complete = object : UniffiForeignFutureCompleteRustBuffer {
                        override fun callback(callbackData: Long, result: UniffiForeignFutureResultRustBuffer.UniffiByValue) {
                            FfiConverterTypeRawOutcome.lift(result.returnValue)
                            owned.setMemory(0, 16, sentinel)
                            completed.countDown()
                        }
                    }
                    function.invoke(Void.TYPE, arrayOf(handle, FfiConverterTypeHttpRequest.lower(HttpRequest(Author.SESSION, 0uL, 1u, "https://graphql.test/", "POST", emptyMap(), "", 1uL, 1uL, NetworkPurpose.STANDARD)), complete, 0L, owned))
                    if (!completed.await(10, TimeUnit.SECONDS)) {
                        unanswered.incrementAndGet()
                        break
                    }
                    if (owned.getByteArray(0, 16).any { byte -> byte != sentinel }) clobbered.incrementAndGet()
                }
                done.countDown()
            }.start()
        }
        val finished = done.await(60, TimeUnit.SECONDS)
        FfiConverterTypeTransport.handleMap.remove(handle)
        assertTrue("the workers did not finish", finished)
        assertEquals(0, unanswered.get())
        assertEquals(0, clobbered.get())
    }
}

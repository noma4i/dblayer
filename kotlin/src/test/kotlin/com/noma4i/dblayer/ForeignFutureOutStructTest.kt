package com.noma4i.dblayer

import com.sun.jna.CallbackReference
import com.sun.jna.Function
import com.sun.jna.Memory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.junit.Assert.assertEquals
import org.junit.Test

class ForeignFutureOutStructTest {
    private val rounds = 400
    private val threads = 8
    private val sentinel: Byte = 0x5A

    private class Instant : Transport {
        override suspend fun openCable(lease: CableLease) {}
        override suspend fun cableSend(token: String, text: String) {}
        override suspend fun cableNext(token: String): EncryptedSocketEvent? = null
        override suspend fun cableClose(token: String) {}
        override suspend fun query(request: TransportRequest): String = "{}"
        override suspend fun mutate(request: TransportRequest): String = "{}"
        override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink) {}
        override fun unsubscribe(token: String) {}
        override suspend fun put(url: String, headers: Map<String, String>, file: PutFile, tag: String, progress: ByteProgress): PutReply = PutReply(200u, null)
    }

    @Test
    fun theOutStructIsNotWrittenAfterTheFutureCompleted() {
        uniffiEnsureInitialized()
        val function = Function.getFunction(CallbackReference.getFunctionPointer(uniffiCallbackInterfaceTransport.query))
        val handle = FfiConverterTypeTransport.handleMap.insert(Instant())
        val clobbered = AtomicInteger()
        val done = CountDownLatch(threads)
        repeat(threads) {
            Thread {
                repeat(rounds) {
                    val owned = Memory(16).also { memory -> memory.setMemory(0, 16, 0) }
                    val completed = CountDownLatch(1)
                    val complete = object : UniffiForeignFutureCompleteRustBuffer {
                        override fun callback(callbackData: Long, result: UniffiForeignFutureResultRustBuffer.UniffiByValue) {
                            FfiConverterString.lift(result.returnValue)
                            owned.setMemory(0, 16, sentinel)
                            completed.countDown()
                        }
                    }
                    function.invoke(Void.TYPE, arrayOf(handle, FfiConverterTypeTransportRequest.lower(TransportRequest("", "", "", false)), complete, 0L, owned))
                    completed.await(10, TimeUnit.SECONDS)
                    if (owned.getByteArray(0, 16).any { byte -> byte != sentinel }) clobbered.incrementAndGet()
                }
                done.countDown()
            }.start()
        }
        done.await(60, TimeUnit.SECONDS)
        assertEquals(0, clobbered.get())
    }
}

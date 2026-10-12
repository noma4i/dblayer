package com.noma4i.dblayer

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

class LiveSourceTest {
    @Test
    fun aSubscribeThatThrowsEndsTheFlowAndCurrentReturnsTheInitialValue() = runBlocking {
        val source = LiveSource<Int>(7) { _ -> throw IllegalStateException("refused") }

        assertEquals(7, withTimeout(3000) { source.current() })
    }

    @Test
    fun theSubscriptionRunsOffTheThreadThatCollects() = runBlocking {
        val collector = Thread.currentThread()
        var subscriber: Thread? = null
        val source = LiveSource<Int>(0) { emit ->
            subscriber = Thread.currentThread()
            emit(1)
            ({})
        }

        assertEquals(1, withTimeout(3000) { source.current() })
        assertEquals(false, subscriber === collector)
    }
}

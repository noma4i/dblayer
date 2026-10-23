package com.noma4i.dblayer

import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

class LiveSourceTest {
    @Test
    fun aSubscribeThatThrowsSurfacesItsErrorFromCurrent() = bounded {
        val source = LiveSource<Int>(7) { _ -> throw IllegalStateException("refused") }

        val error = runCatching { withTimeout(3000) { source.current() } }.exceptionOrNull()

        assertEquals("refused", error?.message)
    }

    @Test
    fun theSubscriptionRunsOffTheThreadThatCollects() = bounded {
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

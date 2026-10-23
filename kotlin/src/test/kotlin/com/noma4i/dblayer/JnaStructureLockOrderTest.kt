package com.noma4i.dblayer

import com.sun.jna.Structure
import java.lang.management.ManagementFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.junit.Assert.assertNull
import org.junit.Test

class JnaStructureLockOrderTest {
    companion object {
        val guardNanos = TimeUnit.SECONDS.toNanos(10)
        val secondReady = CountDownLatch(1)
        val firstHoldsValidation = CountDownLatch(1)
        @Volatile var first: Thread? = null
        @Volatile var second: Thread? = null
        @Volatile var firstHooked = false
        @Volatile var secondHooked = false

        fun secondIsBlockedInTypeInfo(): Boolean {
            val thread = second ?: return false
            val state = thread.state
            if (state != Thread.State.WAITING && state != Thread.State.BLOCKED) return false
            return thread.stackTrace.any { frame -> frame.className.contains("FFIType") }
        }
    }

    class Warm : Structure(), Structure.ByValue {
        @JvmField var value: Long = 0
        override fun getFieldOrder(): List<String> = listOf("value")
    }

    open class FirstLeaf : Structure() {
        @JvmField var value: Long = 0
        override fun getFieldOrder(): List<String> {
            if (Thread.currentThread() === first && !firstHooked) {
                firstHooked = true
                firstHoldsValidation.countDown()
                val end = System.nanoTime() + guardNanos
                while (System.nanoTime() < end && !secondIsBlockedInTypeInfo()) Thread.onSpinWait()
            }
            return listOf("value")
        }
    }

    class FirstLeafByValue : FirstLeaf(), Structure.ByValue

    class FirstOuter : Structure() {
        @JvmField var leaf: FirstLeafByValue = FirstLeafByValue()
        override fun getFieldOrder(): List<String> = listOf("leaf")
    }

    class SecondByValue : Structure(), Structure.ByValue {
        @JvmField var value: Long = 0
        override fun getFieldOrder(): List<String> {
            if (Thread.currentThread() === second && !secondHooked) {
                secondHooked = true
                secondReady.countDown()
                firstHoldsValidation.await(10, TimeUnit.SECONDS)
            }
            return listOf("value")
        }
    }

    @Test
    fun firstStructuresOfTwoBindingsDoNotDeadlock() {
        Warm()
        second = Thread { SecondByValue() }.apply { isDaemon = true }
        first = Thread {
            secondReady.await()
            FirstOuter()
        }.apply { isDaemon = true }
        second!!.start()
        first!!.start()
        first!!.join(TimeUnit.SECONDS.toMillis(15))
        second!!.join(TimeUnit.SECONDS.toMillis(15))
        val deadlocked = ManagementFactory.getThreadMXBean().findDeadlockedThreads()
        assertNull("deadlocked threads: ${deadlocked?.toList()}", deadlocked)
    }
}

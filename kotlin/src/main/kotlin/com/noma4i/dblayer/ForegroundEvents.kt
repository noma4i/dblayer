package com.noma4i.dblayer

import android.os.Handler
import android.os.Looper
import androidx.lifecycle.DefaultLifecycleObserver
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.ProcessLifecycleOwner

internal class ForegroundEvents(private val lifecycle: Lifecycle, private val deliver: () -> Unit) {
    @Volatile private var active = true
    private var stopped = false

    private val observer = object : DefaultLifecycleObserver {
        override fun onStop(owner: LifecycleOwner) {
            stopped = true
        }

        override fun onStart(owner: LifecycleOwner) {
            if (!active || !stopped) return
            stopped = false
            deliver()
        }
    }

    fun start() {
        if (!active) return
        stopped = !lifecycle.currentState.isAtLeast(Lifecycle.State.STARTED)
        lifecycle.addObserver(observer)
    }

    fun deactivate() {
        active = false
    }

    fun detach() = lifecycle.removeObserver(observer)

    companion object {
        private var current: ForegroundEvents? = null

        @Synchronized
        fun start(sink: OnlineSink) {
            retire()
            val made = ForegroundEvents(ProcessLifecycleOwner.get().lifecycle) { sink.enterForeground() }
            current = made
            onMain { made.start() }
        }

        @Synchronized
        fun stop() = retire()

        private fun retire() {
            val old = current ?: return
            current = null
            old.deactivate()
            onMain { old.detach() }
        }

        private fun onMain(work: () -> Unit) {
            Handler(Looper.getMainLooper()).post(work)
        }
    }
}

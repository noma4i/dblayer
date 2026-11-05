package com.noma4i.dblayer

import android.os.Handler
import android.os.Looper
import androidx.lifecycle.DefaultLifecycleObserver
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.ProcessLifecycleOwner

internal class ForegroundEvents(private val lifecycle: Lifecycle, private val deliver: () -> Unit) {
    @Volatile private var active = true

    private val observer = object : DefaultLifecycleObserver {
        override fun onStart(owner: LifecycleOwner) {
            if (active) deliver()
        }
    }

    fun start() {
        if (!active) return
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

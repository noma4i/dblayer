package com.noma4i.dblayer

import android.os.Handler
import android.os.Looper
import androidx.lifecycle.DefaultLifecycleObserver
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.ProcessLifecycleOwner
import java.util.concurrent.atomic.AtomicBoolean

internal object ForegroundEvents {
    /** Calls [deliver] on every start and [background] on every stop of the process lifecycle, and [background] once at registration when the lifecycle is not started. After the returned stop no new [deliver] starts and a queued registration is void; the stop needs no main thread. */
    fun observe(lifecycle: Lifecycle = ProcessLifecycleOwner.get().lifecycle, post: (() -> Unit) -> Unit = ::onMain, background: () -> Unit = {}, deliver: () -> Unit): () -> Unit {
        val live = AtomicBoolean(true)
        val observer = object : DefaultLifecycleObserver {
            override fun onStart(owner: LifecycleOwner) {
                if (live.get()) deliver()
            }

            override fun onStop(owner: LifecycleOwner) {
                if (live.get()) background()
            }
        }
        post {
            if (!live.get()) return@post
            lifecycle.addObserver(observer)
            if (!lifecycle.currentState.isAtLeast(Lifecycle.State.STARTED)) background()
        }
        return {
            live.set(false)
            post { lifecycle.removeObserver(observer) }
        }
    }

    private fun onMain(work: () -> Unit) {
        Handler(Looper.getMainLooper()).post(work)
    }
}

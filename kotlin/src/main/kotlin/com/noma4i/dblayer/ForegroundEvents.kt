package com.noma4i.dblayer

import android.os.Handler
import android.os.Looper
import androidx.lifecycle.DefaultLifecycleObserver
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.ProcessLifecycleOwner
import java.util.concurrent.atomic.AtomicBoolean

internal object ForegroundEvents {
    /** Calls [deliver] on every start of the process lifecycle. After the returned stop no new [deliver] starts and a queued registration is void; the stop needs no main thread. */
    fun observe(lifecycle: Lifecycle = ProcessLifecycleOwner.get().lifecycle, post: (() -> Unit) -> Unit = ::onMain, deliver: () -> Unit): () -> Unit {
        val live = AtomicBoolean(true)
        val observer = object : DefaultLifecycleObserver {
            override fun onStart(owner: LifecycleOwner) {
                if (live.get()) deliver()
            }
        }
        post { if (live.get()) lifecycle.addObserver(observer) }
        return {
            live.set(false)
            post { lifecycle.removeObserver(observer) }
        }
    }

    private fun onMain(work: () -> Unit) {
        Handler(Looper.getMainLooper()).post(work)
    }
}

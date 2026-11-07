package com.noma4i.dblayer

import android.os.Handler
import android.os.Looper
import androidx.lifecycle.DefaultLifecycleObserver
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.ProcessLifecycleOwner

internal object ForegroundEvents {
    /** Calls [deliver] on every start of the process lifecycle until the returned stop runs. */
    fun observe(lifecycle: Lifecycle = ProcessLifecycleOwner.get().lifecycle, post: (() -> Unit) -> Unit = ::onMain, deliver: () -> Unit): () -> Unit {
        val observer = object : DefaultLifecycleObserver {
            override fun onStart(owner: LifecycleOwner) = deliver()
        }
        post { lifecycle.addObserver(observer) }
        return { post { lifecycle.removeObserver(observer) } }
    }

    private fun onMain(work: () -> Unit) {
        Handler(Looper.getMainLooper()).post(work)
    }
}

package com.noma4i.dblayer

internal class OnlineRelay(private val connectivity: (Boolean) -> Unit) {
    private class Work(val online: Boolean, val connectivity: Boolean, val sink: ((Boolean) -> Unit)?)

    private val lock = Any()
    private var online = false
    private var sink: ((Boolean) -> Unit)? = null
    private var connectivityDue = false
    private var sinkDue = false
    private var delivering = false

    fun attach(sink: (Boolean) -> Unit) {
        synchronized(lock) { this.sink = sink; sinkDue = true }
        deliver()
    }

    fun detach() {
        synchronized(lock) { sink = null; sinkDue = false }
    }

    fun set(online: Boolean) {
        synchronized(lock) { this.online = online; connectivityDue = true; sinkDue = sink != null }
        deliver()
    }

    private fun deliver() {
        synchronized(lock) {
            if (delivering) return
            delivering = true
        }
        try {
            while (true) {
                val work = synchronized(lock) {
                    if (!connectivityDue && !sinkDue) { delivering = false; null }
                    else Work(online, connectivityDue, if (sinkDue) sink else null).also { connectivityDue = false; sinkDue = false }
                } ?: return
                if (work.connectivity) connectivity(work.online)
                work.sink?.invoke(work.online)
            }
        } catch (error: Throwable) {
            synchronized(lock) { delivering = false }
            throw error
        }
    }
}

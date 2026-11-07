package com.noma4i.dblayer

import java.util.concurrent.Executors

internal object RuntimeLane {
    private val executor = Executors.newSingleThreadExecutor { task -> Thread(task, "dblayer-runtime-lane").apply { isDaemon = true } }

    fun enqueue(work: () -> Unit) {
        executor.execute(work)
    }
}

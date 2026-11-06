package com.noma4i.dblayer

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

internal fun finishesElsewhere(probe: () -> Unit): Boolean {
    val done = CountDownLatch(1)
    Thread { probe(); done.countDown() }.apply { isDaemon = true }.start()
    return done.await(10, TimeUnit.SECONDS)
}

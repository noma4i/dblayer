package com.noma4i.dblayer

import android.app.ActivityManager
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.PowerManager
import java.util.concurrent.atomic.AtomicReference

internal object PowerSaving {
    private class Registration(val receiver: BroadcastReceiver, val context: Context)

    private val registration = AtomicReference<Registration?>(null)

    fun receiver(read: () -> Boolean, report: (Boolean) -> Unit): BroadcastReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context, intent: Intent) = report(read())
    }

    fun saving(context: Context): Boolean =
        (context.applicationContext.getSystemService(Context.POWER_SERVICE) as PowerManager).isPowerSaveMode

    fun lowRam(context: Context): Boolean =
        (context.applicationContext.getSystemService(Context.ACTIVITY_SERVICE) as ActivityManager).isLowRamDevice

    fun start(context: Context, sink: OnlineSinkInterface) {
        val app = context.applicationContext
        val made = receiver({ saving(app) }, sink::setPowerSaving)
        app.registerReceiver(made, IntentFilter(PowerManager.ACTION_POWER_SAVE_MODE_CHANGED))
        unregister(registration.getAndSet(Registration(made, app)))
        sink.setPowerSaving(saving(app))
    }

    fun stop() {
        unregister(registration.getAndSet(null))
    }

    private fun unregister(old: Registration?) {
        old?.context?.unregisterReceiver(old.receiver)
    }
}

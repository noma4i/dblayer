package com.noma4i.dblayer

import android.app.ActivityManager
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.PowerManager

internal object PowerSaving {
    private var receiver: BroadcastReceiver? = null
    private var registeredOn: Context? = null

    fun receiver(read: () -> Boolean, report: (Boolean) -> Unit): BroadcastReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context, intent: Intent) = report(read())
    }

    fun saving(context: Context): Boolean =
        (context.applicationContext.getSystemService(Context.POWER_SERVICE) as PowerManager).isPowerSaveMode

    fun lowRam(context: Context): Boolean =
        (context.applicationContext.getSystemService(Context.ACTIVITY_SERVICE) as ActivityManager).isLowRamDevice

    @Synchronized
    fun start(context: Context, sink: OnlineSink) {
        val app = context.applicationContext
        stop()
        val made = receiver({ saving(app) }, sink::setPowerSaving)
        app.registerReceiver(made, IntentFilter(PowerManager.ACTION_POWER_SAVE_MODE_CHANGED))
        receiver = made
        registeredOn = app
        sink.setPowerSaving(saving(app))
    }

    @Synchronized
    fun stop() {
        val old = receiver ?: return
        registeredOn?.unregisterReceiver(old)
        receiver = null
        registeredOn = null
    }
}

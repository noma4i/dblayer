package com.noma4i.dblayer

import android.app.ActivityManager
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.PowerManager

internal object PowerSaving {
    fun receiver(read: () -> Boolean, report: (Boolean) -> Unit): BroadcastReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context, intent: Intent) = report(read())
    }

    fun saving(context: Context): Boolean =
        (context.applicationContext.getSystemService(Context.POWER_SERVICE) as PowerManager).isPowerSaveMode

    fun lowRam(context: Context): Boolean =
        (context.applicationContext.getSystemService(Context.ACTIVITY_SERVICE) as ActivityManager).isLowRamDevice

    fun start(context: Context, sink: OnlineSinkInterface): Registration {
        val app = context.applicationContext
        val made = receiver({ saving(app) }, sink::setPowerSaving)
        app.registerReceiver(made, IntentFilter(PowerManager.ACTION_POWER_SAVE_MODE_CHANGED))
        sink.setPowerSaving(saving(app))
        return Registration(made, app)
    }

    class Registration(private val receiver: BroadcastReceiver, private val context: Context) {
        fun stop() = context.unregisterReceiver(receiver)
    }
}

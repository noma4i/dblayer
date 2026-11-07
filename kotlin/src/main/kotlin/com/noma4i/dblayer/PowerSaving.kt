package com.noma4i.dblayer

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.PowerManager

internal object PowerSaving {
    fun receiver(read: () -> Boolean, report: (Boolean) -> Unit): BroadcastReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context, intent: Intent) = report(read())
    }

    /** Reports the OS value at once and on every change until the returned stop runs. */
    fun observe(context: Context, report: (Boolean) -> Unit): () -> Unit {
        val app = context.applicationContext
        val made = receiver({ saving(app) }, report)
        app.registerReceiver(made, IntentFilter(PowerManager.ACTION_POWER_SAVE_MODE_CHANGED))
        report(saving(app))
        return { app.unregisterReceiver(made) }
    }

    private fun saving(context: Context): Boolean =
        (context.getSystemService(Context.POWER_SERVICE) as PowerManager).isPowerSaveMode
}

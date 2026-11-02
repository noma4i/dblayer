package com.noma4i.dblayer

import android.content.Intent
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner

@RunWith(RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class PowerSavingTest {
    @Test
    fun theOsValueReachesTheCoreOnEveryBroadcast() {
        val context = org.robolectric.RuntimeEnvironment.getApplication()
        var saving = true
        val reports = mutableListOf<Boolean>()
        val receiver = PowerSaving.receiver({ saving }) { reports.add(it) }
        receiver.onReceive(context, Intent())
        saving = false
        receiver.onReceive(context, Intent())
        assertEquals(listOf(true, false), reports)
    }

    @Test
    fun theCoreIsToldTheStateWithNoPowerSavingLockHeld() {
        val context = org.robolectric.RuntimeEnvironment.getApplication()
        val probes = java.util.concurrent.CopyOnWriteArrayList<Boolean>()
        val sink = object : OnlineSinkInterface {
            override fun enterForeground() {}
            override fun setOnline(online: Boolean) {}
            override fun setPowerSaving(saving: Boolean) { probes += finishesElsewhere { PowerSaving.stop() } }
        }
        PowerSaving.start(context, sink)
        PowerSaving.stop()

        assertEquals(listOf(true), probes.toList())
    }
}

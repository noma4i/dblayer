package com.noma4i.dblayer

import android.os.Looper
import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.*
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class EntityMainThreadTest {
    @Test
    fun entityMainNativeCalls() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val row = Moment.create(text = "warm")
        var owner: Entity<Moment>? = Moment.entity(row.id)
        owner!!.waitForInitialLoad()
        val weak = java.lang.ref.WeakReference(owner)
        owner = null
        eventually("the warm entity releases") { System.gc(); weak.get() == null }
        assertSame(Looper.getMainLooper().thread, Thread.currentThread())
        val probe = EntityCallProbe()
        val warm = Moment.entity(row.id)
        val cold = Moment.entity("absent")
        assertSame(warm, Moment.entity(row.id))
        assertEquals(0uL, probe.calls())
        warm.waitForInitialLoad()
        cold.waitForInitialLoad()
        assertEquals("warm", warm.value.value?.text)
        assertEquals(EntityPhase.Absent, cold.phase.value)
        assertEquals(0uL, probe.calls())
    }
}

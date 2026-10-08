package dev.dblayer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class BridgeProbeTest {
  @Test
  fun versionComesFromRust() {
    assertEquals("0.1.0", version())
  }

  @Test
  fun storedPayloadReadsBack() {
    val store = Store.openInMemory()
    store.put("moments", "m1", "{\"a\":1}")

    assertEquals("{\"a\":1}", store.get("moments", "m1"))
    assertNull(store.get("moments", "m2"))
  }
}

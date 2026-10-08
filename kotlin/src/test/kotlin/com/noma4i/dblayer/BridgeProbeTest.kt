package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Test

class BridgeProbeTest {
  @Test
  fun versionComesFromRust() {
    assertEquals("0.1.0", version())
  }
}

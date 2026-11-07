package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

class UploadPermissionTest {
    @Test
    fun theManifestIsReadOnlyWhereTheForegroundServiceRuns() {
        assertEquals(false, dataSyncDeclared(33) { listOf("android.permission.INTERNET") })
        assertEquals(true, dataSyncDeclared(33) { listOf(DATA_SYNC_PERMISSION) })
        assertNull(dataSyncDeclared(34) { emptyList() })
    }

    @Test
    fun xfer_android_permission_checked_at_open() = bounded {
        try {
            OpenDatabase().open(Config(null, null), EMPTY_SPECS, OpenOptions(emptyList()), OpenPorts(null, null, null, null, null, null, false), null)
            fail("open has to refuse")
        } catch (error: DbException.Config) {
            assertEquals(ConfigCode.INVALID_REQUEST, error.code)
            assertTrue(error.detail, DATA_SYNC_PERMISSION in error.detail)
        }
    }
}

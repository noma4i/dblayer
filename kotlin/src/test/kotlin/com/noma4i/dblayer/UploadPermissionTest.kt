package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

class UploadPermissionTest {
    @Test
    fun api33WithoutTheDataSyncPermissionFailsOpenNamingIt() {
        try {
            checkUploadPermission(33) { listOf("android.permission.INTERNET") }
            fail("open has to refuse")
        } catch (error: DbException.Config) {
            assertEquals(ConfigCode.INVALID_REQUEST, error.code)
            assertTrue(error.detail, DATA_SYNC_PERMISSION in error.detail)
        }
    }

    @Test
    fun api33WithThePermissionAndApi34WithoutItOpen() {
        checkUploadPermission(33) { listOf(DATA_SYNC_PERMISSION) }
        checkUploadPermission(34) { emptyList() }
    }
}

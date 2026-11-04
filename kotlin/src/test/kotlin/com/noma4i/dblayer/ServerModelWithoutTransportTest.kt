package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(DBlayerTestingApi::class)
class ServerModelWithoutTransportTest {
    /** A database opened without a transport keeps rows of local models only. A server model gets its rows from the replies of a transport (`ClubTest` queues the `CreateClub` reply). */
    @Test
    fun aCreateOfAServerModelOnADatabaseWithoutATransportIsRefusedAsInvalid() = bounded {
        try {
            DBlayer.close()
            DBlayer.openTesting(null, modelsJSON)
            val refused = runCatching {
                DBlayer.database.create(DBlayer.database.sessionEpoch(), "Club", """{"name":"friends"}""", "{}")
            }.exceptionOrNull()
            assertTrue("the create was not refused: $refused", refused is DbException.Invalid)
        } finally {
            DBlayer.close()
        }
    }
}

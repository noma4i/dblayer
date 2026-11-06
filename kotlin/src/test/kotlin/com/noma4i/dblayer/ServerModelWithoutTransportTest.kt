package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(DBlayerTestingApi::class)
class ServerModelWithoutTransportTest {
    /** A database opened without a transport keeps rows of local models only. A server model gets its rows from the replies of a transport (`ClubTest` queues the `CreateClub` reply). */
    @Test
    fun aCreateOfAServerModelOnADatabaseWithoutATransportIsRefusedAsNoSession() = bounded {
        try {
            closeForCleanup()
            DBlayer.openTesting(null, modelsJSON)
            val refused = runCatching {
                DBlayer.database.create(DBlayer.database.sessionEpoch(), "Club", """{"name":"friends"}""", "{}")
            }.exceptionOrNull()
            assertTrue("the create was not refused: $refused", refused is DbException.NoSession)
            val load = runCatching {
                DBlayer.database.beginLoad(DBlayer.database.sessionEpoch(), "Club", "all", "{}").result()
            }.exceptionOrNull()
            assertTrue("the load was not refused: $load", load is DbException.NoSession)
            val act = runCatching {
                DBlayer.database.act(DBlayer.database.sessionEpoch(), "Club", "g1", "leave", "{}", "{}")
            }.exceptionOrNull()
            assertTrue("the act was not refused: $act", act is DbException.NoSession)
        } finally {
            closeForCleanup()
        }
    }
}

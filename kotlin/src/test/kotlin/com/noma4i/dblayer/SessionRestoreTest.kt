package com.noma4i.dblayer

import com.noma4i.dblayer.models.CurrentRegion
import com.noma4i.dblayer.models.CurrentUser
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Test

@OptIn(DBlayerTestingApi::class)
class SessionRestoreTest {
    @Test
    fun theLoadThatRestoresAResetSessionReturnsItsOwnResult() = bounded {
        try {
            DBlayer.close()
            val transport = MockTransport()
            for ((user, region) in listOf("u1" to "r1", "u2" to "r2")) {
                transport.enqueue("CurrentUserQuery", """{"data":{"user":{"id":"$user","name":"$user"}}}""")
                transport.enqueue("CurrentRegionQuery", """{"data":{"region":{"id":"$region","name":"$region"}}}""")
            }
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"},{"name":"CurrentRegionQuery","document":"query CurrentRegionQuery { region { id name } }"}]""")
            CurrentUser.current.load(AccountScope.current())
            CurrentRegion.current.load(AccountScope.current())
            assertEquals(listOf("u1"), CurrentUser.current.read(AccountScope.current()).map { it.id })

            DBlayer.sessionReset()
            CurrentUser.current.load(AccountScope.current())
            CurrentRegion.current.load(AccountScope.current())

            assertEquals(4, transport.calls.size)
            assertEquals(listOf("u2"), CurrentUser.current.read(AccountScope.current()).map { it.id })
            assertEquals(listOf("r2"), CurrentRegion.current.read(AccountScope.current()).map { it.id })
        } finally {
            DBlayer.close()
        }
    }
}

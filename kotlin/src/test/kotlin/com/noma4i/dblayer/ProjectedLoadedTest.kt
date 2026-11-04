package com.noma4i.dblayer

import com.noma4i.dblayer.models.CurrentUser
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import org.junit.Assert.assertEquals
import org.junit.Test

@OptIn(DBlayerTestingApi::class)
class ProjectedLoadedTest {
    /** The order of the app: a plain transport, no sign-in call, a held stream, `loaded()`, then the value at once. */
    @Test
    fun theValueOfAHeldStreamIsThereWhenLoadedReturnsAndARefreshStillAsksTheServer() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            DBlayer.close()
            val transport = MockTransport()
            repeat(2) { transport.enqueue("CurrentUserQuery", """{"data":{"user":{"id":"u1","name":"u1"}}}""") }
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]""")
            val held = Projected(AccountScope.current().stream(CurrentUser.current), CurrentUser, scope)

            held.loaded()
            assertEquals("u1", held.value?.id)
            held.loaded()
            assertEquals("u1", held.value?.id)
            assertEquals(1, transport.calls.count { it.operationName == "CurrentUserQuery" })
            CurrentUser.current.refresh()
            assertEquals(2, transport.calls.count { it.operationName == "CurrentUserQuery" })
            held.close()
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }
}

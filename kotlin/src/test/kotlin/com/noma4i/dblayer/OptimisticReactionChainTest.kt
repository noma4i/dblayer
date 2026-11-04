package com.noma4i.dblayer

import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(DBlayerTestingApi::class)
class OptimisticReactionChainTest {
    /** The order of the app: a send the server confirmed, then each reaction on the row the previous call returned. The session of the plain transport holds from the open. */
    @Test
    fun eachReactionOnTheRowOfThePreviousCallKeepsOtherUsersReactions() = bounded {
        openRecordSession(reactDelayMillis = 30_000, reacts = 4)
        var reacted = settledRecord().react(reactionBody = "A", userId = "u1")
        reacted = reacted.react(reactionBody = "B", userId = "u2")
        reacted = reacted.react(reactionBody = "C", userId = "u1")
        assertEquals(listOf("u1" to "C", "u2" to "B"), reacted.reactions.map { it.userId to it.body })

        reacted = reacted.react(reactionBody = "", userId = "u1")
        assertEquals(listOf("u2" to "B"), reacted.reactions.map { it.userId to it.body })
    }

    /** A database opened without a transport holds no session, so a send is refused (`docs/value-bridge.md`: a send needs a signed-in session). */
    @Test
    fun aSendOnADatabaseWithoutATransportIsRefusedAsNoSession() = bounded {
        try {
            DBlayer.close()
            DBlayer.openTesting(null, modelsJSON)
            val refused = runCatching { Record.send(AccountScope.current(), containerId = "c1", authorId = "u1", text = "hello") }.exceptionOrNull()
            assertTrue("the send was not refused: $refused", refused is DbException.NoSession)
        } finally {
            DBlayer.close()
        }
    }
}

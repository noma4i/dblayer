package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/** The transport of the app harness: a target that the tests set and unset. An unset target throws the error the app harness throws. */
class SwitchableTransport(@Volatile var target: MockTransport?, private val unset: () -> DbException) : ScriptedTransport {
    private fun resolve(): MockTransport = target ?: throw unset()

    override suspend fun query(request: TransportRequest): String = resolve().query(request)
    override suspend fun mutate(request: TransportRequest): String = resolve().mutate(request)
    override suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome = resolve().scriptedCable(url, events)
    override suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply = resolve().scriptedPut(url, headers, file, identity, progress)
}

@OptIn(DBlayerTestingApi::class)
class SwitchableHarnessTest {
    private fun profile(name: String) = """{"data":{"me":{"__typename":"User","id":"1","name":"$name"}}}"""

    private val operations = """[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"""
}

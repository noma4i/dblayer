package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Test
import java.util.concurrent.Executors

/** Property P-BR-10: calls started in an order enter the owners in that order, without the caller awaiting the previous call. The observable is the order of the mutation requests at the transport. */
class CallOrderTest {
    private val executor = Executors.newSingleThreadExecutor { Thread(it, "caller-main") }
    private val caller = executor.asCoroutineDispatcher()

    @After fun stop() = caller.close()

    /** Requirement: 12 creates launched on one thread without waiting for each other reach the transport in the order they were launched. Defect caught: the place of the call is taken after the call left the thread of the caller (D-419). */
    @Test
    fun callsStartedWithoutWaitingReachTheTransportInTheOrderTheyStarted() = bounded {
        val transport = MockTransport()
        transport.respondWith("CreateMoment") { """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""" }
        openSigned(Config(null), modelsJSON, transport, SEND_MOMENT, onlineAtOpen = true)
        val screen = CoroutineScope(SupervisorJob() + caller)
        try {
            val account = AccountScope.current()
            val names = (0 until 12).map { "call-%02d".format(it) }
            joinAll(*names.map { name -> screen.launch { Moment.create(text = name, media = null, account = account) } }.toTypedArray())
            eventually("every request reached the transport") { transport.calls.count { it.operationName == "CreateMoment" } == names.size }
            val sent = transport.calls.filter { it.operationName == "CreateMoment" }.map { Regex("call-\\d\\d").find(it.variablesJson)!!.value }
            assertEquals("the requests left in another order than the calls started", names, sent)
        } finally {
            screen.cancel()
            closeForCleanup()
        }
    }
}

package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Test

class OpenListeningTest {
    private object RecordEvents : GraphQLOperation {
        override val operationName = "RecordEvents"
        override val document = "subscription RecordEvents { recordEvents { entries { topic action recordId } } }"
    }

    @OptIn(DBlayerTestingApi::class)
    @Test
    fun inv_api_05_openTestingHoldsSubscriptionsUntilRegistrationCompletes() = bounded {
        DBlayer.close()
        try {
            val transport = MockTransport()
            DBlayer.openTesting(transport, modelsJSON, operations = listOf(RecordEvents), listening = false)
            DBlayer.resume()
            assertEquals(emptyList<String>(), transport.calls.map { it.operationName })
            DBlayer.setListening(true)
            eventually("registration complete starts the subscription") { transport.calls.size == 1 }
            DBlayer.setListening(true)
            DBlayer.resume()
            assertEquals(listOf("RecordEvents"), transport.calls.map { it.operationName })
        } finally {
            DBlayer.close()
        }
    }
}

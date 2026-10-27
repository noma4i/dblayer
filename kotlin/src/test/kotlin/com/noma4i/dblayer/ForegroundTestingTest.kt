package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Test

class ForegroundTestingTest {
    private object ForegroundFeed : GraphQLOperation {
        override val operationName = "MomentsFeed"
        override val document = "query MomentsFeed { feed { nodes { id uuid createdAt } pageInfo { hasNextPage endCursor } } }"
    }

    @OptIn(DBlayerTestingApi::class)
    @Test
    fun inv_api_07_aTestingForegroundEventReloadsOnlyAWindowWithAReader() = bounded {
        DBlayer.close()
        val scope = CoroutineScope(SupervisorJob())
        try {
            val models = dbJson.parseToJsonElement(modelsJSON).jsonArray.map { model ->
                if (model.jsonObject["key"]?.jsonPrimitive?.content != "Moment") model else {
                    val relations = model.jsonObject.getValue("relations").jsonArray.map { relation ->
                        if (relation.jsonObject["name"]?.jsonPrimitive?.content != "feed") relation else
                            JsonObject(relation.jsonObject + ("refresh_on_foreground" to JsonPrimitive(true)))
                    }
                    JsonObject(model.jsonObject + ("relations" to JsonArray(relations)))
                }
            }
            val transport = MockTransport()
            DBlayer.openTesting(transport, JsonArray(models).toString(), operations = listOf(ForegroundFeed))
            DBlayer.enterForegroundForTesting()
            DBlayer.resume()
            assertEquals(emptyList<String>(), transport.calls.map { it.operationName })

            transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"id":"m1","uuid":"first","createdAt":"2026-10-27T00:00:00Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
            transport.enqueue("MomentsFeed", """{"data":{"feed":{"nodes":[{"id":"m2","uuid":"second","createdAt":"2026-10-27T00:00:00Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}""")
            val list = LiveList(Moment.feed, scope)
            list.waitForInitialLoad()
            assertEquals(listOf("MomentsFeed"), transport.calls.map { it.operationName })
            DBlayer.enterForegroundForTesting()
            DBlayer.resume()
            withTimeout(5000) { list.ids.first { it == listOf("m2") } }
            assertEquals(listOf("MomentsFeed", "MomentsFeed"), transport.calls.map { it.operationName })
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }
}

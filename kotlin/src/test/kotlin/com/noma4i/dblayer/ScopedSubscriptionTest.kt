package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Test

class ScopedSubscriptionTest {
    @Test
    fun readingARelationWithParamsSubscribesWithThoseVariables() = bounded {
        val mock = MockTransport()
        val database = openDatabase(Config(null, null, 8u), modelsJSON, mock)
        database.registerOperations("""[{"name":"documentBySlug","document":"query documentBySlug(${'$'}slug: String!, ${'$'}source: String!) { documentBySlug(slug: ${'$'}slug, source: ${'$'}source) { id slug } }","rootPath":"documentBySlug"},{"name":"documentUpdated","document":"subscription documentUpdated(${'$'}slug: String!, ${'$'}source: String!) { documentUpdated(slug: ${'$'}slug, source: ${'$'}source) { id slug } }","rootPath":"documentUpdated"}]""")
        val listener = object : QueryListener { override fun changed(change: LiveChange) {} }
        val first = database.liveQuery("""{"model":"Document","relation":"bySlug","params":{"slug":"a","source":"web"}}""", listener)
        val second = database.liveQuery("""{"model":"Document","relation":"bySlug","params":{"slug":"b","source":"web"}}""", listener)
        try {
            eventually("both subscriptions", 2000) { mock.calls.count { it.operationName == "documentUpdated" } == 2 }
            val sent = mock.calls.filter { it.operationName == "documentUpdated" }.map { parseJson(it.variablesJson).toString() }.toSet()
            assertEquals(setOf(parseJson("""{"slug":"a","source":"web"}""").toString(), parseJson("""{"slug":"b","source":"web"}""").toString()), sent)
        } finally { first.cancel(); second.cancel() }
    }
}

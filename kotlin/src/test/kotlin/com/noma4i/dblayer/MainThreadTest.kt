package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.withContext
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Test
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors

class MainThreadTest {
    @Volatile private var mainThread: Thread? = null
    private val executor = Executors.newSingleThreadExecutor { Thread(it, "probe-main").also { thread -> mainThread = thread } }
    private val main = executor.asCoroutineDispatcher()
    private val touched = ConcurrentLinkedQueue<Pair<String, Thread>>()

    @After
    fun stop() { executor.shutdownNow() }

    private fun touch(stage: String) { touched += stage to Thread.currentThread() }

    private fun stages() = touched.map { it.first }.toSet()

    private fun onMain(stage: String) = touched.count { it.first == stage && it.second === mainThread }

    private class Probe(val moment: Moment, val touch: () -> Unit) : DBModel by moment {
        override val id: String get() { touch(); return moment.id }
    }

    @Test
    fun a_call_prepares_its_json_and_reads_its_answer_off_the_caller_thread() = bounded {
        val transport = MockTransport()
        transport.enqueue("QrCodeCreate", """{"data":{"qrCodeCreate":{"url":"https://q"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"QrCodeCreate","document":"mutation QrCodeCreate(${'$'}input: CreateQrCodeInput!) { qrCodeCreate(input: ${'$'}input) { url } }"}]""")
        val inner = mapOf<String, JsonElement>("input" to JsonPrimitive(1))
        val variables = JsonObject(object : Map<String, JsonElement> by inner {
            override val entries get() = inner.entries.also { touch("prepare") }
            override val size get() = inner.size.also { touch("prepare") }
        })
        val answer = object : KSerializer<String> {
            override val descriptor = PrimitiveSerialDescriptor("probe", PrimitiveKind.STRING)
            override fun serialize(encoder: Encoder, value: String) = throw UnsupportedOperationException()
            override fun deserialize(decoder: Decoder): String {
                touch("decode")
                return (decoder as JsonDecoder).decodeJsonElement().toString()
            }
        }

        val result = withContext(main) { DBlayer.run("QrCodeCreate", variables, answer) }

        assertEquals("""{"url":"https://q"}""", result)
        assertEquals(setOf("prepare", "decode"), stages())
        assertEquals(0, onMain("prepare"))
        assertEquals(0, onMain("decode"))
    }

    @Test
    fun a_live_list_prepares_its_ids_off_the_owner_thread() = bounded {
        reopen(Config(null, null, 64u), modelsJSON, null)
        repeat(2) { Moment.create(text = "t%05d".format(it)) }
        val decoder = object : ModelDecoder<Probe> {
            override val modelName = Moment.modelName
            override val layout = Moment.layout
            override fun decode(row: WireRow) = Probe(Moment.decode(row)) { touch("ids") }
        }
        val scope = CoroutineScope(main)
        try {
            val list = LiveList(Query("Moment", decoder).order(Order("text", "asc")), scope)

            eventually("the ids") { list.ids.value.size == 2 }

            assertEquals(setOf("ids"), stages())
            assertEquals(0, onMain("ids"))
        } finally {
            scope.cancel()
        }
    }
}

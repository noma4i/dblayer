package com.noma4i.dblayer

import com.noma4i.dblayer.models.Asset
import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.MomentInput
import com.noma4i.dblayer.models.Operations
import com.noma4i.dblayer.models.SettingsInput
import com.noma4i.dblayer.models.WidgetInput
import com.noma4i.dblayer.models.WidgetKind
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
import org.junit.Assert.assertTrue
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

    private fun recording(key: String): JsonObject {
        val inner = mapOf<String, JsonElement>(key to JsonPrimitive(1))
        return JsonObject(object : Map<String, JsonElement> by inner {
            override val entries get() = inner.entries.also { touch("prepare") }
            override val size get() = inner.size.also { touch("prepare") }
        })
    }

    private fun onMain(stage: String) = touched.count { it.first == stage && it.second === mainThread }

    private class Probe(val moment: Moment, val compared: () -> Unit = {}, val touch: () -> Unit) : DBModel by moment {
        override val id: String get() { touch(); return moment.id }
        override fun equals(other: Any?): Boolean { compared(); return other is Probe && other.moment == moment }
        override fun hashCode() = moment.hashCode()
    }

    @Test
    fun a_call_prepares_its_json_and_reads_its_answer_off_the_caller_thread() = bounded {
        val transport = MockTransport()
        transport.enqueue("QrCodeCreate", """{"data":{"qrCodeCreate":{"url":"https://q"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"QrCodeCreate","document":"mutation QrCodeCreate(${'$'}input: CreateQrCodeInput!) { qrCodeCreate(input: ${'$'}input) { url } }"}]""")
        val variables = recording("input")
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
    fun generated_create_and_update_prepare_their_json_off_the_caller_thread() = bounded {
        reopen(Config(null, null, 64u), modelsJSON, null)
        val media = recording("media")

        val created = withContext(main) { Moment.create(text = "a", media = media) }
        withContext(main) { created.update(text = GraphQLNullable.Value("b"), media = media) }

        assertEquals(setOf("prepare"), stages())
        assertEquals(0, onMain("prepare"))
    }

    @Test
    fun a_generated_operation_prepares_its_variables_off_the_caller_thread() = bounded {
        val transport = MockTransport()
        transport.enqueue("MomentCreate", """{"data":{"momentCreate":{"moment":{"id":9}}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"MomentCreate","document":"mutation MomentCreate(${'$'}input: MomentInput!) { momentCreate(input: ${'$'}input) { moment { id } } }"}]""")
        val widget = WidgetInput(kind = WidgetKind.Emoji, settings = SettingsInput(itemProps = recording("emoji")))

        withContext(main) { Operations.momentCreate(MomentInput(file = "s1", widgets = GraphQLNullable.Value(listOf(widget)))) }

        assertEquals(setOf("prepare"), stages())
        assertEquals(0, onMain("prepare"))
    }

    @Test
    fun uploads_prepare_their_request_off_the_caller_thread() = bounded {
        reopen(Config(null, null, 64u), modelsJSON, null)
        val moment = Moment.create(text = "a")
        val request = UploadRequest("/nonexistent", variables = recording("v"))

        runCatching { withContext(main) { moment.upload("upload_clip", request) } }
        runCatching { withContext(main) { uploadCreate("Moment", "upload_clip", recording("v"), request, Moment) } }
        runCatching { withContext(main) { Asset.uploadCreate(listOf(UploadItem(recording("v"), "/nonexistent")), UploadBatchRequest("create_files")) } }

        assertEquals(setOf("prepare"), stages())
        assertEquals(0, onMain("prepare"))
    }

    @Test
    fun a_warm_query_decodes_its_cached_rows_off_the_caller_thread() = bounded {
        reopen(Config(null, null, 64u), modelsJSON, null)
        repeat(2) { Moment.create(text = "t%05d".format(it)) }
        val decoder = object : ModelDecoder<Probe> {
            override val modelName = Moment.modelName
            override val layout = Moment.layout
            override fun decode(row: WireRow) = Probe(Moment.decode(row)) {}.also { touch("cached") }
        }
        val query = Query("Moment", decoder).order(Order("text", "asc"))
        query.warm()

        val cached = withContext(main) { query.cached() }
        val row = withContext(main) { query.cachedRow(cached!!.value.first().moment.id) }

        assertEquals(2, cached?.value?.size)
        assertEquals("t00000", row?.moment?.text)
        assertEquals(setOf("cached"), stages())
        assertEquals(0, onMain("cached"))
    }

    @Test
    fun a_same_size_change_of_the_last_row_is_published_off_the_owner_thread_by_lists_and_windows() = bounded {
        reopen(Config(null, null, 128u), modelsJSON, null)
        val created = List(40) { Moment.create(text = "t%05d".format(it)) }
        fun decoder(stage: String) = object : ModelDecoder<Probe> {
            override val modelName = Moment.modelName
            override val layout = Moment.layout
            override fun decode(row: WireRow) = Probe(Moment.decode(row), { touch(stage) }) {}
        }
        val scope = CoroutineScope(main)
        try {
            val ordered = { stage: String -> Query("Moment", decoder(stage)).order(Order("text", "asc")) }
            val list = LiveList(ordered("list"), scope)
            val window = LiveWindow(ordered("window").window(tail = 40), scope)
            eventually("the first delivery") { list.rows.value.size == 40 && window.rows.value.size == 40 }

            created[39].update(text = GraphQLNullable.Value("t00039x"))
            eventually("the second delivery") { list.rows.value.last().moment.text == "t00039x" && window.rows.value.last().moment.text == "t00039x" }

            for (stage in listOf("list", "window")) {
                assertTrue("$stage: the delivery compared the changed row", touched.any { it.first == stage })
                assertEquals("$stage: comparisons on the owner thread", 0, onMain(stage))
            }
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun the_owner_thread_consumes_a_list_and_a_window_while_their_publication_is_held_up() = bounded {
        reopen(Config(null, null, 128u), modelsJSON, null)
        val created = List(40) { Moment.create(text = "t%05d".format(it)) }
        val armed = java.util.concurrent.atomic.AtomicReference<Pair<java.util.concurrent.CountDownLatch, java.util.concurrent.CountDownLatch>?>(null)
        val decoder = object : ModelDecoder<Probe> {
            override val modelName = Moment.modelName
            override val layout = Moment.layout
            override fun decode(row: WireRow) = Probe(Moment.decode(row), {
                val (entered, gate) = armed.get() ?: return@Probe
                if (Thread.currentThread() !== mainThread) { entered.countDown(); gate.await(10, java.util.concurrent.TimeUnit.SECONDS) }
            }) {}
        }
        val scope = CoroutineScope(main)
        try {
            val query = Query("Moment", decoder).order(Order("text", "asc"))
            val list = LiveList(query, scope)
            val window = LiveWindow(query.window(tail = 40), scope)
            eventually("the first delivery") { list.rows.value.size == 40 && window.rows.value.size == 40 }

            list.takeUpdate()
            window.takeUpdate()
            var edit = 0
            suspend fun waitedForPublication(consume: () -> ListUpdate<Probe>): Boolean {
                val entered = java.util.concurrent.CountDownLatch(2)
                val gate = java.util.concurrent.CountDownLatch(1)
                armed.set(entered to gate)
                val text = "t00039-${edit++}"
                val updated = created[39].update(text = GraphQLNullable.Value(text))
                try {
                    assertTrue("a publication is comparing rows", entered.await(5, java.util.concurrent.TimeUnit.SECONDS))
                    val consumed = executor.submit<ListUpdate<Probe>> { consume() }
                    return try {
                        val update = consumed.get(2, java.util.concurrent.TimeUnit.SECONDS)
                        assertEquals("the rows and the edits of the held publication", text, update.rows.last().moment.text)
                        assertTrue("the edits reach the rows from the previous revision", update.edits?.any { it is ListEdit.Rewrite && it.index == 39 } == true)
                        false
                    } catch (_: java.util.concurrent.TimeoutException) { true }
                } finally {
                    gate.countDown()
                    armed.set(null)
                    assertEquals(text, updated.text)
                }
            }

            assertEquals("the owner thread waited for the list publication", false, waitedForPublication { list.takeUpdate() })
            eventually("the list publication ended") { list.rows.value.last().moment.text == "t00039-0" && window.rows.value.last().moment.text == "t00039-0" }
            assertEquals("the owner thread waited for the window publication", false, waitedForPublication { window.takeUpdate() })
        } finally {
            scope.cancel()
        }
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

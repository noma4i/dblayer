package com.noma4i.dblayer

import java.io.File
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.delay
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.longOrNull
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

private class Heard {
    val count = AtomicLong()
    fun add() { count.incrementAndGet() }
}

/** A stream the runner reads: a task pulls it into a queue, `pull` of the corpus takes the next value. */
private class Pulled(val stream: ValueStream, heard: Heard, scope: CoroutineScope) {
    private val queue = java.util.concurrent.ConcurrentLinkedQueue<ReadValue>()
    private val position = AtomicLong()
    private val job = scope.launch {
        try {
            stream.values.collect { value -> queue.add(value); heard.add() }
        } catch (_: Exception) {
        }
    }

    fun next(): Pair<ULong, ReadValue>? {
        val value = queue.poll() ?: return null
        return position.incrementAndGet().toULong() to value
    }

    fun stop() = job.cancel()
}

private class Credentials : CredentialStore {
    @Volatile var current: String? = null
    override fun get(): String? = current
    override fun set(credential: String) { current = credential }
    override fun clear() { current = null }
}

/** What a finished call hands back to the runner: what to observe and what to remember under a name. */
private class Done(val observed: List<JsonObject> = emptyList(), val row: Pair<String, String>? = null, val receipt: Pair<String, WireOperationReceipt>? = null)

private fun failure(kind: String): List<JsonObject> = listOf(obs("error", "kind" to kind))

private fun kindOf(error: DbException): String = when (error) {
    is DbException.NoSession -> "no_session"
    is DbException.Security -> "security"
    is DbException.Config -> "config"
    is DbException.Invalid -> "invalid"
    is DbException.NotFound -> "not_found"
    is DbException.AlreadyExists -> "already_exists"
    is DbException.Database -> "database"
    is DbException.Sync -> "sync"
    is DbException.Network -> if (error.detail == "the operation needs the network") "offline" else "network"
    is DbException.Interrupted -> "interrupted"
    is DbException.Http -> if (error.status == 401.toUShort()) "unauthorized" else "http"
    is DbException.RequestTooLarge -> "request_too_large"
    is DbException.Graphql -> "graphql"
    is DbException.Session -> "session"
    is DbException.Closed -> "closed"
    is DbException.CorruptManifest -> "corrupt_manifest"
}

private fun failed(error: Throwable): Done = Done(failure(if (error is DbException) kindOf(error) else "invalid"))

private fun done(kind: String, value: Any? = null): Done = Done(listOf(if (value == null) observation(kind) else observation(kind, js(value))))

private fun phaseName(phase: ReadPhase): String = when (phase) {
    ReadPhase.IDLE -> "idle"
    ReadPhase.LOADING -> "loading"
    ReadPhase.LOADED -> "loaded"
    ReadPhase.FAILED -> "failed"
}

private fun causeName(end: ReadEnd): String = when (end) {
    ReadEnd.SessionEnded -> "session_ended"
    ReadEnd.Closed -> "closed"
    ReadEnd.Cancelled -> "cancelled"
    ReadEnd.Invalid -> "invalid"
    is ReadEnd.Storage -> "storage"
}

private fun fetchOf(name: String?): ReadFetch? = when (name) {
    "never" -> ReadFetch.NEVER
    "stale" -> ReadFetch.STALE
    "force" -> ReadFetch.FORCE
    "if_missing" -> ReadFetch.IF_MISSING
    else -> null
}

private fun outcomeName(outcome: WireOperationOutcome): String = when (outcome) {
    is WireOperationOutcome.Confirmed -> "confirmed"
    is WireOperationOutcome.Rejected -> "rejected"
    WireOperationOutcome.Interrupted -> "interrupted"
}

private fun moreName(result: LoadMoreResult): String = when (result) {
    is LoadMoreResult.Loaded -> if (result.hasMore) "loaded_more" else "loaded_last"
    LoadMoreResult.NoMore -> "no_more"
    LoadMoreResult.AlreadyLoading -> "already_loading"
}

private val corpusDirectory = File("../core/tests/corpus")

private fun JsonObject.text(key: String): String? = (this[key] as? JsonPrimitive)?.contentOrNull

/** Runs the inputs of the `corpus` machine through the public API of `Database`. */
private class DatabaseRunner {
    var database: Database? = null
    val mock = CorpusMock()
    val clock = TestClock(1_800_000_000_000uL)
    val heard = Heard()
    val rows = HashMap<String, String>()
    val receipts = HashMap<String, WireOperationReceipt>()
    val loads = HashMap<String, LoadRequest>()
    val streams = HashMap<String, Pulled>()
    var calls = ArrayList<Deferred<Done>>()
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    fun idOf(name: String): String = rows[name] ?: name

    fun nameOf(id: String): String = rows.entries.firstOrNull { it.value == id }?.key ?: id

    fun names(list: List<WireRow>): List<String> = list.map { nameOf(it.id) }

    fun stateFields(stream: String, state: ReadState): MutableMap<String, Any?> {
        val out = mutableMapOf<String, Any?>("stream" to stream, "phase" to phaseName(state.phase))
        state.error?.let { out["error"] = it.toString() }
        state.loaded?.let { out["loaded"] = it }
        return out
    }

    fun value(stream: String, value: ReadValue): JsonObject = when (value) {
        is ReadValue.Snapshot -> observation("snapshot", js(stateFields(stream, value.state).also { it["revision"] = value.revision; it["rows"] = names(value.rows) }))
        is ReadValue.Delta -> observation("delta", js(stateFields(stream, value.state).also { it["base"] = value.base; it["revision"] = value.revision; it["rows"] = names(value.edits.mapNotNull { (it as? RowEdit.Insert)?.row ?: (it as? RowEdit.Rewrite)?.row }); it["removed"] = value.edits.mapNotNull { (it as? RowEdit.Remove)?.id }.map(::nameOf) }))
        is ReadValue.Count -> observation("count", js(stateFields(stream, value.state).also { it["revision"] = value.revision; it["count"] = value.count }))
        is ReadValue.State -> observation("state", js(stateFields(stream, value.state)))
        ReadValue.Ready -> obs("ready", "stream" to stream)
        ReadValue.Gap -> obs("gap", "stream" to stream)
        is ReadValue.Retry -> observation("unsupported", JsonPrimitive("retry"))
        is ReadValue.Event -> obs("event", "stream" to stream, "position" to value.position, "area" to value.area, "kind" to value.kind, "row" to nameOf(value.id))
        is ReadValue.Lost -> obs("lost", "stream" to stream, "first" to value.first, "last" to value.last)
        is ReadValue.Status -> observation("unsupported", JsonPrimitive("status"))
        is ReadValue.Ended -> obs("ended", "stream" to stream, "cause" to causeName(value.cause))
    }

    fun spawn(work: suspend () -> Done) { calls.add(scope.async { try { work() } catch (error: Throwable) { failed(error) } }) }

    suspend fun settle(): List<JsonObject> {
        quiesce("the corpus run to go quiet") { listOf(mock.count.toLong(), calls.count { it.isCompleted }.toLong(), heard.count.get().toLong()) }
        val observed = ArrayList<JsonObject>()
        val waiting = ArrayList<Deferred<Done>>()
        for (call in calls) {
            if (!call.isCompleted) { waiting.add(call); continue }
            val result = call.await()
            observed.addAll(result.observed)
            result.row?.let { rows[it.first] = it.second }
            result.receipt?.let { receipts[it.first] = it.second }
        }
        calls = waiting
        observed.addAll(mock.announce())
        return observed
    }

    suspend fun open(models: String, encrypted: Boolean): List<JsonObject> = try {
        val session = if (encrypted) EncryptedConfig("https://api.example.com", SealedServer.SERVER_KEY, SealedServer.SERVER_PIN, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("otp" to EncryptedSessionOperation("/api/auth/otp", EncryptedOperationClass.AUTHENTICATED), "confirm" to EncryptedSessionOperation("/api/auth/confirm", EncryptedOperationClass.AUTHENTICATED), "social" to EncryptedSessionOperation("/api/auth/social", EncryptedOperationClass.AUTHENTICATED), "ticket" to EncryptedSessionOperation("/api/ticket", EncryptedOperationClass.SESSIONLESS))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel") else null
        val opened = OpenDatabase().openWithClock(Config(null, null, 32u), File(corpusDirectory, if (models == "events") "models_events.json" else "models.json").readText(), OpenPorts(mock, session, if (encrypted) Credentials() else null, if (encrypted) mock else null), clock)
        opened.registerOperations(File(corpusDirectory, "operations.json").readText())
        opened.setOnline(true)
        database = opened
        settle()
        listOf(observation("opened"))
    } catch (error: Exception) { failure("open") }

    suspend fun receiptDone(name: String, row: String, op: String, call: suspend () -> WireOperationReceipt): Done =
        Done(listOf(obs("receipt", "row" to row, "op" to op)), receipt = name to call())

    fun start(database: Database, name: String, args: JsonObject): List<JsonObject>? {
        val epoch = database.sessionEpoch()
        fun text(key: String): String? = args.text(key)
        fun label(row: String, suffix: String): String = text("as") ?: "$row:$suffix"
        when (name) {
            "create" -> {
                val row = text("as") ?: return failure("arguments")
                val body = text("text") ?: return failure("arguments")
                spawn {
                    val receipt = database.send(CommandRequest(epoch, "Moment", JsonObject(mapOf("text" to JsonPrimitive(body))).toString(), "{}", null))
                    Done(listOf(obs("receipt", "row" to row, "op" to "create")), row = receipt.optimisticValue?.let { row to it.id }, receipt = row to receipt)
                }
            }
            "update" -> {
                val row = text("row") ?: return failure("arguments")
                val body = text("text") ?: return failure("arguments")
                val id = idOf(row); val key = label(row, "update")
                spawn { receiptDone(key, row, "update") { database.update(epoch, "Moment", id, JsonObject(mapOf("text" to JsonPrimitive(body))).toString(), "{}") } }
            }
            "act" -> {
                val row = text("row") ?: return failure("arguments")
                val action = text("action") ?: return failure("arguments")
                val id = idOf(row); val key = label(row, action)
                spawn { receiptDone(key, row, "act") { database.act(epoch, "Moment", id, action, "{}", "{}") } }
            }
            "destroy" -> {
                val row = text("row") ?: return failure("arguments")
                val id = idOf(row); val key = label(row, "destroy")
                spawn { receiptDone(key, row, "destroy") { database.destroy(epoch, "Moment", id) } }
            }
            "retry" -> {
                val row = text("row") ?: return failure("arguments")
                val id = idOf(row); val key = label(row, "retry"); val action = text("action")
                spawn { receiptDone(key, row, "retry") { database.retry(epoch, "Moment", id, action, null) } }
            }
            "discard" -> {
                val row = text("row") ?: return failure("arguments")
                val id = idOf(row); val action = text("action")
                spawn { database.discard(epoch, "Moment", id, action); done("discarded", mapOf("row" to row)) }
            }
            "upload" -> {
                val row = text("row") ?: return failure("arguments")
                val action = text("action") ?: return failure("arguments")
                val id = idOf(row); val key = label(row, action)
                val file = File.createTempFile("corpus_", ".bin").also { it.writeText("corpus"); it.deleteOnExit() }
                val request = WireUploadRequest(file.path, "clip.bin", "application/octet-stream", "{}")
                spawn { receiptDone(key, row, "upload") { database.upload(epoch, "Moment", id, action, request) } }
            }
            "cancel_upload" -> {
                val row = text("row") ?: return failure("arguments")
                val action = text("action") ?: return failure("arguments")
                val id = idOf(row)
                spawn { done("cancelled", mapOf("row" to row, "value" to database.cancelUpload(epoch, "Moment", id, action))) }
            }
            "outcome" -> {
                val key = text("receipt") ?: return failure("arguments")
                val receipt = receipts[key] ?: return failure("unknown_receipt")
                spawn { done("outcome", mapOf("receipt" to key, "result" to outcomeName(database.outcome(receipt)))) }
            }
            "begin_load" -> {
                val key = text("as") ?: return failure("arguments")
                val relation = text("relation") ?: return failure("arguments")
                try {
                    val load = database.beginLoad(epoch, "Moment", relation, "{}")
                    loads[key] = load
                    spawn { load.result(); done("loaded", mapOf("load" to key)) }
                } catch (error: DbException) { return failure(kindOf(error)) }
            }
            "refresh" -> {
                val relation = text("relation") ?: return failure("arguments")
                spawn { database.refresh(epoch, "Moment", relation, "{}"); done("refreshed", mapOf("relation" to relation)) }
            }
            "load_more" -> {
                val relation = text("relation") ?: return failure("arguments")
                spawn { done("more", mapOf("relation" to relation, "result" to moreName(database.loadMore(epoch, "Moment", relation, "{}")))) }
            }
            "node" -> {
                val relation = text("relation") ?: return failure("arguments")
                val row = text("row") ?: return failure("arguments")
                val id = idOf(row)
                spawn { done("node", mapOf("row" to row, "present" to (database.node(epoch, "Moment", relation, id, false) != null))) }
            }
            "run" -> {
                val operation = text("operation") ?: return failure("arguments")
                val variables = (args["variables"] ?: JsonObject(emptyMap())).toString()
                spawn { done("ran", mapOf("operation" to operation, "answer" to Json.parseToJsonElement(database.run(epoch, operation, variables)))) }
            }
            "nodes" -> {
                val relation = text("relation") ?: return failure("arguments")
                val names = (args["rows"] as? JsonArray)?.map { (it as JsonPrimitive).content } ?: return failure("arguments")
                val model = text("model") ?: "Moment"
                val ids = names.map(::idOf)
                val byId = ids.zip(names).toMap()
                spawn { done("nodes", mapOf("relation" to relation, "rows" to database.nodes(epoch, model, relation, ids).map { byId[it.id] ?: it.id })) }
            }
            "begin_load_all" -> {
                val key = text("as") ?: return failure("arguments")
                val relation = text("relation") ?: return failure("arguments")
                try {
                    val load = database.beginLoadAll(epoch, "Moment", relation, "{}", (args["force"] as? JsonPrimitive)?.booleanOrNull ?: false)
                    loads[key] = load
                    spawn { load.result(); done("loaded_all", mapOf("load" to key)) }
                } catch (error: DbException) { return failure(kindOf(error)) }
            }
            "peek_row" -> {
                val row = text("row") ?: return failure("arguments")
                return try {
                    listOf(obs("peeked", "row" to row, "present" to (database.peekRow(epoch, text("model") ?: "Moment", idOf(row)) != null)))
                } catch (error: DbException) { failure(kindOf(error)) }
            }
            "edge_values" -> {
                val relation = text("relation") ?: return failure("arguments")
                val names = (args["rows"] as? JsonArray)?.map { (it as JsonPrimitive).content } ?: return failure("arguments")
                val model = text("model") ?: "Moment"
                val ids = names.map(::idOf)
                val byId = ids.zip(names).toMap()
                spawn {
                    val parsed = Json.parseToJsonElement(database.edgeValues(epoch, model, relation, "{}", ids)).jsonObject
                    done("edges", mapOf("relation" to relation, "values" to parsed.entries.associate { (byId[it.key] ?: it.key) to it.value }))
                }
            }
            "upload_batch" -> {
                val key = text("as") ?: return failure("arguments")
                val count = (args["count"] as? JsonPrimitive)?.longOrNull?.toInt() ?: return failure("arguments")
                val operation = text("operation") ?: return failure("arguments")
                val file = File.createTempFile("corpus_batch_", ".bin").also { it.writeText("corpus"); it.deleteOnExit() }
                val items = (1..count).map { WireUploadItem(JsonObject(mapOf("text" to JsonPrimitive("$key$it"))).toString(), file.path, "clip.bin", "application/octet-stream", "{}") }
                spawn {
                    val receipt = database.uploadCreateBatch(epoch, "Moment", items, WireUploadBatch(operation, "{}"))
                    Done(receipt.items.indices.map { obs("receipt", "row" to "$key${it + 1}", "op" to "upload") })
                }
            }
            "session_call" -> {
                val operation = text("operation") ?: return failure("arguments")
                val body = args["body"] ?: return failure("arguments")
                spawn { done("session", mapOf("operation" to operation, "answer" to Json.parseToJsonElement(database.sessionCall(operation, body.toString())))) }
            }
            "session_reset" -> spawn { database.sessionReset(); done("reset") }
            "close" -> { mock.releaseAll(); spawn { database.closeDatabase(); done("closed") } }
            "has" -> {
                val row = text("row") ?: return failure("arguments")
                val id = idOf(row)
                spawn { done("row", mapOf("row" to row, "present" to database.readQuery(epoch, """{"model":"Moment"}""").any { it.id == id })) }
            }
            else -> return null
        }
        return emptyList()
    }

    fun openStream(database: Database, name: String, args: JsonObject): List<JsonObject> {
        val key = args.text("as") ?: return failure("arguments")
        fun query(relation: String?): String = if (relation != null) JsonObject(mapOf("model" to JsonPrimitive("Moment"), "relation" to JsonPrimitive(relation), "params" to JsonObject(emptyMap()))).toString() else """{"model":"Moment"}"""
        val request = when (name) {
            "open_list" -> ReadRequest.List(query(args.text("relation")))
            "open_count" -> ReadRequest.Count(query(args.text("relation")))
            "open_entity" -> {
                val row = args.text("row") ?: return failure("arguments")
                val fetch = fetchOf(args.text("fetch")) ?: return failure("arguments")
                ReadRequest.Entity("Moment", idOf(row), fetch)
            }
            else -> ReadRequest.Events(args.text("model") ?: return failure("arguments"))
        }
        streams[key] = Pulled(ValueStream(database.sessionEpoch(), database.subscribeValues(database.sessionEpoch(), request), database, null), heard, scope)
        return emptyList()
    }

    suspend fun apply(input: JsonObject): List<JsonObject> {
        val name = input.text("in") ?: ""
        val args = (input["args"] as? JsonObject) ?: JsonObject(emptyMap())
        when (name) {
            "open" -> return open(args.text("models") ?: "", (args["encrypted"] as? JsonPrimitive)?.booleanOrNull ?: false)
            "tick" -> { clock.advance((args["ms"] as? JsonPrimitive)?.longOrNull?.toULong() ?: 0uL); return settle() }
            "answer", "error", "drop", "ready", "frame" -> {
                val id = args.text("id") ?: return failure("arguments")
                val reached = when (name) {
                    "answer" -> mock.answer(id, (args["json"] ?: return failure("arguments")).toString())
                    "error" -> mock.fail(id, args.text("kind") ?: return failure("arguments"))
                    "drop" -> mock.dropConnection(id)
                    "ready" -> mock.ready(id)
                    else -> try { mock.frame(id, (args["event"] ?: return failure("arguments")).toString()) } catch (error: Exception) { return failure("frame") }
                }
                if (!reached) return failure("no_request")
                return settle()
            }
            "lost" -> return listOf(observation("unsupported", JsonPrimitive(name)))
        }
        val database = database ?: return failure("not_open")
        val online = (args["online"] as? JsonPrimitive)?.booleanOrNull ?: false
        val observed = ArrayList<JsonObject>()
        when (name) {
            "online" -> try { database.onlineSink().setOnline(online) } catch (error: Exception) { observed.addAll(failure("online")) }
            "set_online" -> try { database.setOnline(online); observed.add(observation("online", JsonPrimitive(online))) } catch (error: Exception) { observed.addAll(failure("online")) }
            "published" -> observed.add(obs("published", "epoch" to database.sessionEpoch()))
            "open_list", "open_count", "open_entity", "open_events" -> observed.addAll(openStream(database, name, args))
            "pull" -> {
                val key = args.text("stream") ?: return failure("arguments")
                val pulled = streams[key] ?: return failure("unknown_stream")
                val next = pulled.next()
                observed.add(if (next != null) value(key, next.second) else obs("empty", "stream" to key))
            }
            "cancel" -> {
                val target = args.text("target") ?: return failure("arguments")
                val load = loads[target]
                val pulled = streams[target]
                if (load != null) load.cancel() else if (pulled != null) spawn { pulled.stream.cancel(); Done() } else return failure("unknown_target")
            }
            else -> observed.addAll(start(database, name, args) ?: return listOf(observation("unsupported", JsonPrimitive(name))))
        }
        observed.addAll(settle())
        if (name == "close" && calls.isEmpty()) this.database = null
        return observed
    }
}

private suspend fun runCorpus(file: java.io.File): String? {
    val runner = DatabaseRunner()
    val lines = file.readLines().filter { it.isNotBlank() }.drop(1)
    var red: String? = null
    for ((index, line) in lines.withIndex()) {
        val step = Json.parseToJsonElement(line).jsonObject
        val expected: JsonElement = step["out"] ?: JsonArray(emptyList())
        val got = JsonArray(runner.apply(step))
        if (expected != got) { red = "${file.name} step ${index + 1} input ${step["in"]} expected $expected got $got"; break }
    }
    runner.mock.releaseAll()
    runner.database?.closeDatabase()
    return red
}

class CorpusRunnerTest {
    @Test
    fun theSharedCorpusIsGreenThroughTheKotlinWrapper() = bounded {
        val files = corpusDirectory.listFiles { file -> file.extension == "jsonl" }!!.sortedBy { it.name }
        assertTrue(files.isNotEmpty())
        val gate = kotlinx.coroutines.sync.Semaphore(6)
        val red = files.map { file -> async(kotlinx.coroutines.Dispatchers.Default) { gate.withPermit { runCorpus(file) } } }.awaitAll().filterNotNull()
        assertTrue("red traces:\n" + red.joinToString("\n"), red.isEmpty())
    }
}

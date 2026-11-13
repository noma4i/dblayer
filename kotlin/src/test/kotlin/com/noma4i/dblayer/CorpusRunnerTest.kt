package com.noma4i.dblayer

import java.io.File
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
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
    fun add() { count.incrementAndGet(); TestPulse.pulse() }
}

/** A stream the runner reads: a task pulls it into a queue, `pull` of the corpus folds the unread values. */
private class Pulled(val stream: ValueStream, heard: Heard, scope: CoroutineScope) {
    private val queue = java.util.concurrent.ConcurrentLinkedQueue<ReadValue>()
    private val job = scope.launch {
        try {
            stream.values.collect { value -> queue.add(value); heard.add() }
        } catch (_: Exception) {
        }
    }

    fun peek(): List<ReadValue> = queue.toList()

    fun drain(): List<ReadValue> = generateSequence { queue.poll() }.toList()

    fun stop() = job.cancel()
}

private sealed class Folded {
    class Delta(val base: Long, val revision: Long, val rows: MutableList<String>, val removed: MutableList<String>, val state: ReadState) : Folded()
    class Value(val value: ReadValue) : Folded()
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
    is DbException.Storage -> "storage"
    is DbException.Offline -> "offline"
    is DbException.Dropped -> "dropped"
    is DbException.Unauthorized -> "unauthorized"
    is DbException.InFlight -> "in_flight"
    is DbException.Runtime -> "runtime"
    is DbException.Internal -> "internal"
    is DbException.Copy -> "copy"
    is DbException.Network -> "network"
    is DbException.Interrupted -> "interrupted"
    is DbException.Http -> "http"
    is DbException.RequestTooLarge -> "request_too_large"
    is DbException.ResponseTooLarge -> "response_too_large"
    is DbException.Graphql -> "graphql"
    is DbException.Session -> "session"
    is DbException.Closed -> "closed"
    is DbException.CorruptManifest -> "corrupt_manifest"
    is DbException.NotOpen -> "not_open"
    is DbException.Rejected -> "rejected"
}

private fun failed(error: Throwable): Done = Done(failure(if (error is DbException) kindOf(error) else "invalid"))

private fun done(kind: String, value: Any? = null): Done = Done(listOf(if (value == null) observation(kind) else observation(kind, js(value))))

private fun phaseName(phase: ReadPhase): String = when (phase) {
    ReadPhase.IDLE -> "idle"
    ReadPhase.LOADING -> "loading"
    ReadPhase.LOADED -> "loaded"
    ReadPhase.FAILED -> "failed"
}

private fun causeName(end: EndCause): String = when (end) {
    EndCause.SessionEnded -> "session_ended"
    EndCause.Closed -> "closed"
    EndCause.Cancelled -> "cancelled"
    EndCause.Discarded -> "discarded"
    is EndCause.Invalid -> "invalid"
    is EndCause.Storage -> "storage"
    is EndCause.Internal -> "internal"
}

private fun fetchOf(name: String?): ReadFetch? = when (name) {
    "never" -> ReadFetch.NEVER
    "stale" -> ReadFetch.STALE
    "force" -> ReadFetch.FORCE
    "if_missing" -> ReadFetch.IF_MISSING
    else -> null
}

private suspend fun outcomeName(database: Database, receipt: WireOperationReceipt): String = try {
    database.confirmed(receipt, false)
    "confirmed"
} catch (_: DbException.Rejected) {
    "rejected"
} catch (_: DbException.NoSession) {
    "ended session_ended"
} catch (_: DbException.Closed) {
    "ended closed"
} catch (_: DbException.Invalid) {
    "ended invalid"
} catch (_: DbException.Storage) {
    "ended storage"
} catch (_: DbException.Internal) {
    "ended internal"
} catch (interrupted: DbException.Interrupted) {
    "ended ${interrupted.detail.lowercase()}"
}

private fun moreName(result: LoadMoreResult): String = when (result) {
    is LoadMoreResult.Loaded -> if (result.hasMore) "loaded_more" else "loaded_last"
    LoadMoreResult.NoMore -> "no_more"
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
    var wanted = 0
    private var previousStart = 0uL
    private var currentStart = 0uL
    var wantedFirst: String? = null
    var wantedHere: (String) -> Int = { 0 }
    var at = ""
    private val fence: suspend () -> Unit = { runCatching { database?.subscriptionCensus() } }
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

    fun folded(stream: String, values: List<ReadValue>): List<JsonObject> {
        val folded = ArrayList<Folded>()
        for (value in values) {
            when (value) {
                is ReadValue.Delta -> {
                    val last = folded.lastOrNull() as? Folded.Delta
                    val rows = last?.rows ?: ArrayList()
                    val removed = last?.removed ?: ArrayList()
                    apply(value.edits, rows, removed)
                    val next = Folded.Delta(last?.base ?: value.base, value.revision, rows, removed, value.state)
                    if (last != null) folded[folded.size - 1] = next else folded.add(next)
                }
                is ReadValue.Snapshot, is ReadValue.Count -> {
                    folded.removeAll { it is Folded.Delta || (it as Folded.Value).value.let { kept -> kept is ReadValue.State || kept is ReadValue.Snapshot || kept is ReadValue.Count } }
                    folded.add(Folded.Value(value))
                }
                is ReadValue.State -> {
                    folded.removeAll { it is Folded.Value && it.value is ReadValue.State }
                    folded.add(Folded.Value(value))
                }
                else -> folded.add(Folded.Value(value))
            }
        }
        return folded.map { item ->
            when (item) {
                is Folded.Delta -> observation("delta", js(stateFields(stream, item.state).also { it["base"] = item.base; it["revision"] = item.revision; it["rows"] = item.rows.map(::nameOf); it["removed"] = item.removed.map(::nameOf) }))
                is Folded.Value -> value(stream, item.value)
            }
        }
    }

    fun apply(edits: List<RowEdit>, rows: MutableList<String>, removed: MutableList<String>) {
        for (edit in edits) {
            when (edit) {
                is RowEdit.Insert -> { removed.remove(edit.row.id); if (edit.row.id !in rows) rows.add(edit.row.id) }
                is RowEdit.Rewrite -> { removed.remove(edit.row.id); if (edit.row.id !in rows) rows.add(edit.row.id) }
                is RowEdit.Remove -> { rows.remove(edit.id); if (edit.id !in removed) removed.add(edit.id) }
                is RowEdit.Move -> {}
            }
        }
    }

    fun value(stream: String, value: ReadValue): JsonObject = when (value) {
        is ReadValue.Snapshot -> observation("snapshot", js(stateFields(stream, value.state).also { it["revision"] = value.revision; it["rows"] = names(value.rows) }))
        is ReadValue.Delta -> error("a delta is shown folded")
        is ReadValue.Count -> observation("count", js(stateFields(stream, value.state).also { it["revision"] = value.revision; it["count"] = value.count }))
        is ReadValue.State -> observation("state", js(stateFields(stream, value.state)))
        ReadValue.Ready -> obs("ready", "stream" to stream)
        ReadValue.Gap -> obs("gap", "stream" to stream)
        is ReadValue.Changes -> obs("changes", "stream" to stream, "revision" to value.revision, "rows" to value.rows.size)
        is ReadValue.Event -> obs("event", "stream" to stream, "position" to value.position, "area" to value.area, "kind" to value.kind, "row" to nameOf(value.id))
        is ReadValue.Lost -> obs("lost", "stream" to stream, "first" to value.first, "last" to value.last)
        is ReadValue.Status -> observation("unsupported", JsonPrimitive("status"))
        is ReadValue.Operations -> observation("unsupported", JsonPrimitive("operations"))
        is ReadValue.CableClose -> observation("unsupported", JsonPrimitive("cable_close"))
        is ReadValue.Ended -> obs("ended", "stream" to stream, "cause" to causeName(value.cause))
    }

    fun spawn(work: suspend () -> Done) { calls.add(scope.async { try { work() } catch (error: Throwable) { failed(error) } }.also { call -> call.invokeOnCompletion { TestPulse.pulse() } }) }

    @OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
    private fun available(): Int = mock.unannounced + calls.filter { it.isCompleted && !it.isCancelled }.sumOf { it.getCompleted().observed.size }

    suspend fun settle(already: Int = 0): List<JsonObject> {
        eventually("${wanted - already} observations of $at") { available() >= wanted - already }
        fence()
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
        val opened = OpenDatabase().openWithClock(Config(null, null), withRawCalls(File(corpusDirectory, if (models == "events") "models_events.json" else "models.json").readText(), (parseJson(File(corpusDirectory, "operations.json").readText()) as kotlinx.serialization.json.JsonArray).map { (it as kotlinx.serialization.json.JsonObject).getValue("name").toString().trim('"') }), OpenPorts(mock, session, if (encrypted) Credentials() else null, if (encrypted) mock else null, SCRIPTED_ENDPOINT, partialOperations = true), clock, OpenOptions(emptyList()), null)
        registerStandOperations(opened, File(corpusDirectory, "operations.json").readText())
        opened.setOnline(true)
        database = opened
        settle(wanted)
        listOf(observation("opened"))
    } catch (error: Exception) { failure("open") }

    suspend fun receiptDone(name: String, row: String, op: String, call: suspend () -> WireOperationReceipt): Done =
        Done(listOf(obs("receipt", "row" to row, "op" to op)), receipt = name to call())

    suspend fun start(database: Database, name: String, args: JsonObject): List<JsonObject>? {
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
                val request = WireUploadRequest(WireSource.File(file.path, "clip.bin", "application/octet-stream", null), "{}")
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
                spawn { done("outcome", mapOf("receipt" to key, "result" to outcomeName(database, receipt))) }
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
                spawn { done("ran", mapOf("operation" to operation, "answer" to Json.parseToJsonElement(database.call(epoch, operation, variables)))) }
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
                    val edges = database.edgeValues(epoch, model, relation, "{}", ids)
                    done("edges", mapOf("relation" to relation, "values" to edges.associate { (byId[it.id] ?: it.id) to Cell.Keyed(it.values).asJson() }))
                }
            }
            "upload_batch" -> {
                val key = text("as") ?: return failure("arguments")
                val count = (args["count"] as? JsonPrimitive)?.longOrNull?.toInt() ?: return failure("arguments")
                val operation = text("operation") ?: return failure("arguments")
                val file = File.createTempFile("corpus_batch_", ".bin").also { it.writeText("corpus"); it.deleteOnExit() }
                val items = (1..count).map { WireUploadItem(JsonObject(mapOf("text" to JsonPrimitive("$key$it"))).toString(), WireSource.File(file.path, "clip.bin", "application/octet-stream", null), "{}") }
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
                spawn { done("row", mapOf("row" to row, "present" to database.readQuery(epoch, QuerySpec("Moment", null, "{}", emptyList(), emptyList(), null, null, false)).any { it.id == id })) }
            }
            else -> return null
        }
        return emptyList()
    }

    fun openStream(database: Database, name: String, args: JsonObject): List<JsonObject> {
        val key = args.text("as") ?: return failure("arguments")
        fun query(relation: String?) = QuerySpec("Moment", relation, "{}", emptyList(), emptyList(), null, null, false)
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
        streams[key] = Pulled(ValueStream(database.sessionEpoch(), database.subscribeValues(database.sessionEpoch(), request)), heard, scope)
        return emptyList()
    }

    suspend fun apply(input: JsonObject): List<JsonObject> {
        val name = input.text("in") ?: ""
        val args = (input["args"] as? JsonObject) ?: JsonObject(emptyMap())
        previousStart = currentStart
        currentStart = clock.armed()
        when (name) {
            "open" -> return open(args.text("models") ?: "", (args["encrypted"] as? JsonPrimitive)?.booleanOrNull ?: false)
            "tick" -> {
                val ms = (args["ms"] as? JsonPrimitive)?.longOrNull?.toULong() ?: 0uL
                if (ms > 0uL && wanted > 0 && clock.armed() <= previousStart) awaitWithin("the timer of $at is armed") { clock.awaitArmed(previousStart) }
                clock.advance(ms)
                return settle()
            }
            "answer", "error", "drop", "ready", "frame" -> {
                val id = args.text("id") ?: return failure("arguments")
                if (wantedFirst != "error") eventually("the request $id of $at") { mock.knows(id) }
                val reached = when (name) {
                    "answer" -> mock.answer(id, (args["json"] ?: return failure("arguments")).toString())
                    "error" -> mock.fail(id, args.text("kind") ?: return failure("arguments"))
                    "drop" -> mock.dropConnection(id)
                    "ready" -> mock.ready(id)
                    else -> try {
                        val event = (args["event"] ?: return failure("arguments")).toString()
                        var delivered = mock.frame(id, event)
                        if (!delivered && wantedFirst != "error") eventually("the frame $id of $at") { delivered = mock.frame(id, event); delivered }
                        delivered
                    } catch (error: Exception) { return failure("frame") }
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
                val wantedValues = wantedHere(key)
                if (wantedValues > 0) eventually("$wantedValues values of the stream $key at $at") { folded(key, pulled.peek()).size >= wantedValues }
                val unread = pulled.drain()
                observed.addAll(if (unread.isEmpty()) listOf(obs("empty", "stream" to key)) else folded(key, unread))
            }
            "cancel" -> {
                val target = args.text("target") ?: return failure("arguments")
                val load = loads[target]
                val pulled = streams[target]
                if (load != null) load.cancel() else if (pulled != null) spawn { pulled.stream.cancel(); Done() } else return failure("unknown_target")
            }
            else -> observed.addAll(start(database, name, args) ?: return listOf(observation("unsupported", JsonPrimitive(name))))
        }
        observed.addAll(settle(observed.size))
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
        runner.at = "${file.name} step ${index + 1} input ${step["in"]}"
        runner.wanted = (expected as? JsonArray)?.size ?: 0
        runner.wantedHere = { key -> (expected as? JsonArray).orEmpty().count { entry -> val item = entry as? JsonObject; (item?.get("v") as? JsonObject)?.get("stream")?.let { (it as? JsonPrimitive)?.contentOrNull } == key && (item?.get("o") as? JsonPrimitive)?.contentOrNull != "empty" } }
        runner.wantedFirst = ((expected as? JsonArray)?.firstOrNull() as? JsonObject)?.get("o")?.let { (it as? JsonPrimitive)?.contentOrNull }
        val got = JsonArray(runner.apply(step))
        if (expected != got) { red = "${file.name} step ${index + 1} input ${step["in"]} expected $expected got $got"; break }
    }
    runner.mock.releaseAll()
    runner.database?.closeDatabase()
    return red
}

@org.junit.runner.RunWith(org.junit.runners.Parameterized::class)
class CorpusRunnerTest(private val file: File) {
    companion object {
        @JvmStatic
        @org.junit.runners.Parameterized.Parameters(name = "{0}")
        fun traces(): List<Array<File>> {
            val files = corpusDirectory.listFiles { file -> file.extension == "jsonl" }!!.sortedBy { it.name }
            require(files.isNotEmpty())
            return files.map { arrayOf(it) }
        }
    }

    @Test
    fun bridge_prop_one_history_theSharedCorpusIsGreenThroughTheKotlinWrapper() = bounded {
        val red = runCorpus(file)
        assertTrue(red ?: "", red == null)
    }
}

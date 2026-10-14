package com.noma4i.dblayer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonNull
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

private fun <Data> decodeResponse(body: ByteArray, serializer: kotlinx.serialization.KSerializer<Data>): Response<Data> {
    val root = parseJson(String(body)) as? JsonObject ?: throw GraphQLTransportError.Server(0)
    val data = root["data"]?.takeUnless { it is JsonNull }?.let { dbJson.decodeFromJsonElement(serializer, it) }
    val errors = (root["errors"] as? kotlinx.serialization.json.JsonArray).orEmpty().mapNotNull { GraphQLError.parse(it) }
    return Response(data, errors)
}

class NetworkTransport(private val run: OperationRun, private val cable: CableClient) : GraphQLTransport {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    override suspend fun <Data, Variables> query(operation: GraphQLQuery<Data, Variables>, variables: Variables): Response<Data> {
        val body = run.send(operation.operationName, operation.document, dbJson.encodeToJsonElement(operation.variablesSerializer, variables), GraphQLOperationKind.Query)
        return decodeResponse(body, operation.dataSerializer)
    }

    override suspend fun <Data, Variables> mutate(operation: GraphQLMutation<Data, Variables>, variables: Variables): Response<Data> {
        val body = run.send(operation.operationName, operation.document, dbJson.encodeToJsonElement(operation.variablesSerializer, variables), GraphQLOperationKind.Mutation, dataSystemMutation = true)
        return decodeResponse(body, operation.dataSerializer)
    }

    override fun <Data, Variables> subscribe(
        operation: GraphQLSubscription<Data, Variables>,
        variables: Variables,
        onReady: () -> Unit,
        onNext: (Response<Data>) -> Unit,
        onError: (SubscriptionFailure) -> Unit,
    ): SubscriptionHandle {
        val job: Job = scope.launch {
            try {
                cable.subscribeGraphQL(operation.document, operation.operationName).collect { event ->
                    when (event) {
                        GraphQLSubscriptionEvent.Ready -> onReady()
                        is GraphQLSubscriptionEvent.Payload -> runCatching { dbJson.decodeFromJsonElement(operation.dataSerializer, event.value) }
                            .onSuccess { onNext(Response(it)) }
                            .onFailure { onError(SubscriptionFailure(SubscriptionFailureKind.Internal, it.toString())) }
                        is GraphQLSubscriptionEvent.Errors -> onError(SubscriptionFailure(SubscriptionFailureKind.Graphql, event.messages.joinToString("\n")))
                        GraphQLSubscriptionEvent.Completed -> Unit
                    }
                }
                onError(SubscriptionFailure(SubscriptionFailureKind.Transport, "subscription completed"))
            } catch (error: CancellationException) {
                throw error
            } catch (error: Exception) {
                val kind = if ((error as? CableClientException)?.reason == CableClientException.Reason.Rejected) SubscriptionFailureKind.Graphql else SubscriptionFailureKind.Transport
                onError(SubscriptionFailure(kind, error.toString()))
            }
        }
        return SubscriptionHandle { job.cancel() }
    }
}

/** Serves the database core over the signed HTTP pipeline and the Cable socket. */
class DatabaseTransport(
    private val run: suspend () -> OperationRun,
    private val cable: suspend () -> CableClient?,
    private val client: OkHttpClient = OkHttpClient(),
    private val onEvent: (operationName: String, json: String) -> Unit = { _, _ -> },
) : Transport {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val jobs = ConcurrentHashMap<String, Job>()

    override suspend fun query(request: TransportRequest): String = send(request, GraphQLOperationKind.Query)

    override suspend fun mutate(request: TransportRequest): String = send(request, GraphQLOperationKind.Mutation)

    override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink): String {
        val socket = cable() ?: throw DbException.Network("the subscription socket is not open")
        val token = UUID.randomUUID().toString()
        jobs[token] = scope.launch {
            try {
                socket.subscribeGraphQL(request.document, request.operationName).collect { event ->
                    when (event) {
                        GraphQLSubscriptionEvent.Ready -> sink.ready()
                        is GraphQLSubscriptionEvent.Payload -> {
                            onEvent(request.operationName, event.value.toString())
                            sink.event(JsonObject(mapOf("data" to event.value)).toString())
                        }
                        is GraphQLSubscriptionEvent.Errors -> sink.failed("graphql", event.messages.joinToString("\n"))
                        GraphQLSubscriptionEvent.Completed -> Unit
                    }
                }
                sink.failed("transport", "subscription completed")
            } catch (error: CancellationException) {
                throw error
            } catch (error: Exception) {
                val kind = if ((error as? CableClientException)?.reason == CableClientException.Reason.Rejected) "graphql" else "transport"
                sink.failed(kind, error.toString())
            }
        }
        return token
    }

    override fun unsubscribe(token: String) {
        jobs.remove(token)?.cancel()
    }

    override suspend fun put(url: String, headers: Map<String, String>, bytes: ByteArray, progress: ByteProgress): PutReply =
        putBytes(client, url, headers, bytes, progress)

    private suspend fun send(request: TransportRequest, kind: GraphQLOperationKind): String = try {
        String(run().send(request.operationName, request.document, parseJson(request.variablesJson), kind))
    } catch (error: CancellationException) {
        throw error
    } catch (error: Exception) {
        throw classifyTransportFailure(error, kind)
    }
}

/** A plain GraphQL endpoint over HTTP with `graphql-transport-ws` subscriptions. */
class OkHttpTransport(
    private val endpoint: String,
    private val headers: () -> Map<String, String> = { emptyMap() },
    private val client: OkHttpClient = OkHttpClient(),
) : Transport {
    private val sockets = ConcurrentHashMap<String, okhttp3.WebSocket>()

    private fun body(request: TransportRequest): String = kotlinx.serialization.json.buildJsonObject {
        put("query", kotlinx.serialization.json.JsonPrimitive(request.document))
        put("operationName", kotlinx.serialization.json.JsonPrimitive(request.operationName))
        put("variables", parseJson(request.variablesJson))
    }.toString()

    private suspend fun post(request: TransportRequest, kind: GraphQLOperationKind): String {
        val http = Request.Builder().url(endpoint).post(body(request).toRequestBody("application/json".toMediaType())).apply {
            headers().forEach { (field, value) -> header(field, value) }
        }.build()
        return try {
            client.newCall(http).exchange { response ->
                val text = response.body.string()
                if (response.code !in 200..299) throw DbException.Http(response.code.toUShort(), text)
                text
            }
        } catch (error: java.io.IOException) {
            throw classifyTransportFailure(error, kind)
        }
    }

    override suspend fun query(request: TransportRequest): String = post(request, GraphQLOperationKind.Query)

    override suspend fun mutate(request: TransportRequest): String = post(request, GraphQLOperationKind.Mutation)

    override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink): String {
        val token = UUID.randomUUID().toString()
        val url = endpoint.replaceFirst("https", "wss").replaceFirst("http", "ws")
        val payload = body(request)
        val socket = client.newWebSocket(
            Request.Builder().url(url).header("Sec-WebSocket-Protocol", "graphql-transport-ws").build(),
            object : okhttp3.WebSocketListener() {
                override fun onOpen(webSocket: okhttp3.WebSocket, response: okhttp3.Response) {
                    webSocket.send(kotlinx.serialization.json.buildJsonObject {
                        put("type", kotlinx.serialization.json.JsonPrimitive("connection_init"))
                        put("payload", JsonObject(headers().mapValues { kotlinx.serialization.json.JsonPrimitive(it.value) }))
                    }.toString())
                }

                override fun onMessage(webSocket: okhttp3.WebSocket, text: String) {
                    val message = parseJson(text) as? JsonObject ?: return
                    when ((message["type"] as? kotlinx.serialization.json.JsonPrimitive)?.content) {
                        "connection_ack" -> {
                            webSocket.send(kotlinx.serialization.json.buildJsonObject {
                                put("id", kotlinx.serialization.json.JsonPrimitive(token))
                                put("type", kotlinx.serialization.json.JsonPrimitive("subscribe"))
                                put("payload", parseJson(payload))
                            }.toString())
                            sink.ready()
                        }
                        "next" -> message["payload"]?.let { sink.event(it.toString()) }
                        "error" -> sink.failed("graphql", message["payload"]?.toString().orEmpty())
                        "complete" -> { sink.closed(); webSocket.close(1000, null) }
                        "ping" -> webSocket.send("""{"type":"pong"}""")
                    }
                }

                override fun onFailure(webSocket: okhttp3.WebSocket, t: Throwable, response: okhttp3.Response?) {
                    sink.failed("transport", t.message ?: t.toString())
                    sink.closed()
                }
            },
        )
        sockets[token] = socket
        return token
    }

    override fun unsubscribe(token: String) {
        sockets.remove(token)?.close(1000, null)
    }

    override suspend fun put(url: String, headers: Map<String, String>, bytes: ByteArray, progress: ByteProgress): PutReply =
        putBytes(client, url, headers, bytes, progress)
}

internal suspend fun putBytes(client: OkHttpClient, url: String, headers: Map<String, String>, bytes: ByteArray, progress: ByteProgress): PutReply {
    val body = object : okhttp3.RequestBody() {
        override fun contentType() = null
        override fun contentLength() = bytes.size.toLong()
        override fun writeTo(sink: okio.BufferedSink) {
            var sent = 0
            while (sent < bytes.size) {
                if (progress.isCancelled()) throw java.io.IOException("cancelled")
                val chunk = minOf(64 * 1024, bytes.size - sent)
                sink.write(bytes, sent, chunk)
                sent += chunk
                progress.sent(sent.toULong())
            }
        }
    }
    val request = Request.Builder().url(url).put(body).apply { headers.forEach { (field, value) -> header(field, value) } }.build()
    return try {
        client.newCall(request).exchange { PutReply(it.code.toUShort(), it.header("ETag")) }
    } catch (error: java.io.IOException) {
        if (progress.isCancelled()) throw DbException.Interrupted(error.message ?: "cancelled")
        throw DbException.Network(error.message ?: error.toString())
    }
}

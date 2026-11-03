package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

data class BlobUpload(val name: String, val create: String) { companion object }
typealias BlobSnapshot = WireBlobSnapshot

/** The status of one media upload. It reads the status stream of its account until it is closed, and closing it ends the stream in the core. */
@Serializable(with = BlobSerializer::class)
class Blob internal constructor(private val database: Database, initial: BlobSnapshot, private val stream: ValueStream) : AutoCloseable {
    val id: String = initial.id
    private val shown = MutableStateFlow(initial)
    val snapshot: StateFlow<BlobSnapshot> get() = shown
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val reading: Job = scope.launch(start = CoroutineStart.ATOMIC) {
        try {
            stream.uploadStatus.collect { shown.value = it }
        } catch (error: kotlinx.coroutines.CancellationException) { throw error }
        catch (error: Exception) { shown.value = shown.value.copy(state = BlobState.FAILED, error = error.message, signedId = null) }
    }.also { it.invokeOnCompletion { stream.close() } }
    suspend fun waitUntilUploaded(): String = offCaller { database.waitBlob(id) }
    suspend fun retry() = offCaller { database.retryBlob(id) }
    suspend fun discard() = offCaller { database.discardBlob(id) }
    override fun close() { scope.cancel(); stream.close() }
}

object BlobSerializer : KSerializer<Blob> {
    override val descriptor = PrimitiveSerialDescriptor("Blob", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: Blob) = encoder.encodeString(value.id)
    override fun deserialize(decoder: Decoder): Blob = throw SerializationException("Restore Blob with DBlayer.blob(id)")
}

suspend fun DBlayer.upload(upload: BlobUpload, request: UploadRequest): Blob = offCaller {
    val database = DBlayer.database
    val snapshot = database.uploadBlob(upload.name, upload.create, request.wire())
    Blob(database, snapshot, database.openValueStream(database.sessionEpoch(), StreamRequest.Blob(snapshot.id)))
}

suspend fun DBlayer.blob(id: String): Blob = offCaller {
    val database = DBlayer.database
    Blob(database, database.blob(id), database.openValueStream(database.sessionEpoch(), StreamRequest.Blob(id)))
}

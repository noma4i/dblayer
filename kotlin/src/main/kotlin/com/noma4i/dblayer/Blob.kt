package com.noma4i.dblayer

import java.lang.ref.Cleaner
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

data class BlobUpload(val name: String, val create: String) { companion object }
typealias BlobSnapshot = WireBlobSnapshot

private val blobCleaner = Cleaner.create()

@Serializable(with = BlobSerializer::class)
class Blob internal constructor(private val database: Database, initial: BlobSnapshot, changes: BlobChanges) : AutoCloseable {
    val id: String = initial.id
    private val shown = MutableStateFlow(initial)
    val snapshot: StateFlow<BlobSnapshot> get() = shown
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val cleanup = blobCleaner.register(this, ReleaseBlob(scope, changes))
    init {
        val state = shown
        scope.launch {
            try {
                var next = changes.next()
                while (next != null) { state.value = next; next = changes.next() }
            } catch (error: kotlinx.coroutines.CancellationException) { throw error }
            catch (error: Exception) { state.value = state.value.copy(state = BlobState.FAILED, error = error.message, signedId = null) }
            finally { changes.cancel() }
        }
    }
    suspend fun waitUntilUploaded(): String = offCaller { database.waitBlob(id) }
    suspend fun retry() = offCaller { database.retryBlob(id) }
    suspend fun discard() = offCaller { database.discardBlob(id) }
    override fun close() = cleanup.clean()
}

private class ReleaseBlob(private val scope: CoroutineScope, private val changes: BlobChanges) : Runnable {
    override fun run() { changes.cancel(); scope.cancel() }
}

object BlobSerializer : KSerializer<Blob> {
    override val descriptor = PrimitiveSerialDescriptor("Blob", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: Blob) = encoder.encodeString(value.id)
    override fun deserialize(decoder: Decoder): Blob = throw SerializationException("Restore Blob with DBlayer.blob(id)")
}

suspend fun DBlayer.upload(upload: BlobUpload, request: UploadRequest): Blob = offCaller {
    val database = DBlayer.database
    val snapshot = database.uploadBlob(upload.name, upload.create, request.wire())
    Blob(database, snapshot, database.blobChanges(snapshot.id))
}

suspend fun DBlayer.blob(id: String): Blob = offCaller {
    val database = DBlayer.database
    Blob(database, database.blob(id), database.blobChanges(id))
}

package com.noma4i.dblayer

/** A refusal as a row reads it, the other fields are empty. */
fun operationFailure(kind: String, reason: String? = null, message: String = ""): OperationFailure =
    OperationFailure(kind, null, null, null, message, null, null, reason)

/** The sync value of a row: the state and the refusal of its last operation, if any. */
fun syncInfo(state: SyncState, error: OperationFailure? = null): SyncInfo = SyncInfo(state, emptyList(), emptyList(), error, 0, 0L)

/** The value of `uploads[action]`: the status, the sent share from 0 to 1 and the refusal. */
fun uploadInfo(status: UploadStatus, progress: Double = 0.0, error: OperationFailure? = null): UploadInfo =
    UploadInfo(status, "", 0UL, 0UL, progress, progress, null, 0, error, null, null)

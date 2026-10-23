package com.noma4i.dblayer

class RecordingLease : CableLease(NoHandle) {
    @Volatile var token: String? = null
    override fun opened(token: String) { this.token = token }
}

suspend fun Transport.openedCable(): String {
    val lease = RecordingLease()
    openCable(lease)
    return requireNotNull(lease.token) { "the transport finished openCable without opened" }
}

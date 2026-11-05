package com.noma4i.dblayer

suspend fun openDatabase(config: Config, modelsJson: String, transport: Transport? = null, encrypted: EncryptedConfig? = null, credentials: CredentialStore? = null, http: EncryptedHttp? = null, operations: String = "[]", uploads: UploadOperations? = null): Database =
    OpenDatabase().open(config, modelsJson, OpenOptions(operations, uploads?.wire(), false, false), transport, encrypted, credentials, http, null)

suspend fun adopt(database: Database): Database {
    DBlayer.close()
    DBlayer.database = database
    return database
}

suspend fun reopen(config: Config, modelsJson: String, transport: Transport? = null, operations: String = "[]", uploads: UploadOperations? = null): Database = adopt(openDatabase(config, modelsJson, transport, operations = operations, uploads = uploads))

/** A database whose plain transport holds a session and sees no network: a sent row stays queued. A send needs a session (`specs/architecture.md` D). */
suspend fun reopenSigned(config: Config, modelsJson: String, operations: String = "[]", uploads: UploadOperations? = null): Database {
    val database = reopen(config, modelsJson, MockTransport(), operations, uploads)
    database.setOnline(false)
    return database
}

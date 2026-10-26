package com.noma4i.dblayer

suspend fun openDatabase(config: Config, modelsJson: String, transport: Transport? = null, encrypted: EncryptedConfig? = null, credentials: CredentialStore? = null, http: EncryptedHttp? = null, operations: String = "[]", uploads: UploadOperations? = null, listening: Boolean = true): Database =
    OpenDatabase().open(config, modelsJson, OpenOptions(operations, uploads?.wire(), false, false, true, listening), transport, encrypted, credentials, http, null)

fun adopt(database: Database): Database {
    DBlayer.close()
    DBlayer.database = database
    return database
}

suspend fun reopen(config: Config, modelsJson: String, transport: Transport? = null, operations: String = "[]", uploads: UploadOperations? = null, listening: Boolean = true): Database = adopt(openDatabase(config, modelsJson, transport, operations = operations, uploads = uploads, listening = listening))

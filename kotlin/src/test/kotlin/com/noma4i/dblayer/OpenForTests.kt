package com.noma4i.dblayer

suspend fun openDatabase(config: Config, modelsJson: String, transport: Transport? = null, encrypted: EncryptedConfig? = null, credentials: CredentialStore? = null, http: EncryptedHttp? = null, operations: String = "[]", uploads: UploadOperations? = null): Database =
    OpenDatabase().open(config, modelsJson, OpenOptions(operations, uploads?.wire()), transport, encrypted, credentials, http, null)

fun adopt(database: Database): Database {
    DBlayer.close()
    DBlayer.database = database
    return database
}

suspend fun reopen(config: Config, modelsJson: String, transport: Transport? = null, operations: String = "[]", uploads: UploadOperations? = null): Database = adopt(openDatabase(config, modelsJson, transport, operations = operations, uploads = uploads))

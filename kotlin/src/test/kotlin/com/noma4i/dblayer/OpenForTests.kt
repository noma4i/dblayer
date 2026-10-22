package com.noma4i.dblayer

suspend fun openDatabase(config: Config, modelsJson: String, transport: Transport? = null, encrypted: EncryptedConfig? = null, credentials: CredentialStore? = null, http: EncryptedHttp? = null): Database =
    OpenDatabase().open(config, modelsJson, OpenOptions("[]"), transport, encrypted, credentials, http)

fun adopt(database: Database): Database {
    DBlayer.close()
    DBlayer.database = database
    return database
}

suspend fun reopen(config: Config, modelsJson: String, transport: Transport? = null): Database = adopt(openDatabase(config, modelsJson, transport))

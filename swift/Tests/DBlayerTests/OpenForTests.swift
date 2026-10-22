import Foundation
@testable import DBlayer

func openDatabase(config: Config, modelsJson: String, transport: Transport?, encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, http: EncryptedHttp? = nil) async throws -> Database {
  try await OpenDatabase().open(config: config, modelsJson: modelsJson, options: OpenOptions(operations: "[]"), transport: transport, encrypted: encrypted, credentials: credentials, http: http)
}

@MainActor @discardableResult
func adopt(_ database: Database) -> Database {
  DB.close()
  DB.current = database
  return database
}

@MainActor @discardableResult
func reopen(config: Config, modelsJson: String, transport: Transport?) async throws -> Database {
  adopt(try await openDatabase(config: config, modelsJson: modelsJson, transport: transport))
}

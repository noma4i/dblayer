import DBlayerTesting
import Foundation
@testable import DBlayer

func openDatabase(config: Config, modelsJson: String, transport: Transport?, encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, http: EncryptedHttp? = nil, operations: String = "[]", uploads: UploadOperations? = nil, listening: Bool = true) async throws -> Database {
  try await OpenDatabase().open(config: config, modelsJson: modelsJson, options: OpenOptions(operations: operations, uploads: uploads?.wire, powerSaving: false, lowRam: false, preload: true, listening: listening), transport: transport, encrypted: encrypted, credentials: credentials, http: http, retries: nil)
}

@MainActor @discardableResult
func adopt(_ database: Database) async throws -> Database {
  try await DB.close()
  DB.current = database
  return database
}

@MainActor @discardableResult
func reopen(config: Config, modelsJson: String, transport: Transport?, operations: String = "[]", uploads: UploadOperations? = nil, listening: Bool = true) async throws -> Database {
  try await adopt(try await openDatabase(config: config, modelsJson: modelsJson, transport: transport, operations: operations, uploads: uploads, listening: listening))
}

/// A database whose plain transport holds a session and sees no network: a sent row stays queued. A send needs a session (`specs/architecture.md` D).
@MainActor @discardableResult
func reopenSigned(config: Config, modelsJson: String, operations: String = "[]", uploads: UploadOperations? = nil) async throws -> Database {
  let database = try await reopen(config: config, modelsJson: modelsJson, transport: MockTransport(), operations: operations, uploads: uploads)
  try database.setOnline(online: false)
  return database
}

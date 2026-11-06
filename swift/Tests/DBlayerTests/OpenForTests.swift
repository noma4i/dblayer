import DBlayerTesting
import Foundation
@testable import DBlayer

func openDatabase(config: Config, modelsJson: String, transport: Transport?, encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, http: EncryptedHttp? = nil, operations: String = "[]", uploads: UploadOperations? = nil) async throws -> Database {
  try await OpenDatabase().open(config: config, modelsJson: modelsJson, options: OpenOptions(operations: operations, uploads: uploads?.wire, powerSaving: false, lowRam: false), transport: transport, encrypted: encrypted, credentials: credentials, http: http, retries: nil)
}

@MainActor @discardableResult
func adopt(_ database: Database) async throws -> Database {
  await DB.closeForCleanup()
  DB.use(database)
  return database
}

/// A test transport that forwards to a `MockTransport`: the session cable of the stand goes to that mock.
protocol WrapsMock { var base: MockTransport { get } }

private final class HeldCredential: CredentialStore, @unchecked Sendable {
  private let lock = NSLock()
  private var value: String?
  func get() throws -> String? { lock.withLock { value } }
  func set(credential: String) throws { lock.withLock { value = credential } }
  func clear() throws { lock.withLock { value = nil } }
}

private struct StandLogin: Encodable, Sendable { let account: String }

let sendMoment = #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid text likedByMe media createdAt } }"}]"#

private let sessionProfile = #"{"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"}"#

/// The HTTP of the encrypted session answers from the mock transport, so the test database signs in the real way and every request still reaches the mock.
private func bridgeToTransport(_ transport: Transport) -> @Sendable (String, [String: Any]) async throws -> [String: Any] {
  { path, body in
    if path == "/api/auth/login_link" { return ["sessionId": "stand"] }
    if path == "/api/logout" { return ["revoked": true] }
    let operation = body["operationName"] as? String ?? ""
    if operation == "SessionProfile" { return ["data": ["sessionProfile": ["id": "stand"]]] }
    let variables = (body["variables"] as? [String: Any]).flatMap { try? JSONSerialization.data(withJSONObject: $0, options: [.sortedKeys]) }.flatMap { String(data: $0, encoding: .utf8) } ?? "{}"
    let text = try await transport.query(request: TransportRequest(document: body["query"] as? String ?? "", operationName: operation, variablesJson: variables, purpose: .standard))
    return try JSONSerialization.jsonObject(with: Data(text.utf8)) as? [String: Any] ?? [:]
  }
}

private func standEncrypted() -> EncryptedConfig {
  EncryptedConfig(origin: SealedServer.origin, serverKey: SealedServer.serverKey, pin: SealedServer.pin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["login_link": EncryptedSessionOperation(path: "/api/auth/login_link", class: .authenticated)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel")
}

private func withSessionProfile(_ operations: String) -> String {
  operations.trimmingCharacters(in: .whitespaces) == "[]" ? "[\(sessionProfile)]" : String(operations.dropLast()) + ",\(sessionProfile)]"
}

private func signIn() async throws {
  try DB.database.setOnline(online: true)
  try await DB.sessionCall("login_link", body: StandLogin(account: "stand"))
  try await DB.run("SessionProfile", variables: [:])
}

/// A transport signs in through the real session path, and its queries and mutations ride the session HTTP.
@MainActor @discardableResult
func reopen(config: Config, modelsJson: String, transport: Transport?, operations: String = "[]", uploads: UploadOperations? = nil) async throws -> Database {
  guard let mock = transport else {
    return try await adopt(try await openDatabase(config: config, modelsJson: modelsJson, transport: transport, operations: operations, uploads: uploads))
  }
  let server = try SealedServer(bridge: bridgeToTransport(mock))
  (mock as? MockTransport ?? (mock as? any WrapsMock)?.base)?.cable = EncryptedCablePeer(server: server)
  let database = try await adopt(try await openDatabase(config: config, modelsJson: modelsJson, transport: mock, encrypted: standEncrypted(), credentials: HeldCredential(), http: server, operations: withSessionProfile(operations), uploads: uploads))
  try await signIn()
  return database
}

/// The same session stand on a clock that only `advance(by:)` moves.
@MainActor @discardableResult
func reopenClocked(modelsJson: String, transport: MockTransport, operations: String = "[]", path: String? = nil) async throws -> TestClock {
  let server = try SealedServer(bridge: bridgeToTransport(transport))
  transport.cable = EncryptedCablePeer(server: server)
  await DB.closeForCleanup()
  let clock = try await DB.openTesting(transport: transport, modelsJSON: modelsJson, path: path, encrypted: standEncrypted(), credentials: HeldCredential(), http: server)
  try DB.database.registerOperations(defsJson: withSessionProfile(operations))
  try await signIn()
  return clock
}

/// A database whose plain transport holds a session and sees no network: a sent row stays queued. A send needs a session.
@MainActor @discardableResult
func reopenSigned(config: Config, modelsJson: String, operations: String = sendMoment, uploads: UploadOperations? = nil) async throws -> Database {
  let database = try await reopen(config: config, modelsJson: modelsJson, transport: MockTransport(), operations: operations, uploads: uploads)
  try database.setOnline(online: false)
  return database
}

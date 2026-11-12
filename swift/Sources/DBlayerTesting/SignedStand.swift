@_spi(Testing) import DBlayer
import Foundation
import Testing

private let standServerLock = NSLock()
nonisolated(unsafe) private var standServer: SealedServer?

public func registerStandOperations(_ database: Database, defsJson: String) async throws {
  let server = standServerLock.withLock { standServer }
  try server?.registerDocuments(defsJson)
  try await database.registerOperations(defsJson: defsJson)
}

/// The one database of the process: a test that opens the next one closes the previous.
func closePrevious() async {
  standServerLock.withLock { standServer = nil }
  do { try await DB.opener.opened().closeDatabase() } catch DbError.NotOpen(_) {} catch { fatalError("closing the previous database failed: \(error)") }
}

public func openDatabase(config: Config, modelsJson: String, transport: Transport?, encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, http: EncryptedHttp? = nil, operations: String = "[]", preparer: Preparer? = ScriptedPreparer()) async throws -> Database {
  await closePrevious()
  return try await DB.opener.open(config: config, modelsJson: withRawCalls(modelsJson), options: OpenOptions(operations: definitions(of: withUploadSteps(operations, models: modelsJson))), ports: OpenPorts(transport: transport, encrypted: encrypted, credentials: credentials, http: http, endpoint: transport == nil ? nil : scriptedEndpoint, preparer: preparer, partialOperations: true), observers: nil)
}

public func withRawCalls(_ source: String, _ names: [String] = ["SessionProfile", "PurchaseCreate", "RecordAlbumSend"]) -> String {
  var object = try! JSONSerialization.jsonObject(with: Data(source.utf8)) as! [String: Any]
  var operations = object["operations"] as? [[String: Any]] ?? []
  for name in names where !operations.contains(where: { $0["name"] as? String == name }) {
    operations.append(["name": name, "returns": ["name": name, "kind": ["type": "raw"]]])
  }
  object["operations"] = operations
  return String(decoding: try! JSONSerialization.data(withJSONObject: object), as: UTF8.self)
}

private func definitions(of operations: String) -> [OperationDefinition] {
  let parsed: Any
  do { parsed = try JSONSerialization.jsonObject(with: Data(operations.utf8)) } catch { preconditionFailure("the operations of the stand are not JSON: \(error)") }
  let listed = parsed as? [[String: Any]] ?? []
  return listed.compactMap { entry in
    guard let name = entry["name"] as? String, let document = entry["document"] as? String else { return nil }
    return OperationDefinition(name: name, document: document)
  }
}

private let uploadStepOperations: [(name: String, document: String)] = [
  ("CreateBlob", "mutation CreateBlob { createBlob { url headers signedId blobId } }"),
  ("InitMultipart", "mutation InitMultipart { initMultipart { key uploadId } }"),
  ("PresignParts", "mutation PresignParts { presignParts { parts { partNumber url } } }"),
  ("CompleteMultipart", "mutation CompleteMultipart { completeMultipart { signedId blobId } }"),
  ("AbortMultipart", "mutation AbortMultipart { abortMultipart { ok } }"),
  ("CreateAsset", "mutation CreateAsset { createAsset { asset { id clientKey size position } } }"),
]

/// The models declare the upload steps once, so every open registers the operations of the steps.
public func withUploadSteps(_ operations: String, models: String) -> String {
  guard models.contains("upload_steps"), operations.hasSuffix("]") else { return operations }
  let missing = uploadStepOperations.filter { !operations.contains("\"name\":\"\($0.name)\"") }
  guard !missing.isEmpty else { return operations }
  let added = missing.map { #"{"name":"\#($0.name)","document":"\#($0.document)"}"# }.joined(separator: ",")
  return String(operations.dropLast()) + (operations == "[]" ? "" : ",") + added + "]"
}

@MainActor @discardableResult
public func adopt(_ database: Database) async throws -> Database {
  await ChangePump.start(database)
  return database
}

/// A test transport that forwards to a `MockTransport`: the session cable of the stand goes to that mock.
public protocol WrapsMock { var base: MockTransport { get } }

private final class HeldCredential: CredentialStore, @unchecked Sendable {
  let contract = ServerContract()
  private let lock = NSLock()
  private var value: String?
  func get() throws -> String? { lock.withLock { value } }
  func set(credential: String) throws { lock.withLock { value = credential } }
  func clear() throws { lock.withLock { value = nil } }
}

private let heldCredentials = NSLock()
nonisolated(unsafe) private var credentialsByPath: [String: HeldCredential] = [:]

/// The secret outlives a process like the Keychain does: a database file opened again finds the credential of its stored session.
private func credential(of config: Config) -> HeldCredential {
  guard let path = config.path else { return HeldCredential() }
  return heldCredentials.withLock { credentialsByPath[path] ?? { let made = HeldCredential(); credentialsByPath[path] = made; return made }() }
}

private struct StandLogin: Encodable, Sendable { let account: String }

public let sendMoment = #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid text likedByMe media createdAt } }"}]"#

private let sessionProfile = #"{"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"}"#

/// The HTTP of the encrypted session answers from the mock transport, so the test database signs in the real way and every request still reaches the mock.
public func bridgeToTransport(_ transport: any ScriptedTransport) -> @Sendable (String, [String: Any]) async throws -> [String: Any] {
  return { path, body in
    if path == "/api/auth/login_link" { return ["sessionId": "stand"] }
    if path == "/api/logout" { return ["revoked": true] }
    let operation = body["operationName"] as? String ?? ""
    if operation == "SessionProfile" { return ["data": ["sessionProfile": ["id": "stand"]]] }
    let variables = try (body["variables"] as? [String: Any]).map { String(decoding: try JSONSerialization.data(withJSONObject: $0, options: [.sortedKeys]), as: UTF8.self) } ?? "{}"
    let document = body["query"] as? String ?? ""
    let request = TransportRequest(document: document, operationName: operation, variablesJson: variables, purpose: SealedServer.purpose, author: ScriptedWire.ids.author, requestId: ScriptedWire.ids.requestId, attempt: ScriptedWire.ids.attempt)
    let text: String
    if document.drop(while: \.isWhitespace).hasPrefix("mutation") { text = try await transport.mutate(request: request) } else { text = try await transport.query(request: request) }
    return try JSONSerialization.jsonObject(with: Data(text.utf8)) as? [String: Any] ?? [:]
  }
}

private func standEncrypted() -> EncryptedConfig {
  EncryptedConfig(origin: SealedServer.origin, serverKey: SealedServer.serverKey, pin: SealedServer.pin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["login_link": EncryptedSessionOperation(path: "/api/auth/login_link", class: .authenticated)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel")
}

private func withSessionProfile(_ operations: String) -> String {
  operations.trimmingCharacters(in: .whitespaces) == "[]" ? "[\(sessionProfile)]" : String(operations.dropLast()) + ",\(sessionProfile)]"
}

/// The platform reports online the moment the database opens, before the operations are declared.
public final class OnlineAtOpen: PlatformObserver {
  public init() {}
  public func start(sink: OnlineSink) { sink.setOnline(online: true) }
  public func stop() {}
}

/// The platform reports nothing at open: the test sets online itself after the operations are declared.
public final class SilentPlatform: PlatformObserver {
  public init() {}
  public func start(sink: OnlineSink) {}
  public func stop() {}
}

private func signIn(restored: Bool = false, setsOnline: Bool = true) async throws {
  if setsOnline { try DB.opener.opened().setOnline(online: true) }
  if !restored { try await DB.sessionCall("login_link", body: StandLogin(account: "stand")) }
  try await DB.call("SessionProfile", variables: [:])
}

/// After a session reset the account signs in again through the same stand.
@MainActor
public func loginAgain() async throws {
  try await signIn()
}

/// A transport signs in through the real session path, and its queries and mutations ride the session HTTP. By default the operations are declared at open and online follows in the sign-in. With `onlineAtOpen` the platform reports online at open and the operations are declared after it.
@MainActor @discardableResult
public func openSigned(config: Config, modelsJson: String, transport: Transport?, operations: String = "[]", onlineAtOpen: Bool = false, preparer: Preparer? = ScriptedPreparer()) async throws -> Database {
  guard let mock = transport as? any ScriptedTransport else {
    return try await adopt(try await openDatabase(config: config, modelsJson: modelsJson, transport: transport, operations: operations, preparer: preparer))
  }
  let held = credential(of: config)
  let server = try SealedServer(contract: held.contract, bridge: bridgeToTransport(mock))
  try server.registerDocuments(withSessionProfile(withUploadSteps(operations, models: modelsJson)))
  (mock as? MockTransport ?? (mock as? any WrapsMock)?.base)?.cable = EncryptedCablePeer(server: server)
  let restored = try held.get() != nil
  let database: Database
  if onlineAtOpen {
    let ports = OpenPorts(transport: mock, encrypted: standEncrypted(), credentials: held, http: server, endpoint: scriptedEndpoint, preparer: preparer, partialOperations: true)
    let declared = definitions(of: withUploadSteps(operations, models: modelsJson))
    let steps = Set(uploadStepOperations.map(\.name))
    await closePrevious()
    database = try await adopt(try await DB.opener.open(config: config, modelsJson: withRawCalls(modelsJson), options: OpenOptions(operations: declared.filter { steps.contains($0.name) }), ports: ports, observers: OnlineAtOpen()))
    let later = declared.filter { !steps.contains($0.name) }.map { ["name": $0.name, "document": $0.document] }
    try await database.registerOperations(defsJson: withSessionProfile(String(decoding: try JSONSerialization.data(withJSONObject: later), as: UTF8.self)))
  } else {
    database = try await adopt(try await openDatabase(config: config, modelsJson: modelsJson, transport: mock, encrypted: standEncrypted(), credentials: held, http: server, operations: withSessionProfile(operations), preparer: preparer))
  }
  standServerLock.withLock { standServer = server }
  try await signIn(restored: restored, setsOnline: !onlineAtOpen)
  return database
}

/// The same session stand on a clock that only `advance(by:)` moves. The platform reports online at open unless `onlineAtOpen` is false: then the sign-in sets online after the operations.
@MainActor @discardableResult
public func openSignedClocked(modelsJson: String, transport: MockTransport, operations: String = "[]", path: String? = nil, startingAt start: Date = Date(), observers: (any PlatformObserver)? = nil, onlineAtOpen: Bool = true, preparer: Preparer? = ScriptedPreparer()) async throws -> TestClock {
  let held = path.map { credential(of: Config(path: $0, dataVersion: nil)) } ?? HeldCredential()
  let server = try SealedServer(contract: held.contract, bridge: bridgeToTransport(transport))
  try server.registerDocuments(withSessionProfile(withUploadSteps(operations, models: modelsJson)))
  transport.cable = EncryptedCablePeer(server: server)
  await DB.closeForCleanup()
  let restored = try held.get() != nil
  let clock = try await DB.openTesting(transport: transport, modelsJSON: withRawCalls(modelsJson), path: path, startingAt: start, encrypted: standEncrypted(), credentials: held, http: server, observers: observers ?? (onlineAtOpen ? nil : SilentPlatform()), preparer: preparer)
  standServerLock.withLock { standServer = server }
  try await DB.opener.opened().registerOperations(defsJson: withSessionProfile(operations))
  try await signIn(restored: restored)
  return clock
}

/// A database whose plain transport holds a session and sees no network: a sent row stays queued. A send needs a session.
@MainActor @discardableResult
public func openSignedOffline(config: Config, modelsJson: String, operations: String = sendMoment, preparer: Preparer? = ScriptedPreparer()) async throws -> Database {
  let database = try await openSigned(config: config, modelsJson: modelsJson, transport: MockTransport(), operations: operations, preparer: preparer)
  try database.setOnline(online: false)
  return database
}

extension DB {
  /// Test cleanup: a database a test already closed is closed.
  @MainActor public static func closeForCleanup() async {
    do { try await close() } catch DbError.Closed(_) {} catch DbError.NotOpen(_) {} catch { Issue.record("close failed: \(error)") }
  }
}

import DBlayerTesting
import Foundation
@_spi(Testing) @testable import DBlayer

func openDatabase(config: Config, modelsJson: String, transport: Transport?, encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, http: EncryptedHttp? = nil, operations: String = "[]") async throws -> Database {
  try await OpenDatabase().open(config: config, modelsJson: withRawCalls(modelsJson), options: OpenOptions(operations: definitions(of: withUploadSteps(operations, models: modelsJson))), ports: OpenPorts(transport: transport, encrypted: encrypted, credentials: credentials, http: http, endpoint: transport == nil ? nil : scriptedEndpoint), observers: nil)
}

let modelsArrayJSON: String = {
  let parsed = try! JSONSerialization.jsonObject(with: Data(modelsJSON.utf8)) as! [String: Any]
  return String(decoding: try! JSONSerialization.data(withJSONObject: parsed["models"]!), as: UTF8.self)
}()

let emptySpecs = #"{"models":[]}"#

func specs(_ models: String) -> String { #"{"models":\#(models)}"# }

func withRawCalls(_ source: String, _ names: [String] = ["SessionProfile", "PurchaseCreate", "RecordAlbumSend"]) -> String {
  var object = try! JSONSerialization.jsonObject(with: Data(source.utf8)) as! [String: Any]
  var operations = object["operations"] as? [[String: Any]] ?? []
  for name in names where !operations.contains(where: { $0["name"] as? String == name }) {
    operations.append(["name": name, "returns": ["name": name, "kind": ["type": "raw"]]])
  }
  object["operations"] = operations
  return String(decoding: try! JSONSerialization.data(withJSONObject: object), as: UTF8.self)
}

private func definitions(of operations: String) -> [OperationDefinition] {
  let listed = (try? JSONSerialization.jsonObject(with: Data(operations.utf8))) as? [[String: Any]] ?? []
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
func withUploadSteps(_ operations: String, models: String) -> String {
  guard models.contains("upload_steps"), operations.hasSuffix("]") else { return operations }
  let missing = uploadStepOperations.filter { !operations.contains("\"name\":\"\($0.name)\"") }
  guard !missing.isEmpty else { return operations }
  let added = missing.map { #"{"name":"\#($0.name)","document":"\#($0.document)"}"# }.joined(separator: ",")
  return String(operations.dropLast()) + (operations == "[]" ? "" : ",") + added + "]"
}

@MainActor @discardableResult
func adopt(_ database: Database) async throws -> Database {
  await DB.closeForCleanup()
  DB.adopt(database)
  await ChangePump.start(database)
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

private let heldCredentials = NSLock()
nonisolated(unsafe) private var credentialsByPath: [String: HeldCredential] = [:]

/// The secret outlives a process like the Keychain does: a database file opened again finds the credential of its stored session.
private func credential(of config: Config) -> HeldCredential {
  guard let path = config.path else { return HeldCredential() }
  return heldCredentials.withLock { credentialsByPath[path] ?? { let made = HeldCredential(); credentialsByPath[path] = made; return made }() }
}

private struct StandLogin: Encodable, Sendable { let account: String }

let sendMoment = #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid text likedByMe media createdAt } }"}]"#

private let sessionProfile = #"{"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"}"#

/// The HTTP of the encrypted session answers from the mock transport, so the test database signs in the real way and every request still reaches the mock.
private func mutationNames(_ operations: String) throws -> Set<String> {
  let listed = try JSONSerialization.jsonObject(with: Data(operations.utf8)) as? [[String: Any]] ?? []
  return Set(listed.compactMap { ($0["document"] as? String)?.hasPrefix("mutation") == true ? $0["name"] as? String : nil })
}

private func bridgeToTransport(_ transport: any ScriptedTransport, operations: String) throws -> @Sendable (String, [String: Any]) async throws -> [String: Any] {
  let mutations = try mutationNames(operations)
  return { path, body in
    if path == "/api/auth/login_link" { return ["sessionId": "stand"] }
    if path == "/api/logout" { return ["revoked": true] }
    let operation = body["operationName"] as? String ?? ""
    if operation == "SessionProfile" { return ["data": ["sessionProfile": ["id": "stand"]]] }
    let variables = (body["variables"] as? [String: Any]).flatMap { try? JSONSerialization.data(withJSONObject: $0, options: [.sortedKeys]) }.flatMap { String(data: $0, encoding: .utf8) } ?? "{}"
    let document = body["query"] as? String ?? ""
    let request = TransportRequest(document: document, operationName: operation, variablesJson: variables, purpose: SealedServer.purpose, author: ScriptedWire.ids.author, requestId: ScriptedWire.ids.requestId, attempt: ScriptedWire.ids.attempt)
    let text: String
    if mutations.contains(operation) { text = try await transport.mutate(request: request) } else { text = try await transport.query(request: request) }
    return try JSONSerialization.jsonObject(with: Data(text.utf8)) as? [String: Any] ?? [:]
  }
}

private func standEncrypted() -> EncryptedConfig {
  EncryptedConfig(origin: SealedServer.origin, serverKey: SealedServer.serverKey, pin: SealedServer.pin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["login_link": EncryptedSessionOperation(path: "/api/auth/login_link", class: .authenticated)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel")
}

private func withSessionProfile(_ operations: String) -> String {
  operations.trimmingCharacters(in: .whitespaces) == "[]" ? "[\(sessionProfile)]" : String(operations.dropLast()) + ",\(sessionProfile)]"
}

/// The two orders of the open inputs: the platform reports online before the operations are declared, or the test sets it after them.
enum OpenOrder: String, CaseIterable {
  case onlineAtOpen, onlineAfterRegister
  var onlineAtOpen: Bool { self == .onlineAtOpen }
}

/// The platform reports online the moment the database opens, before the operations are declared.
final class OnlineAtOpen: PlatformObserver {
  func start(sink: OnlineSink) { sink.setOnline(online: true) }
  func stop() {}
}

/// The platform reports nothing at open: the test sets online itself after the operations are declared.
final class SilentPlatform: PlatformObserver {
  func start(sink: OnlineSink) {}
  func stop() {}
}

private func signIn(restored: Bool = false, setsOnline: Bool = true) async throws {
  if setsOnline { try DB.database.setOnline(online: true) }
  if !restored { try await DB.sessionCall("login_link", body: StandLogin(account: "stand")) }
  try await DB.call("SessionProfile", variables: [:])
}

/// After a session reset the account signs in again through the same stand.
@MainActor
func loginAgain() async throws {
  try await signIn()
}

/// A transport signs in through the real session path, and its queries and mutations ride the session HTTP. By default the operations are declared at open and online follows in the sign-in. With `onlineAtOpen` the platform reports online at open and the operations are declared after it.
@MainActor @discardableResult
func reopen(config: Config, modelsJson: String, transport: Transport?, operations: String = "[]", onlineAtOpen: Bool = false) async throws -> Database {
  guard let mock = transport as? any ScriptedTransport else {
    return try await adopt(try await openDatabase(config: config, modelsJson: modelsJson, transport: transport, operations: operations))
  }
  let server = try SealedServer(bridge: try bridgeToTransport(mock, operations: withUploadSteps(operations, models: modelsJson)))
  (mock as? MockTransport ?? (mock as? any WrapsMock)?.base)?.cable = EncryptedCablePeer(server: server)
  let held = credential(of: config)
  let restored = try held.get() != nil
  let database: Database
  if onlineAtOpen {
    let ports = OpenPorts(transport: mock, encrypted: standEncrypted(), credentials: held, http: server, endpoint: scriptedEndpoint)
    let declared = definitions(of: withUploadSteps(operations, models: modelsJson))
    let steps = Set(uploadStepOperations.map(\.name))
    database = try await adopt(try await OpenDatabase().open(config: config, modelsJson: withRawCalls(modelsJson), options: OpenOptions(operations: declared.filter { steps.contains($0.name) }), ports: ports, observers: OnlineAtOpen()))
    let later = declared.filter { !steps.contains($0.name) }.map { ["name": $0.name, "document": $0.document] }
    try await database.registerOperations(defsJson: withSessionProfile(String(decoding: try JSONSerialization.data(withJSONObject: later), as: UTF8.self)))
  } else {
    database = try await adopt(try await openDatabase(config: config, modelsJson: modelsJson, transport: mock, encrypted: standEncrypted(), credentials: held, http: server, operations: withSessionProfile(operations)))
  }
  try await signIn(restored: restored, setsOnline: !onlineAtOpen)
  return database
}

/// The same session stand on a clock that only `advance(by:)` moves. The platform reports online at open unless `onlineAtOpen` is false: then the sign-in sets online after the operations.
@MainActor @discardableResult
func reopenClocked(modelsJson: String, transport: MockTransport, operations: String = "[]", path: String? = nil, startingAt start: Date = Date(), observers: (any PlatformObserver)? = nil, onlineAtOpen: Bool = true) async throws -> TestClock {
  let server = try SealedServer(bridge: try bridgeToTransport(transport, operations: withUploadSteps(operations, models: modelsJson)))
  transport.cable = EncryptedCablePeer(server: server)
  await DB.closeForCleanup()
  let held = path.map { credential(of: Config(path: $0, dataVersion: nil)) } ?? HeldCredential()
  let restored = try held.get() != nil
  let clock = try await DB.openTesting(transport: transport, modelsJSON: withRawCalls(modelsJson), path: path, startingAt: start, encrypted: standEncrypted(), credentials: held, http: server, observers: observers ?? (onlineAtOpen ? nil : SilentPlatform()))
  try await DB.database.registerOperations(defsJson: withSessionProfile(operations))
  try await signIn(restored: restored)
  return clock
}

/// A database whose plain transport holds a session and sees no network: a sent row stays queued. A send needs a session.
@MainActor @discardableResult
func reopenSigned(config: Config, modelsJson: String, operations: String = sendMoment) async throws -> Database {
  let database = try await reopen(config: config, modelsJson: modelsJson, transport: MockTransport(), operations: operations)
  try database.setOnline(online: false)
  return database
}

/// The schema with one server subscription: a schema holds one subscription, so a test of a model-level subscription runs on a schema that declares only that one.
func schema(onlySubscription name: String, in source: String = modelsJSON) -> String {
  var models = ((try? JSONSerialization.jsonObject(with: Data(source.utf8))) as? [String: Any])?["models"] as? [[String: Any]] ?? []
  for index in models.indices {
    models[index].removeValue(forKey: "change_feed")
    if let listens = models[index]["listens"] as? [[String: Any]] { models[index]["listens"] = listens.filter { $0["subscription"] as? String == name } }
    if var relations = models[index]["relations"] as? [[String: Any]] {
      for at in relations.indices { if let updates = relations[at]["updates"] as? [[String: Any]] { relations[at]["updates"] = updates.filter { $0["subscription"] as? String == name } } }
      models[index]["relations"] = relations
    }
  }
  return specs(String(decoding: (try? JSONSerialization.data(withJSONObject: models)) ?? Data(), as: UTF8.self))
}

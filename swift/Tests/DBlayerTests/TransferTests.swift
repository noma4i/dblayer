import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

/// The item outcomes of a batch: each item has its own receipt.
func confirmedPairs<Model: DBModel>(_ receipt: OperationReceipt<Model>) async throws -> [(key: String, id: String)?] {
  var pairs: [(key: String, id: String)?] = []
  for item in receipt.items {
    if case let .confirmed(value?, _)? = try? await item.receipt.settled() { pairs.append((item.item.value, value.id)) } else { pairs.append(nil) }
  }
  return pairs
}

private func confirmedIds<Model: DBModel>(_ receipt: OperationReceipt<Model>) async throws -> [String?] {
  try await confirmedPairs(receipt).map { $0?.id }
}


private final class MultipartTransfer: ScriptedTransport, @unchecked Sendable {
  let mock = MockTransport()
  private let lock = NSLock()
  private var urls: [String] = []
  private var captured: [TransferIdentity] = []
  private var capturedPaths: [(String, String)] = []
  var puts: [String] { lock.withLock { urls } }
  var identities: [TransferIdentity] { lock.withLock { captured } }
  var paths: [(String, String)] { lock.withLock { capturedPaths } }
  func scriptedCable(url: String, events: CableEvents) async -> CableOutcome { await mock.scriptedCable(url: url, events: events) }
  func query(request: TransportRequest) async throws -> String { try await mock.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await mock.mutate(request: request) }
  func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    lock.withLock { urls.append(url); captured.append(identity); capturedPaths.append((identity.itemId, URL(fileURLWithPath: file.path).resolvingSymlinksInPath().path)) }
    return PutReply(status: 200, etag: "etag")
  }
}

/// A part gets its own presign: each of the three parts of an 11 MiB file asks once and finds its url in the answer.
private func enqueuePresigns(_ mock: MockTransport) {
  for _ in 0..<3 {
    mock.enqueue("PresignParts", reply: #"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#)
  }
}

private final class TransferCredential: CredentialStore, @unchecked Sendable {
  private let lock = NSLock()
  private var value: String?
  func get() throws -> String? { lock.withLock { value } }
  func set(credential: String) throws { lock.withLock { value = credential } }
  func clear() throws { lock.withLock { value = nil } }
}

private final class HoldSecondFile: ScriptedTransport, @unchecked Sendable {
  private let mock = MockTransport()
  private let lock = NSLock()
  private var count = 0
  private var captured: [TransferIdentity] = []
  var identities: [TransferIdentity] { lock.withLock { captured } }
  let server: SealedServer
  init(_ server: SealedServer) { self.server = server }
  func scriptedCable(url: String, events: CableEvents) async -> CableOutcome { await mock.scriptedCable(url: url, events: events) }
  func query(request: TransportRequest) async throws -> String { try await mock.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await mock.mutate(request: request) }
  func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    let first = lock.withLock { captured.append(identity); count += 1; return count == 1 }
    if first { server.hold(operation: "CreateBlob") }
    progress.sent(bytes: file.length)
    return PutReply(status: 200, etag: nil)
  }
}

@MainActor
private func openStand(_ transport: HoldSecondFile, path: String, encrypted: EncryptedConfig, credential: TransferCredential, server: SealedServer, operations: String, onlineAtOpen: Bool) async throws {
  await DB.closeForCleanup()
  _ = try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, path: path, encrypted: encrypted, credentials: credential, http: server, observers: onlineAtOpen ? nil : SilentPlatform())
  try server.registerDocuments(operations)
  try await registerStandOperations(DB.database, defsJson: operations)
  if !onlineAtOpen { try DB.database.setOnline(online: true) }
}

private func createdBlob(_ index: Int) -> String {
  "{\"data\":{\"blobCreate\":{\"url\":\"https://storage/file\",\"headers\":{},\"signedId\":\"signed-\(index)\",\"blobId\":\"b\(index)\"}}}"
}

private final class RefuseFirstFile: ScriptedTransport, @unchecked Sendable {
  private let mock: MockTransport
  private let lock = NSLock()
  private var refused = false
  init(_ mock: MockTransport) { self.mock = mock }
  func scriptedCable(url: String, events: CableEvents) async -> CableOutcome { await mock.scriptedCable(url: url, events: events) }
  func query(request: TransportRequest) async throws -> String { try await mock.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String {
    let answer = try await mock.mutate(request: request)
    let variables = try? JSONSerialization.jsonObject(with: Data(request.variablesJson.utf8)) as? [String: Any]
    guard request.operationName == "CreateBlob", variables?["filename"] as? String == "0.jpg" else { return answer }
    let first = lock.withLock { () -> Bool in defer { refused = true }; return !refused }
    return first ? #"{"errors":[{"message":"no storage","extensions":{"code":"FORBIDDEN"}}]}"# : answer
  }
  func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    try await mock.scriptedPut(url: url, headers: headers, file: file, identity: identity, progress: progress)
  }
}

extension BridgeProbeTests {

  @MainActor @Test(.timeLimit(.minutes(2)), arguments: OpenOrder.allCases)
  func aSavedStepAndAnOrphanAbortGoOutOnceInEveryOpenOrder(order: OpenOrder) async throws {
    try await shiftingABatch(scenarios: ["reopen"], onlineAtOpen: order.onlineAtOpen)
    try await abortOfADiscardedPhotoAfterReopen(onlineAtOpen: order.onlineAtOpen)
  }

  @MainActor
  private func shiftingABatch(scenarios: [String], onlineAtOpen: Bool) async throws {
    for scenario in scenarios {
      let lose = scenario != "live"
      let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
      try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
      defer { try? FileManager.default.removeItem(at: directory) }
      let server = try SealedServer { path, body in
        if path == "/api/auth/login_link" { return ["sessionId": "A"] }
        switch body["operationName"] as? String {
        case "CreateBlob":
          let variables = body["variables"] as? [String: Any] ?? [:]
          let name = variables["filename"] as? String ?? "missing"
          return ["data": ["blobCreate": ["url": "https://storage/file", "headers": [String: String](), "signedId": "signed-\(name)", "blobId": "blob-\(name)"]]]
        case "CreateAssets": return ["data": ["createAssets": ["assets": [["id": "a2", "clientKey": "key-2", "size": 2, "position": 1], ["id": "a1", "clientKey": "key-1", "size": 2, "position": 0]]]]]
        default: return ["errors": [["message": "unexpected operation"]]]
        }
      }
      defer { server.hold(operation: nil) }
      let encrypted = EncryptedConfig(origin: SealedServer.origin, serverKey: SealedServer.serverKey, pin: SealedServer.pin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["login_link": EncryptedSessionOperation(path: "/api/auth/login_link", class: .authenticated)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel")
      let transport = HoldSecondFile(server)
      let credential = TransferCredential()
      let path = directory.appending(path: "db.sqlite").path
      let operations = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }"}]"#
      try await openStand(transport, path: path, encrypted: encrypted, credential: credential, server: server, operations: operations, onlineAtOpen: onlineAtOpen)
      _ = try await DB.database.sessionCall(operation: "login_link", bodyJson: #"{"account":"A"}"#)
      let items = try (0..<3).map { index in
        let file = directory.appending(path: "\(index).jpg")
        try Data("image-\(index)".utf8).write(to: file)
        return UploadItem(values: ["clientKey": .string("key-\(index)"), "size": .integer(3), "position": .integer(Int64(index))], path: file.path)
      }
      let receipt = try await DB.account().uploadBatch(Asset.self, items: items, request: UploadBatchRequest(operation: "create_files"))
      let rows = receipt.items.map(\.optimisticValue)
      await server.heldRequest()
      try await rows[0].discard(action: "create_files")
      if lose { try DB.database.setOnline(online: false); await server.heldDropped() }
      server.hold(operation: nil)
      if lose {
        try await openStand(transport, path: path, encrypted: encrypted, credential: credential, server: server, operations: operations, onlineAtOpen: onlineAtOpen)
        await eventually("the lost step is requested again") {
          server.requests.filter { $0.body["operationName"] as? String == "CreateBlob" && ($0.body["variables"] as? [String: Any])?["filename"] as? String == "1.jpg" }.count == 2
        }
        try await rowsReach("both assets are stored", Asset.ids(["a1", "a2"])) { Set($0.map(\.id)) == ["a1", "a2"] }
        #expect(try await Asset.find("a1").current()?.clientKey == "key-1")
        #expect(try await Asset.find("a2").current()?.clientKey == "key-2")
      } else {
        #expect(try await confirmedIds(receipt) == [nil, "a1", "a2"])
      }
      let blobs = server.effects.filter { $0["operationName"] as? String == "CreateBlob" }
      #expect(blobs.filter { ($0["variables"] as? [String: Any])?["filename"] as? String == "1.jpg" }.count == 1)
      let attempts = server.requests.filter { $0.body["operationName"] as? String == "CreateBlob" && ($0.body["variables"] as? [String: Any])?["filename"] as? String == "1.jpg" }
      #expect(attempts.count == (lose ? 2 : 1))
      #expect(server.routes.filter { $0.operation == "CreateBlob" }.allSatisfy { $0.purpose == .transfer })
      let puts = transport.identities.filter { $0.step == .put }
      #expect(puts.count == 3 && Set(puts.map(\.operationId)).count == 1 && Set(puts.map(\.itemId)).count == 3, "puts: \(puts.map { ($0.operationId, $0.itemId) })")
      #expect(server.requests.filter { $0.body["operationName"] as? String == "CreateAssets" }.count == 1)
      let final = try #require(server.requests.first { $0.body["operationName"] as? String == "CreateAssets" })
      let variables = try #require(final.body["variables"] as? [String: Any])
      let sent = try #require(variables["items"] as? [[String: Any]])
      #expect(sent.compactMap { $0["clientKey"] as? String } == ["key-1", "key-2"])
      #expect(sent.compactMap { $0["signedId"] as? String } == ["signed-1.jpg", "signed-2.jpg"])
      await DB.closeForCleanup()
    }
  }

  @MainActor
  private func abortOfADiscardedPhotoAfterReopen(onlineAtOpen: Bool) async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let source = directory.appending(path: "photo")
    try Data(repeating: 0, count: 11 * 1024 * 1024).write(to: source)
    let config = Config(path: directory.appending(path: "db.sqlite").path, dataVersion: nil)
    let operations = #"[{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"#
    let transport = MockTransport()
    transport.enqueue("InitMultipart", reply: #"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#)
    let held = transport.hold("PresignParts", reply: #"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#)
    let abort = transport.hold("AbortMultipart", reply: #"{"data":{"abortMultipart":{"ok":true}}}"#)
    transport.enqueue("PresignParts", reply: #"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}"#)
    let db = try await openSigned(config: config, modelsJson: modelsJSON, transport: transport, operations: operations, onlineAtOpen: onlineAtOpen)
    let queued = try #require(try await DB.account().send(Moment.self, values: ["text": .string("group")], upload: UploadCommand(action: "upload_clip", request: UploadRequest(path: source.path))).optimisticValue)
    await held.entered()
    try db.setOnline(online: false)
    await held.dropped()
    try await queued.discard(action: nil)
    #expect(try await Moment.find(queued.id).current() == nil)
    held.release()
    await DB.closeForCleanup()
    _ = try await openSigned(config: config, modelsJson: modelsJSON, transport: transport, operations: operations, onlineAtOpen: onlineAtOpen)
    await abort.entered()
    abort.release()
    await eventually("the saved pair of the held step is sent once more") { transport.calls.filter { $0.operationName == "PresignParts" }.count == 2 }
    let aborts = transport.calls.filter { $0.operationName == "AbortMultipart" }
    #expect(aborts.count == 1 && aborts.first?.purpose == .transfer)
    await DB.closeForCleanup()
  }

  private static let expiryOperations = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"}]"#

  private static func refusedUpload(_ transport: MockTransport, source: URL) async throws -> Moment {
    transport.enqueue("CreateBlob", failure: .Http(status: 422, detail: "rejected"))
    let sent = try await DB.account().send(Moment.self, values: ["text": .string("video")], upload: UploadCommand(action: "upload_clip", request: UploadRequest(path: source.path)))
    let row = try #require(sent.optimisticValue)
    try await rowsReach("the refused upload failed", Moment.ids([row.id])) { $0.first?.sync.state == .failed }
    let refused = try #require(try await Moment.find(row.id).current())
    #expect(refused.sync.state == .failed && refused.sync.error != nil)
    return row
  }
}

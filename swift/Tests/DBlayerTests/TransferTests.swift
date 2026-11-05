import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

/// The item outcomes of a batch, read from the core: the API has no item outcome yet (`holes` of the d4 report).
func confirmedPairs<Model: DBModel>(_ receipt: OperationReceipt<Model>) async throws -> [(key: String, id: String)?] {
  let outcome = try await receipt.database.batchOutcome(receipt: receipt.wire)
  guard case .confirmed = outcome.result else { throw DbError.Invalid(detail: "the batch did not confirm: \(outcome.result)") }
  return try outcome.items.map { item in
    if case let .confirmed(key, value) = item { return (key.value, try Model.decode(value).id) }
    return nil
  }
}

private func confirmedIds<Model: DBModel>(_ receipt: OperationReceipt<Model>) async throws -> [String?] {
  try await confirmedPairs(receipt).map { $0?.id }
}


private final class MultipartTransfer: Transport, @unchecked Sendable {
  let mock = MockTransport()
  private let lock = NSLock()
  private var urls: [String] = []
  private var captured: [TransferIdentity] = []
  private var capturedPaths: [(String, String)] = []
  var puts: [String] { lock.withLock { urls } }
  var identities: [TransferIdentity] { lock.withLock { captured } }
  var paths: [(String, String)] { lock.withLock { capturedPaths } }
  func openCable(lease: CableLease, url: String) async throws { try await mock.openCable(lease: lease, url: url) }
  func cableSend(token: String, text: String) async throws { try await mock.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await mock.cableNext(token: token) }
  func cableClose(token: String) async { await mock.cableClose(token: token) }
  func query(request: TransportRequest) async throws -> String { try await mock.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await mock.mutate(request: request) }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws { try await mock.subscribe(request: request, sink: sink) }
  func unsubscribe(token: String) { mock.unsubscribe(token: token) }
  func put(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    lock.withLock { urls.append(url); captured.append(identity); capturedPaths.append((identity.itemId, URL(fileURLWithPath: file.path).resolvingSymlinksInPath().path)) }
    return PutReply(status: 200, etag: "etag")
  }
}

private final class TransferCredential: CredentialStore, @unchecked Sendable {
  private let lock = NSLock()
  private var value: String?
  func get() throws -> String? { lock.withLock { value } }
  func set(credential: String) throws { lock.withLock { value = credential } }
  func clear() throws { lock.withLock { value = nil } }
}

private final class HoldSecondFile: Transport, @unchecked Sendable {
  private let mock = MockTransport()
  private let lock = NSLock()
  private var count = 0
  private var captured: [TransferIdentity] = []
  var identities: [TransferIdentity] { lock.withLock { captured } }
  let server: SealedServer
  init(_ server: SealedServer) { self.server = server }
  func openCable(lease: CableLease, url: String) async throws { try await mock.openCable(lease: lease, url: url) }
  func cableSend(token: String, text: String) async throws { try await mock.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await mock.cableNext(token: token) }
  func cableClose(token: String) async { await mock.cableClose(token: token) }
  func query(request: TransportRequest) async throws -> String { try await mock.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await mock.mutate(request: request) }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws { try await mock.subscribe(request: request, sink: sink) }
  func unsubscribe(token: String) { mock.unsubscribe(token: token) }
  func put(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    let first = lock.withLock { captured.append(identity); count += 1; return count == 1 }
    if first { server.hold(operation: "CreateBlob") }
    progress.sent(bytes: file.length)
    return PutReply(status: 200, etag: nil)
  }
}

private func createdBlob(_ index: Int) -> String {
  "{\"data\":{\"createBlob\":{\"url\":\"https://storage/file\",\"headers\":{},\"signedId\":\"signed-\(index)\",\"blobId\":\"b\(index)\"}}}"
}

private final class RefuseFirstFile: Transport, @unchecked Sendable {
  private let mock: MockTransport
  private let lock = NSLock()
  private var refused = false
  init(_ mock: MockTransport) { self.mock = mock }
  func openCable(lease: CableLease, url: String) async throws { try await mock.openCable(lease: lease, url: url) }
  func cableSend(token: String, text: String) async throws { try await mock.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await mock.cableNext(token: token) }
  func cableClose(token: String) async { await mock.cableClose(token: token) }
  func query(request: TransportRequest) async throws -> String { try await mock.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String {
    let answer = try await mock.mutate(request: request)
    let variables = try? JSONSerialization.jsonObject(with: Data(request.variablesJson.utf8)) as? [String: Any]
    guard request.operationName == "CreateBlob", variables?["filename"] as? String == "0.jpg" else { return answer }
    let first = lock.withLock { () -> Bool in defer { refused = true }; return !refused }
    return first ? #"{"errors":[{"message":"no storage","extensions":{"code":"FORBIDDEN"}}]}"# : answer
  }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws { try await mock.subscribe(request: request, sink: sink) }
  func unsubscribe(token: String) { mock.unsubscribe(token: token) }
  func put(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    try await mock.put(url: url, headers: headers, file: file, identity: identity, progress: progress)
  }
}

extension BridgeProbeTests {
  @MainActor @Test(.timeLimit(.minutes(1)))
  func multipartBatchAcceptsACorrelationKeyWithPathCharacters() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let source = directory.appending(path: "video")
    try Data(repeating: 0, count: 11 * 1024 * 1024).write(to: source)
    let transport = MultipartTransfer()
    for _ in 0..<2 {
      transport.mock.enqueue("InitMultipart", reply: #"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#)
      transport.mock.enqueue("PresignParts", reply: #"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#)
      transport.mock.enqueue("CompleteMultipart", reply: #"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#)
    }
    transport.mock.enqueue("CreateAssets", reply: #"{"data":{"createAssets":{"assets":[{"id":"a0","clientKey":"../../../outside","size":2,"position":0},{"id":"a1","clientKey":"other","size":2,"position":1}]}}}"#)
    let operations = #"[{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }"}]"#
    let uploads = UploadOperations(multipart: .init(initiate: .init("InitMultipart"), presign: .init("PresignParts"), complete: .init("CompleteMultipart"), abort: .init("AbortMultipart")))
    try await reopen(config: Config(path: directory.appending(path: "db.sqlite").path, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON, transport: transport, operations: operations, uploads: uploads)
    let items = ["../../../outside", "other"].enumerated().map { index, key in UploadItem(values: ["clientKey": .string(key), "size": .integer(2), "position": .integer(Int64(index))], path: source.path) }
    let receipt = try await DB.account().uploadBatch(Asset.self, items: items, request: UploadBatchRequest(operation: "create_files"))
    #expect(try await confirmedIds(receipt) == ["a0", "a1"])
    #expect(transport.identities.filter { $0.itemId == "../../../outside" }.count == 3)
    let ranges = directory.appending(path: "db.sqlite.cache/ranges").resolvingSymlinksInPath().path + "/"
    #expect(transport.paths.filter { $0.0 == "../../../outside" }.allSatisfy { $0.1.hasPrefix(ranges) })
    await DB.closeForCleanup()
  }

  @MainActor @Test(.timeLimit(.minutes(1)))
  func shiftingABatchDuringAHeldEncryptedRequestKeepsTheFileOwner() async throws {
    for scenario in ["live", "reopen"] {
      let lose = scenario != "live"
      let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
      try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
      defer { try? FileManager.default.removeItem(at: directory) }
      let server = try SealedServer(deduplicate: true) { path, body in
        if path == "/api/auth/login_link" { return ["sessionId": "A"] }
        switch body["operationName"] as? String {
        case "CreateBlob":
          let variables = body["variables"] as? [String: Any] ?? [:]
          let name = variables["filename"] as? String ?? "missing"
          return ["data": ["createBlob": ["url": "https://storage/file", "headers": [String: String](), "signedId": "signed-\(name)", "blobId": "blob-\(name)"]]]
        case "CreateAssets": return ["data": ["createAssets": ["assets": [["id": "a2", "clientKey": "key-2", "size": 2, "position": 1], ["id": "a1", "clientKey": "key-1", "size": 2, "position": 0]]]]]
        default: return ["errors": [["message": "unexpected operation"]]]
        }
      }
      defer { server.hold(operation: nil) }
      let encrypted = EncryptedConfig(origin: SealedServer.origin, serverKey: SealedServer.serverKey, pin: SealedServer.pin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["login_link": EncryptedSessionOperation(path: "/api/auth/login_link", sessionless: false)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel")
      let transport = HoldSecondFile(server)
      let credential = TransferCredential()
      let path = directory.appending(path: "db.sqlite").path
      let uploads = UploadOperations(createBlob: .init("CreateBlob"))
      let operations = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }"}]"#
      await DB.closeForCleanup()
      _ = try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, path: path, uploads: uploads, encrypted: encrypted, credentials: credential, http: server)
      try DB.database.registerOperations(defsJson: operations)
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
      if lose { try DB.database.setOnline(online: false); server.loseAnswer(filename: "1.jpg") }
      server.hold(operation: nil)
      if lose {
        await server.lostAnswer()
        await DB.closeForCleanup()
        _ = try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, path: path, uploads: uploads, encrypted: encrypted, credentials: credential, http: server)
        try DB.database.registerOperations(defsJson: operations)
        await eventually("both assets are stored") {
          let a1 = (try? await Asset.find("a1").current()) ?? nil
          let a2 = (try? await Asset.find("a2").current()) ?? nil
          return a1 != nil && a2 != nil
        }
        #expect(try await Asset.find("a1").current() != nil)
        #expect(try await Asset.find("a2").current() != nil)
      } else {
        #expect(try await confirmedIds(receipt) == [nil, "a1", "a2"])
      }
      let blobs = server.effects.filter { $0["operationName"] as? String == "CreateBlob" }
      #expect(blobs.filter { ($0["variables"] as? [String: Any])?["filename"] as? String == "1.jpg" }.count == 1)
      let attempts = server.requests.filter { $0.body["operationName"] as? String == "CreateBlob" && ($0.body["variables"] as? [String: Any])?["filename"] as? String == "1.jpg" }
      #expect(attempts.count == (lose ? 2 : 1))
      #expect(server.routes.filter { $0.operation == "CreateBlob" }.allSatisfy { $0.purpose == .transfer })
      let second = try #require(transport.identities.first { $0.itemId == "key-1" })
      let third = try #require(transport.identities.first { $0.itemId == "key-2" })
      #expect(second.operationId == third.operationId)
      #expect(second.step == .put)
      #expect(third.step == .put)
      let final = try #require(server.requests.first { $0.body["operationName"] as? String == "CreateAssets" })
      let variables = try #require(final.body["variables"] as? [String: Any])
      let sent = try #require(variables["items"] as? [[String: Any]])
      #expect(sent.compactMap { $0["clientKey"] as? String } == ["key-1", "key-2"])
      #expect(sent.compactMap { $0["signedId"] as? String } == ["signed-1.jpg", "signed-2.jpg"])
      await DB.closeForCleanup()
    }
  }

  @MainActor @Test(.timeLimit(.minutes(1)))
  func reopeningAfterSevenDaysExpiresAFailedBlob() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let source = directory.appending(path: "video")
    try Data("video".utf8).write(to: source)
    let path = directory.appending(path: "db.sqlite").path
    let uploads = UploadOperations(createBlob: .init("CreateBlob"))
    let operations = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"}]"#
    await DB.closeForCleanup()
    let transport = MockTransport()
    transport.enqueue("CreateBlob", failure: .Http(status: 422, detail: "rejected"))
    let start = Date(timeIntervalSince1970: 1_000)
    _ = try await DB.openTesting(transport: transport, modelsJSON: "[]", path: path, uploads: uploads, startingAt: start)
    try DB.database.registerOperations(defsJson: operations)
    let blob = try await DB.upload(BlobUpload(name: "video", create: "CreateBlob"), request: UploadRequest(path: source.path))
    do { _ = try await blob.waitUntilUploaded(); Issue.record("the refused upload returned a signed ID") } catch {}
    let id = blob.id
    await DB.closeForCleanup()
    _ = try await DB.openTesting(transport: transport, modelsJSON: "[]", path: path, uploads: uploads, startingAt: start.addingTimeInterval(7 * 24 * 60 * 60))
    try DB.database.registerOperations(defsJson: operations)
    let status = try AccountScope().uploadStatus(id)
    let updates = Cursor(status)
    #expect(await updates.first("the expired status") { $0.state == .expired } != nil)
    status.stream.cancel()
    await DB.closeForCleanup()
  }

  @MainActor @Test(.timeLimit(.minutes(1)))
  func anHttpRefusalOfAPhotoKeepsTheFailedRecordUntilDiscard() async throws {
    let source = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("image".utf8).write(to: source)
    defer { try? FileManager.default.removeItem(at: source) }
    let transport = MockTransport()
    transport.enqueue("CreateBlob", reply: #"{"data":{"createBlob":{"url":"https://storage/file","headers":{},"signedId":"signed","blobId":"blob"}}}"#)
    transport.enqueue("CreateMoment", failure: .Http(status: 422, detail: "rejected"))
    let operations = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt } }"}]"#
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport, operations: operations, uploads: UploadOperations(createBlob: .init("CreateBlob")))
    let receipt = try await DB.account().send(Moment.self, values: ["text": .string("group")], upload: UploadCommand(action: "upload_clip", request: UploadRequest(path: source.path)))
    let row = try #require(receipt.optimisticValue)
    guard case .rejected = try await receipt.outcome() else { throw DbError.Invalid(detail: "the refused photo reported success") }
    let failed = try #require(try await Moment.find(row.id).current())
    #expect(failed.sync.state == .failed)
    try await failed.discard(action: nil)
    #expect(try await Moment.find(row.id).current() == nil)
    await DB.closeForCleanup()
  }

  @MainActor @Test(.timeLimit(.minutes(1)))
  func discardingAQueuedPhotoOfflineRoutesTheAbortAfterReopen() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let source = directory.appending(path: "photo")
    try Data(repeating: 0, count: 11 * 1024 * 1024).write(to: source)
    let config = Config(path: directory.appending(path: "db.sqlite").path, dataVersion: nil, cacheRows: 32)
    let operations = #"[{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"#
    let uploads = UploadOperations(multipart: .init(initiate: .init("InitMultipart"), presign: .init("PresignParts"), complete: .init("CompleteMultipart"), abort: .init("AbortMultipart")))
    let transport = MockTransport()
    transport.enqueue("InitMultipart", reply: #"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#)
    let held = transport.hold("PresignParts")
    let abort = transport.hold("AbortMultipart", reply: #"{"data":{"abortMultipart":{"ok":true}}}"#)
    let db = try await reopen(config: config, modelsJson: modelsJSON, transport: transport, operations: operations, uploads: uploads)
    let queued = try #require(try await DB.account().send(Moment.self, values: ["text": .string("group")], upload: UploadCommand(action: "upload_clip", request: UploadRequest(path: source.path))).optimisticValue)
    await held.entered()
    try db.setOnline(online: false)
    try await queued.discard(action: nil)
    #expect(try await Moment.find(queued.id).current() == nil)
    held.release()
    await DB.closeForCleanup()
    let reopened = try await reopen(config: config, modelsJson: modelsJSON, transport: transport, operations: operations, uploads: uploads)
    try reopened.setOnline(online: false)
    try reopened.setOnline(online: true)
    await abort.entered()
    abort.release()
    try reopened.setOnline(online: false)
    try reopened.setOnline(online: true)
    let aborts = transport.calls.filter { $0.operationName == "AbortMultipart" }
    #expect(aborts.count == 1 && aborts.first?.purpose == .transfer)
    await DB.closeForCleanup()
  }

  @MainActor @Test(.timeLimit(.minutes(1)))
  func discardingARefusedFileSendsTheRemainingFileAsSingle() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let files = try (0..<2).map { index in
      let file = directory.appending(path: "\(index).jpg")
      try Data("image-\(index)".utf8).write(to: file)
      return file
    }
    let transport = MockTransport()
    for index in 0..<2 { transport.enqueue("CreateBlob", reply: createdBlob(index)) }
    transport.enqueue("CreateAsset", reply: #"{"data":{"createAsset":{"asset":{"id":"a1","clientKey":"key-1","size":2,"position":1}}}}"#)
    let operations = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }"}]"#
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: RefuseFirstFile(transport), operations: operations, uploads: UploadOperations(createBlob: .init("CreateBlob")))
    let items = files.enumerated().map { index, file in UploadItem(values: ["clientKey": .string("key-\(index)"), "size": .integer(2), "position": .integer(Int64(index))], path: file.path) }
    let receipt = try await DB.account().uploadBatch(Asset.self, items: items, request: UploadBatchRequest(operation: "create_files"))
    let rows = receipt.items.map(\.optimisticValue)
    await eventually("the refused file failed") { ((try? await Asset.find(rows[0].id).current()?.sync.state) ?? nil) == .failed }
    #expect(try await Asset.find(rows[0].id).current()?.sync.state == .failed)
    try await rows[0].discard(action: "create_files")
    #expect(try await confirmedIds(receipt) == [nil, "a1"])
    #expect(transport.calls.filter { $0.operationName == "CreateBlob" }.count == 2)
    #expect(transport.calls.allSatisfy { $0.operationName != "CreateAssets" })
    #expect(transport.calls.filter { $0.operationName == "CreateAsset" }.count == 1)
    let call = try #require(transport.calls.first { $0.operationName == "CreateAsset" })
    let variables = try JSONSerialization.jsonObject(with: Data(call.variablesJson.utf8)) as! [String: Any]
    #expect(variables["clientKey"] as? String == "key-1")
    await DB.closeForCleanup()
  }

  @MainActor @Test(.timeLimit(.minutes(1)))
  func retryingARefusedFileKeepsTheOtherAlbumFiles() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let files = try (0..<3).map { index in
      let file = directory.appending(path: "\(index).jpg")
      try Data("image-\(index)".utf8).write(to: file)
      return file
    }
    let transport = MockTransport()
    for index in 0..<4 { transport.enqueue("CreateBlob", reply: createdBlob(index)) }
    transport.enqueue("CreateAssets", reply: #"{"data":{"createAssets":{"assets":[{"id":"a2","clientKey":"key-2","size":3,"position":2},{"id":"a1","clientKey":"key-1","size":3,"position":1},{"id":"a0","clientKey":"key-0","size":3,"position":0}]}}}"#)
    let operations = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }"}]"#
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: RefuseFirstFile(transport), operations: operations, uploads: UploadOperations(createBlob: .init("CreateBlob")))
    let items = files.enumerated().map { index, file in UploadItem(values: ["clientKey": .string("key-\(index)"), "size": .integer(3), "position": .integer(Int64(index))], path: file.path) }
    let receipt = try await DB.account().uploadBatch(Asset.self, items: items, request: UploadBatchRequest(operation: "create_files"))
    let rows = receipt.items.map(\.optimisticValue)
    await eventually("the refused file failed") { ((try? await Asset.find(rows[0].id).current()?.sync.state) ?? nil) == .failed }
    #expect(try await Asset.find(rows[0].id).current()?.sync.state == .failed)
    #expect(try await rows[0].retry(action: "create_files").operation == receipt.operation)
    #expect(try await confirmedIds(receipt) == ["a0", "a1", "a2"])
    let call = try #require(transport.calls.first { $0.operationName == "CreateAssets" })
    let variables = try JSONSerialization.jsonObject(with: Data(call.variablesJson.utf8)) as! [String: Any]
    let sent = variables["items"] as! [[String: Any]]
    #expect(sent.compactMap { $0["clientKey"] as? String } == ["key-0", "key-1", "key-2"])
    #expect(transport.calls.filter { $0.operationName == "CreateBlob" }.count == 4)
    let filenames = try transport.calls.filter { $0.operationName == "CreateBlob" }.map { call in
      let variables = try #require(JSONSerialization.jsonObject(with: Data(call.variablesJson.utf8)) as? [String: Any])
      return try #require(variables["filename"] as? String)
    }
    #expect(filenames.sorted() == ["0.jpg", "0.jpg", "1.jpg", "2.jpg"])
    #expect(transport.calls.filter { $0.operationName == "CreateAssets" }.count == 1)
    await DB.closeForCleanup()
  }

  @MainActor @Test(.timeLimit(.minutes(1)))
  func cancellingTheMiddleFileKeepsTheOtherFilesInOrder() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let files = try (0..<3).map { index in
      let file = directory.appending(path: "\(index).jpg")
      try Data("image-\(index)".utf8).write(to: file)
      return file
    }
    let transport = MockTransport()
    let held = transport.hold("CreateBlob", reply: #"{"data":{"createBlob":{"url":"https://storage/file","headers":{},"signedId":"signed-0","blobId":"b0"}}}"#)
    transport.enqueue("CreateBlob", reply: #"{"data":{"createBlob":{"url":"https://storage/file","headers":{},"signedId":"signed-2","blobId":"b2"}}}"#)
    transport.enqueue("CreateAssets", reply: #"{"data":{"createAssets":{"assets":[{"id":"a2","clientKey":"key-2","size":2,"position":1},{"id":"a0","clientKey":"key-0","size":2,"position":0}]}}}"#)
    let operations = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }"}]"#
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport, operations: operations, uploads: UploadOperations(createBlob: .init("CreateBlob")))
    let items = files.enumerated().map { index, file in UploadItem(values: ["clientKey": .string("key-\(index)"), "size": .integer(3), "position": .integer(Int64(index))], path: file.path) }
    let receipt = try await DB.account().uploadBatch(Asset.self, items: items, request: UploadBatchRequest(operation: "create_files"))
    let rows = receipt.items.map(\.optimisticValue)
    await held.entered()
    #expect(try await rows[1].cancelUpload(action: "create_files"))
    let first = try #require(try await Asset.find(rows[0].id).current())
    let last = try #require(try await Asset.find(rows[2].id).current())
    #expect(first.position == 0 && last.position == 1 && first.size == 2 && last.size == 2)
    held.release()
    #expect(try await confirmedIds(receipt) == ["a0", nil, "a2"])
    let call = try #require(transport.calls.first { $0.operationName == "CreateAssets" })
    let variables = try JSONSerialization.jsonObject(with: Data(call.variablesJson.utf8)) as! [String: Any]
    let sent = variables["items"] as! [[String: Any]]
    #expect(sent.compactMap { $0["clientKey"] as? String } == ["key-0", "key-2"])
    #expect(transport.calls.filter { $0.operationName == "CreateBlob" }.count == 2)
    #expect(transport.calls.filter { $0.operationName == "CreateAssets" }.count == 1)
    let filenames = try transport.calls.filter { $0.operationName == "CreateBlob" }.map { call in
      let variables = try #require(JSONSerialization.jsonObject(with: Data(call.variablesJson.utf8)) as? [String: Any])
      return try #require(variables["filename"] as? String)
    }
    #expect(filenames.sorted() == ["0.jpg", "2.jpg"])
    await DB.closeForCleanup()
  }

  @MainActor @Test(.timeLimit(.minutes(1)))
  func aFailedBlobExpiresWhenTheNetworkReturnsAfterSevenDays() async throws {
    let source = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("video".utf8).write(to: source)
    defer { try? FileManager.default.removeItem(at: source) }
    await DB.closeForCleanup()
    let transport = MockTransport()
    transport.enqueue("CreateBlob", failure: .Http(status: 422, detail: "rejected"))
    let clock = try await DB.openTesting(transport: transport, modelsJSON: "[]", uploads: UploadOperations(createBlob: .init("CreateBlob")))
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"}]"#)
    let blob = try await DB.upload(BlobUpload(name: "video", create: "CreateBlob"), request: UploadRequest(path: source.path))
    do { _ = try await blob.waitUntilUploaded(); Issue.record("the refused upload returned a signed ID") } catch {}
    let status = try AccountScope().uploadStatus(blob.id)
    let updates = Cursor(status)
    try DB.database.setOnline(online: false)
    clock.advance(by: 7 * 24 * 60 * 60)
    try DB.database.setOnline(online: true)
    #expect(await updates.first("the expired status") { $0.state == .expired } != nil)
    let result = try await DB.database.blob(epoch: DB.database.sessionEpoch(), id: blob.id)
    #expect(result.state == .expired)
    status.stream.cancel()
    await DB.closeForCleanup()
  }

  @MainActor @Test(.timeLimit(.minutes(1)))
  func completeFailureThenRetryKeepsTheConfirmedParts() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let source = directory.appending(path: "video")
    try Data(repeating: 0, count: 11 * 1024 * 1024).write(to: source)
    let transport = MultipartTransfer()
    transport.mock.enqueue("InitMultipart", reply: #"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#)
    transport.mock.enqueue("PresignParts", reply: #"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#)
    transport.mock.enqueue("CompleteMultipart", failure: .Http(status: 422, detail: "rejected"))
    transport.mock.enqueue("CompleteMultipart", reply: #"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#)
    let operations = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"#
    let uploads = UploadOperations(multipart: .init(initiate: .init("InitMultipart"), presign: .init("PresignParts"), complete: .init("CompleteMultipart"), abort: .init("AbortMultipart")))
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: "[]", transport: transport, operations: operations, uploads: uploads)
    let blob = try await DB.upload(BlobUpload(name: "video", create: "CreateBlob"), request: UploadRequest(path: source.path))
    do { _ = try await blob.waitUntilUploaded(); Issue.record("the failed complete returned a signed ID") } catch {}
    try await blob.retry()
    #expect(try await blob.waitUntilUploaded() == "signed")
    let names = transport.mock.calls.map(\.operationName)
    #expect(names.filter { $0 == "InitMultipart" }.count == 1)
    #expect(names.filter { $0 == "PresignParts" }.count == 1)
    #expect(names.filter { $0 == "CompleteMultipart" }.count == 2)
    #expect(transport.puts.sorted() == ["https://storage/1", "https://storage/2", "https://storage/3"])
    await DB.closeForCleanup()
  }

  @MainActor @Test(.timeLimit(.minutes(1)))
  func attachedFilesSurviveReopenAndDetachingOneKeepsTheOther() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let config = Config(path: directory.appending(path: "files.sqlite").path, dataVersion: nil, cacheRows: 32)
    try await reopen(config: config, modelsJson: modelsJSON, transport: nil)
    let first = try #require(try await User.create(name: "first").optimisticValue)
    let second = try #require(try await User.create(name: "second").optimisticValue)
    let large = Data(repeating: 9, count: 300 * 1024)
    _ = try await first.attach(.avatar, data: Data([1, 2, 3]), name: "small", mime: "image/png")
    _ = try await second.attach(.avatar, data: large, name: "large", mime: "image/png")
    #expect(try await first.read(.avatar) == Data([1, 2, 3]))
    let url = try #require(try await second.url(.avatar))
    #expect(try Data(contentsOf: url) == large)
    _ = try await first.attach(.avatar, data: Data([4]), name: "replacement", mime: "image/png")
    #expect(try await first.detach(.avatar))
    let (firstId, secondId) = (first.id, second.id)
    await DB.closeForCleanup()
    try await reopen(config: config, modelsJson: modelsJSON, transport: nil)
    let restoredFirst = try #require(try await User.find(firstId).current())
    let restoredSecond = try #require(try await User.find(secondId).current())
    #expect(try await restoredFirst.read(.avatar) == nil)
    #expect(try await restoredSecond.read(.avatar) == large)
    let info = try #require(try await restoredSecond.file(.avatar))
    #expect(info.name == "large" && info.size == 300 * 1024)
    await DB.closeForCleanup()
  }

  @MainActor @Test(.timeLimit(.minutes(1)))
  func anOfflineDiscardRoutesItsReopenedAbortAsATransfer() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let source = directory.appending(path: "video")
    try Data(repeating: 0, count: 11 * 1024 * 1024).write(to: source)
    let config = Config(path: directory.appending(path: "db.sqlite").path, dataVersion: nil, cacheRows: 8)
    let operations = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"#
    let uploads = UploadOperations(multipart: .init(initiate: .init("InitMultipart"), presign: .init("PresignParts"), complete: .init("CompleteMultipart"), abort: .init("AbortMultipart")))
    let transport = MockTransport()
    transport.enqueue("InitMultipart", reply: #"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#)
    let held = transport.hold("PresignParts")
    let abort = transport.hold("AbortMultipart", reply: #"{"data":{"abortMultipart":{"ok":true}}}"#)
    let db = try await reopen(config: config, modelsJson: "[]", transport: transport, operations: operations, uploads: uploads)
    let blob = try await DB.upload(BlobUpload(name: "video", create: "CreateBlob"), request: UploadRequest(path: source.path))
    await held.entered()
    let files = directory.appending(path: "db.sqlite.files")
    #expect(try FileManager.default.contentsOfDirectory(atPath: files.path).count == 1)
    try db.setOnline(online: false)
    try await blob.discard()
    #expect(try FileManager.default.contentsOfDirectory(atPath: files.path).isEmpty)
    #expect(FileManager.default.fileExists(atPath: source.path))
    held.release()
    await DB.closeForCleanup()
    let reopened = try await reopen(config: config, modelsJson: "[]", transport: transport, operations: operations, uploads: uploads)
    try reopened.setOnline(online: false)
    try reopened.setOnline(online: true)
    await abort.entered()
    abort.release()
    try reopened.setOnline(online: false)
    try reopened.setOnline(online: true)
    let aborts = transport.calls.filter { $0.operationName == "AbortMultipart" }
    #expect(aborts.count == 1)
    #expect(aborts.first?.purpose == .transfer)
    await DB.closeForCleanup()
  }
}

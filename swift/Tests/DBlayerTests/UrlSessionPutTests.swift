import Foundation
import Testing
@testable import DBlayer

private final class AnswerProtocol: URLProtocol, @unchecked Sendable {
  nonisolated(unsafe) static var body = Data()
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
    client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    client?.urlProtocol(self, didLoad: Self.body)
    client?.urlProtocolDidFinishLoading(self)
  }
  override func stopLoading() {}
}

private func mallocInUse() -> Int {
  var statistics = malloc_statistics_t()
  malloc_zone_statistics(nil, &statistics)
  return statistics.size_in_use
}

/// Reads the body of the request as a stream and records what the transport holds in memory while the body leaves.
private final class PartProtocol: URLProtocol, @unchecked Sendable {
  nonisolated(unsafe) static var received = 0
  nonisolated(unsafe) static var mismatch: Int?
  nonisolated(unsafe) static var contentLength: String?
  nonisolated(unsafe) static var peakInUse = 0
  nonisolated(unsafe) static var first: UInt64 = 0
  static let pattern = (0..<64 * 1024 + 256).map { UInt8(truncatingIfNeeded: $0) }
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    Self.contentLength = request.value(forHTTPHeaderField: "Content-Length")
    if let stream = request.httpBodyStream {
      stream.open()
      var buffer = [UInt8](repeating: 0, count: 64 * 1024)
      while true {
        Self.peakInUse = max(Self.peakInUse, mallocInUse())
        let count = stream.read(&buffer, maxLength: buffer.count)
        if count <= 0 { break }
        let start = Int((Self.first + UInt64(Self.received)) % 256)
        if Self.mismatch == nil && memcmp(buffer, Array(Self.pattern[start..<start + count]), count) != 0 { Self.mismatch = Self.received }
        Self.received += count
      }
      stream.close()
    }
    let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: ["ETag": "part"])!
    client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    client?.urlProtocolDidFinishLoading(self)
  }
  override func stopLoading() {}
}

/// Reads the first piece of the body and then stalls, or fails the request, the way a server that stops reading does.
private final class StallProtocol: URLProtocol, @unchecked Sendable {
  nonisolated(unsafe) static var started: AsyncStream<Void>.Continuation?
  nonisolated(unsafe) static var failsEarly = false
  private var stream: InputStream?
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    stream = request.httpBodyStream
    stream?.open()
    var buffer = [UInt8](repeating: 0, count: 64 * 1024)
    _ = stream?.read(&buffer, maxLength: buffer.count)
    Self.started?.yield()
    if Self.failsEarly { client?.urlProtocol(self, didFailWithError: URLError(.networkConnectionLost)) }
  }
  override func stopLoading() { stream?.close() }
}

private func openDescriptors(of path: String) -> Int {
  let target = URL(fileURLWithPath: path).resolvingSymlinksInPath().path
  var buffer = [CChar](repeating: 0, count: Int(MAXPATHLEN))
  var count = 0
  for descriptor in 0..<getdtablesize() where fcntl(descriptor, F_GETPATH, &buffer) == 0 {
    if URL(fileURLWithPath: String(cString: buffer)).resolvingSymlinksInPath().path == target { count += 1 }
  }
  return count
}

private final class NoProgress: ByteProgress, @unchecked Sendable {
  init() { super.init(noHandle: .init()) }
  required init(unsafeFromHandle handle: UInt64) { fatalError("not a core object") }
  override func sent(bytes: UInt64) {}
  override func isCancelled() -> Bool { false }
}

struct UrlSessionPutTests {
  /// bridge_prop_raw_io (P-BR-08): the body of the PUT answer reaches the core as it is, the empty one too.
  @Test(arguments: ["stored", ""])
  func thePutAnswerBodyReachesTheCore(body: String) async throws {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [AnswerProtocol.self]
    AnswerProtocol.body = Data(body.utf8)
    let file = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try Data("0123".utf8).write(to: file)
    defer { try? FileManager.default.removeItem(at: file) }
    let transport = UrlSessionTransport(session: URLSession(configuration: configuration))
    let outcome = await transport.put(target: PutTarget(url: "https://example.com/part", headers: [:], connectMs: 10_000, requestMs: 30_000), file: PutFile(path: file.path, offset: 0, length: 4), identity: TransferIdentity(operationId: 1, itemId: "i", step: .put), progress: NoProgress())
    #expect(outcome == .response(status: 200, etag: nil, body: body))
  }

  /// bridge.md HTTP of the platform (D-285): a part of a file leaves from the file system, the transport does not hold the part in memory.
  @Test func aPartOfTheFileIsSentWithoutHoldingItInMemory() async throws {
    let megabyte = 1024 * 1024
    let (offset, length, total) = (UInt64(8 * megabyte), UInt64(64 * megabyte), 80 * megabyte)
    let file = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    #expect(FileManager.default.createFile(atPath: file.path, contents: nil))
    defer { try? FileManager.default.removeItem(at: file) }
    let writer = try FileHandle(forWritingTo: file)
    let block = Data((0..<megabyte).map { UInt8(truncatingIfNeeded: $0) })
    for _ in stride(from: 0, to: total, by: megabyte) {
      try writer.write(contentsOf: block)
    }
    try writer.close()
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [PartProtocol.self]
    (PartProtocol.received, PartProtocol.mismatch, PartProtocol.contentLength, PartProtocol.peakInUse, PartProtocol.first) = (0, nil, nil, 0, offset)
    let transport = UrlSessionTransport(session: URLSession(configuration: configuration))
    let before = mallocInUse()
    let outcome = await transport.put(target: PutTarget(url: "https://example.com/part", headers: [:], connectMs: 10_000, requestMs: 30_000), file: PutFile(path: file.path, offset: offset, length: length), identity: TransferIdentity(operationId: 1, itemId: "i", step: .put), progress: NoProgress())
    #expect(outcome == .response(status: 200, etag: "part", body: ""))
    #expect(PartProtocol.received == Int(length), "the server reads exactly the part")
    #expect(PartProtocol.mismatch == nil, "the bytes of the part are the bytes of the file range")
    #expect(PartProtocol.contentLength == String(length), "the request names the length of the part")
    #expect(PartProtocol.peakInUse - before < 16 * megabyte, "a part of 64 MiB leaves without 64 MiB in memory: \(PartProtocol.peakInUse - before) bytes")
  }

  /// A part that stops in the middle, by a cancel of the caller or by the server, ends the call with a failure and leaves no open file behind.
  @Test(.serialized, .timeLimit(.minutes(1)), arguments: [false, true])
  func aStoppedPartEndsTheCallAndLeavesNoOpenFile(serverFails: Bool) async throws {
    let megabyte = 1024 * 1024
    let file = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try #require(FileManager.default.createFile(atPath: file.path, contents: Data(count: 8 * megabyte)))
    defer { try? FileManager.default.removeItem(at: file) }
    let (events, started) = AsyncStream<Void>.makeStream()
    (StallProtocol.started, StallProtocol.failsEarly) = (started, serverFails)
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [StallProtocol.self]
    let transport = UrlSessionTransport(session: URLSession(configuration: configuration))
    let call = Task {
      await transport.put(target: PutTarget(url: "https://example.com/part", headers: [:], connectMs: 10_000, requestMs: 30_000), file: PutFile(path: file.path, offset: UInt64(megabyte), length: UInt64(4 * megabyte)), identity: TransferIdentity(operationId: 1, itemId: "i", step: .put), progress: NoProgress())
    }
    var first = events.makeAsyncIterator()
    _ = await first.next()
    if !serverFails {
      #expect(openDescriptors(of: file.path) == 1, "the stalled part holds its file open")
      call.cancel()
    }
    guard case .failed = await call.value else {
      Issue.record("a stopped part is a failure of the call")
      return
    }
    #expect(openDescriptors(of: file.path) == 0, "the file of the part is closed when the call returns")
  }
}

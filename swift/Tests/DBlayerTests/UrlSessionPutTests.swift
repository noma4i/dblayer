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
}

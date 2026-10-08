import DBlayer
import Foundation

public final class ScriptedURLProtocol: URLProtocol, @unchecked Sendable {
  private static let lock = NSLock()
  private nonisolated(unsafe) static var queuedResponses: [String: [(HTTPURLResponse, Data)]] = [:]
  private nonisolated(unsafe) static var storedRecordedRequests: [String: [URLRequest]] = [:]
  private nonisolated(unsafe) static var storedWireBodies: [String: [Data]] = [:]

  public static func recordedRequests(scriptId: String) -> [URLRequest] {
    lock.lock()
    defer { lock.unlock() }
    return storedRecordedRequests[scriptId, default: []]
  }

  public static func wireBodies(scriptId: String) -> [Data] {
    lock.lock()
    defer { lock.unlock() }
    return storedWireBodies[scriptId, default: []]
  }

  public static func push(scriptId: String, status: Int, headers: [String: String], body: Data, url: URL) {
    guard let response = HTTPURLResponse(
      url: url,
      statusCode: status,
      httpVersion: nil,
      headerFields: headers
    ) else {
      return
    }
    lock.lock()
    defer { lock.unlock() }
    queuedResponses[scriptId, default: []].append((response, body))
  }

  public static func reset(scriptId: String) {
    lock.lock()
    defer { lock.unlock() }
    queuedResponses.removeValue(forKey: scriptId)
    storedRecordedRequests.removeValue(forKey: scriptId)
    storedWireBodies.removeValue(forKey: scriptId)
  }

  public override class func canInit(with request: URLRequest) -> Bool {
    true
  }

  public override class func canonicalRequest(for request: URLRequest) -> URLRequest {
    request
  }

  public override func startLoading() {
    guard let scriptId = request.value(forHTTPHeaderField: "X-Scripted-Session") else {
      client?.urlProtocol(self, didFailWithError: URLError(.resourceUnavailable))
      return
    }

    let wireBody = Self.body(of: request)
    var recordedRequest = request
    recordedRequest.httpBody = Self.recordedBody(of: request, wireBody: wireBody)
    Self.lock.lock()
    Self.storedRecordedRequests[scriptId, default: []].append(recordedRequest)
    Self.storedWireBodies[scriptId, default: []].append(wireBody)
    var queuedResponse: (HTTPURLResponse, Data)?
    if var responses = Self.queuedResponses[scriptId], !responses.isEmpty {
      queuedResponse = responses.removeFirst()
      if responses.isEmpty {
        Self.queuedResponses.removeValue(forKey: scriptId)
      } else {
        Self.queuedResponses[scriptId] = responses
      }
    }
    Self.lock.unlock()

    guard let (response, body) = queuedResponse else {
      client?.urlProtocol(self, didFailWithError: URLError(.resourceUnavailable))
      return
    }

    client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    client?.urlProtocol(self, didLoad: body)
    client?.urlProtocolDidFinishLoading(self)
  }

  public override func stopLoading() {}

  public static func session(scriptId: String) -> URLSession {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.httpAdditionalHeaders = ["X-Scripted-Session": scriptId]
    configuration.protocolClasses = [Self.self]
    return URLSession(configuration: configuration)
  }

  public static func body(of request: URLRequest) -> Data {
    if let body = request.httpBody {
      return body
    }
    guard let stream = request.httpBodyStream else {
      return Data()
    }

    stream.open()
    defer { stream.close() }

    var body = Data()
    var buffer = [UInt8](repeating: 0, count: 4096)
    while stream.hasBytesAvailable {
      let readCount = stream.read(&buffer, maxLength: buffer.count)
      guard readCount > 0 else { break }
      body.append(buffer, count: readCount)
    }
    return body
  }

  private static func recordedBody(of request: URLRequest, wireBody: Data) -> Data {
    guard request.value(forHTTPHeaderField: "X-Payload-Encryption") == "1" else {
      return wireBody
    }
    guard let envelope = try? JSONDecoder().decode(PayloadEnvelope.self, from: wireBody) else {
      return wireBody
    }
    guard let body = try? PayloadCipher(secret: "secret").open(envelope) else {
      return wireBody
    }
    return body
  }
}

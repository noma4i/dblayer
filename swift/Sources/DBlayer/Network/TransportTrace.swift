import Foundation
import os

/// The development trace of `UrlSessionTransport(trace: true)`: one `os_log` line per HTTP exchange, file PUT and cable event.
/// A line carries metadata only: never a body, a header, a credential or a query string.
enum TransportTrace {
  private static let logger = Logger(subsystem: "com.noma4i.dblayer", category: "dblayer.transport")

  static func emit(_ line: String) { logger.info("\(line, privacy: .public)") }

  static func ms(_ from: Date?, _ to: Date?) -> String {
    guard let from, let to else { return "-" }
    return String(Int((to.timeIntervalSince(from) * 1000).rounded()))
  }

  static func target(_ url: URL?) -> String { (url?.host ?? "-") + (url?.path ?? "") }

  static func outcome(_ error: Error?, status: Int?) -> String {
    if let limit = error as? ResponseLimitError { return "DBlayer.ResponseLimit/1 limit=\(limit.limit)" }
    if let error {
      let native = error as NSError
      return native.domain == NSURLErrorDomain && native.code == NSURLErrorCancelled ? "cancelled" : "\(native.domain)/\(native.code)"
    }
    return status.map { "HTTP \($0)" } ?? "-"
  }
}

/// The labels of one HTTP exchange or file PUT, and the line it writes when the exchange completes.
struct TraceCall: Sendable {
  let author: String
  let requestId: String
  let attempt: String
  let purpose: String
  let operation: String
  let sequence: String
  let method: String
  let target: String

  init(_ request: HttpRequest) {
    author = "\(request.author)"
    requestId = String(request.requestId)
    attempt = String(request.attempt)
    purpose = "\(request.purpose)"
    operation = request.operation
    sequence = request.sequence.map { String($0) } ?? "-"
    method = request.method
    target = TransportTrace.target(URL(string: request.url))
  }

  init(put url: URL?, identity: TransferIdentity) {
    author = "transfer"
    requestId = String(identity.operationId)
    attempt = "-"
    purpose = "put"
    operation = "\(identity.step)"
    sequence = "-"
    method = "PUT"
    target = TransportTrace.target(url)
  }

  func line(started: Date, finished: Date, metrics: URLSessionTaskMetrics?, error: Error?, status: Int?) -> String {
    let last = metrics?.transactionMetrics.last
    let remote = last?.remoteAddress.map { "\($0):\(last?.remotePort.map { String($0) } ?? "-")" } ?? "-"
    let facts = [
      "author=\(author)", "request=\(requestId)", "attempt=\(attempt)", "purpose=\(purpose)", "op=\(operation)", "seq=\(sequence)", "\(method) \(target)",
      "start=\(Int(started.timeIntervalSince1970 * 1000))", "total=\(TransportTrace.ms(started, finished))ms",
      "local=\(last?.localPort.map { String($0) } ?? "-")", "remote=\(remote)", "reused=\(last?.isReusedConnection.description ?? "-")", "proto=\(last?.networkProtocolName ?? "-")",
      "connect=\(TransportTrace.ms(last?.connectStartDate, last?.connectEndDate))ms", "ttfb=\(TransportTrace.ms(last?.requestStartDate, last?.responseStartDate))ms",
      "sent=\(last?.countOfRequestBodyBytesSent ?? 0)", "received=\(last?.countOfResponseBodyBytesReceived ?? 0)",
      "outcome=\(TransportTrace.outcome(error, status: status))"
    ]
    return facts.joined(separator: " ")
  }
}

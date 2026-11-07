import DBlayer
import Foundation

/// A GraphQL call of the library as a test reads it: the operation text, its name and its variables.
public struct TransportRequest: Sendable, Equatable {
  public let document: String
  public let operationName: String
  public let variablesJson: String
  public let purpose: NetworkPurpose

  public init(document: String, operationName: String, variablesJson: String, purpose: NetworkPurpose) {
    self.document = document
    self.operationName = operationName
    self.variablesJson = variablesJson
    self.purpose = purpose
  }

  init(_ request: HttpRequest) {
    let body = (try? JSONSerialization.jsonObject(with: Data(request.body.utf8))) as? [String: Any] ?? [:]
    let variables = body["variables"].flatMap { try? JSONSerialization.data(withJSONObject: $0, options: [.sortedKeys, .fragmentsAllowed]) }
    self.init(document: body["query"] as? String ?? "", operationName: body["operationName"] as? String ?? "", variablesJson: variables.map { String(decoding: $0, as: UTF8.self) } ?? "{}", purpose: request.purpose)
  }
}

/// The answer of an encrypted POST as a test scripts it.
public struct EncryptedReply: Sendable {
  public let status: UInt16
  public let body: String

  public init(status: UInt16, body: String) {
    self.status = status
    self.body = body
  }
}

extension RawOutcome {
  /// A scripted error as the raw outcome the platform executor would report.
  public static func scripted(_ error: Error) -> RawOutcome {
    guard let error = error as? DbError else { return .failed(failure: RawFailure(kind: .io, code: nil, message: "\(error)")) }
    switch error {
    case let .Http(status, detail): return .response(status: status, etag: nil, body: detail)
    case let .Network(detail), let .Offline(detail): return .failed(failure: RawFailure(kind: .unreachable, code: nil, message: detail))
    case let .Interrupted(detail): return .failed(failure: RawFailure(kind: .cancelled, code: nil, message: detail))
    case .Security(.tlsTrust, let detail): return .failed(failure: RawFailure(kind: .tls, code: nil, message: detail))
    case let .Graphql(messages, _):
      let errors = messages.map { ["message": $0, "extensions": ["code": $0]] as [String: Any] }
      let body = (try? JSONSerialization.data(withJSONObject: ["errors": errors])).map { String(decoding: $0, as: UTF8.self) } ?? "{}"
      return .response(status: 200, etag: nil, body: body)
    default: return .failed(failure: RawFailure(kind: .io, code: nil, message: "\(error)"))
    }
  }
}

/// A transport a test scripts with values and errors; the conformance turns them into the raw outcomes of the platform port.
public protocol ScriptedTransport: Transport {
  func query(request: TransportRequest) async throws -> String
  func mutate(request: TransportRequest) async throws -> String
  func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply
  func scriptedCable(url: String, events: CableEvents) async -> CableOutcome
}

extension ScriptedTransport {
  public func query(request: TransportRequest) async throws -> String { throw DbError.Invalid(detail: "unexpected plain query") }
  public func mutate(request: TransportRequest) async throws -> String { throw DbError.Invalid(detail: "unexpected plain mutation") }
  public func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply { throw DbError.Invalid(detail: "unexpected upload") }
  public func scriptedCable(url: String, events: CableEvents) async -> CableOutcome { .failed(failure: RawFailure(kind: .unreachable, code: nil, message: "no cable")) }

  public func endpoint() -> String { "https://graphql.test/graphql" }

  public func send(request: HttpRequest) async -> RawOutcome { await sendGraphql(request: request) }

  public func sendGraphql(request: HttpRequest) async -> RawOutcome {
    let scripted = TransportRequest(request)
    do {
      let text = scripted.document.split(whereSeparator: \.isWhitespace).first == "mutation" ? try await mutate(request: scripted) : try await query(request: scripted)
      return .response(status: 200, etag: nil, body: text)
    } catch {
      return .scripted(error)
    }
  }

  public func openCable(url: String, connectMs: UInt64, events: CableEvents) async -> CableOutcome { await scriptedCable(url: url, events: events) }

  public func put(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async -> RawOutcome {
    do {
      let reply = try await scriptedPut(url: url, headers: headers, file: file, identity: identity, progress: progress)
      return .response(status: reply.status, etag: reply.etag, body: "")
    } catch {
      return .scripted(error)
    }
  }
}

/// An encrypted HTTP exchange a test scripts by path.
public protocol ScriptedHttp: EncryptedHttp {
  func post(path: String, body: String, purpose: NetworkPurpose) async throws -> EncryptedReply
}

extension ScriptedHttp {
  public func send(request: HttpRequest) async -> RawOutcome { await sendEncrypted(request: request) }

  public func sendEncrypted(request: HttpRequest) async -> RawOutcome {
    let path = URL(string: request.url).map { $0.path } ?? ""
    do {
      let reply = try await post(path: path, body: request.body, purpose: request.purpose)
      return .response(status: reply.status, etag: nil, body: reply.body)
    } catch {
      return .scripted(error)
    }
  }
}

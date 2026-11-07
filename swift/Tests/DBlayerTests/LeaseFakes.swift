import Foundation
@testable import DBlayer

final class RecordingEvents: CableEvents, @unchecked Sendable {
  enum Event: Equatable {
    case frame(String)
    case binary(Data)
    case closed(UInt16, String)
    case failed(RawFailureKind)
  }

  private let stream = AsyncStream<Event>.makeStream()
  private var iterator: AsyncStream<Event>.Iterator

  init() { iterator = stream.stream.makeAsyncIterator() }

  func frame(text: String) { stream.continuation.yield(.frame(text)) }
  func binary(bytes: Data) { stream.continuation.yield(.binary(bytes)) }
  func closed(code: UInt16, reason: String) { stream.continuation.yield(.closed(code, reason)) }
  func failed(failure: RawFailure) { stream.continuation.yield(.failed(failure.kind)) }

  func next() async -> Event? { await iterator.next() }
}

func cableURL(_ origin: URL) -> String {
  var components = URLComponents(url: origin, resolvingAgainstBaseURL: false)!
  components.scheme = origin.scheme == "https" ? "wss" : "ws"
  components.percentEncodedPath = "/api/cable"
  return components.string!
}

func probeRequest(_ origin: URL, path: String = "/api/session", body: String = "jwe") -> HttpRequest {
  HttpRequest(url: origin.absoluteString.trimmingCharacters(in: CharacterSet(charactersIn: "/")) + path, method: "POST", headers: ["Content-Type": "application/jose", "Accept": "", "Accept-Language": ""], body: body, connectMs: 10_000, requestMs: 30_000, purpose: .standard)
}

func responseStatus(_ outcome: RawOutcome) -> UInt16? {
  if case let .response(status, _, _) = outcome { return status }
  return nil
}

func failureKind(_ outcome: RawOutcome) -> RawFailureKind? {
  if case let .failed(failure) = outcome { return failure.kind }
  return nil
}

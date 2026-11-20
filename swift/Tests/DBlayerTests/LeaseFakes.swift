import Foundation
import Testing
@testable import DBlayer

final class RecordingEvents: CableEvents, @unchecked Sendable {
  enum Event: Equatable {
    case frame(String)
    case binary(Data)
    case ended(UInt16?, String?, RawFailure?)
  }

  private let stream = AsyncStream<Event>.makeStream()
  private let cursor: Cursor<AsyncStream<Event>>

  init() { cursor = Cursor(stream.stream) }

  func frame(text: String) { stream.continuation.yield(.frame(text)) }
  func binary(bytes: Data) { stream.continuation.yield(.binary(bytes)) }
  func ended(closeCode: UInt16?, reason: String?, failure: RawFailure?) { stream.continuation.yield(.ended(closeCode, reason, failure)) }

  func next() async -> Event? { await cursor.next("a cable event") }
}

func cableURL(_ origin: URL) -> String {
  var components = URLComponents(url: origin, resolvingAgainstBaseURL: false)!
  components.scheme = origin.scheme == "https" ? "wss" : "ws"
  components.percentEncodedPath = "/api/cable"
  return components.string!
}

func probeRequest(_ origin: URL, path: String = "/api/session", body: String = "jwe", headers: [String: String] = [:]) -> HttpRequest {
  HttpRequest(author: .session, requestId: 0, attempt: 1, url: origin.absoluteString.trimmingCharacters(in: CharacterSet(charactersIn: "/")) + path, method: "POST", headers: headers.merging(["Content-Type": "application/jose", "Accept": "", "Accept-Language": ""]) { $1 }, body: body, connectMs: 10_000, requestMs: 30_000, purpose: .standard, responseLimit: 16 * 1024 * 1024, operation: "Probe", sequence: nil)
}

func responseStatus(_ outcome: RawOutcome) -> UInt16? {
  if case let .response(status, _, _) = outcome { return status }
  return nil
}

func rawFailure(_ outcome: RawOutcome) -> RawFailure? {
  if case let .failed(failure) = outcome { return failure }
  return nil
}

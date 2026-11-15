public enum RequestLogItem: Sendable {
  case ready
  case request(operation: String, variablesJson: String, reason: RequestReason)
  case lost(first: UInt64, last: UInt64)
  case ended(EndCause)
}

public struct RequestLogStream: AsyncSequence, Sendable {
  public typealias Element = RequestLogItem
  let stream: ValueStream

  public struct AsyncIterator: AsyncIteratorProtocol {
    var inner: ValueStream.AsyncIterator

    public mutating func next() async -> RequestLogItem? {
      while let value = await inner.next() {
        switch value {
        case .ready: return .ready
        case let .request(operation, variablesJson, reason): return .request(operation: operation, variablesJson: variablesJson, reason: reason)
        case let .lost(first, last): return .lost(first: first, last: last)
        case let .ended(cause): return .ended(cause)
        default: continue
        }
      }
      return nil
    }
  }

  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(inner: stream.makeAsyncIterator()) }
}

extension DB {
  public static func requests() throws -> RequestLogStream {
    let database = try database
    return RequestLogStream(stream: ValueStream(epoch: database.sessionEpoch(), reader: database.requests()))
  }
}

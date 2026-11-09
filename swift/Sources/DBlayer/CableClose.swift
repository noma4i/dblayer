public enum CableCloseItem: Sendable {
  case ready
  case close(position: UInt64, event: CableClose)
  case lost(first: UInt64, last: UInt64)
  case ended(ReadEnd)
}

public struct CableCloseStream: AsyncSequence, Sendable {
  public typealias Element = CableCloseItem
  let stream: ValueStream

  public struct AsyncIterator: AsyncIteratorProtocol {
    var inner: ValueStream.AsyncIterator

    public mutating func next() async -> CableCloseItem? {
      while let value = await inner.next() {
        switch value {
        case .ready: return .ready
        case let .cableClose(position, event): return .close(position: position, event: event)
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
  public static func cableCloses() throws -> CableCloseStream {
    let database = try database
    return CableCloseStream(stream: ValueStream(epoch: database.sessionEpoch(), reader: database.cableCloses()))
  }
}

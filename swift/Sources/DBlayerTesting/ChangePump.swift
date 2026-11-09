@_spi(Testing) import DBlayer
import Foundation

public enum ChangePump {
  public static func start(_ database: Database) async {
    let (opened, announce) = AsyncStream<Void>.makeStream()
    Task.detached { await run(database, announce: announce) }
    for await _ in opened { break }
  }

  private static func run(_ database: Database, announce: AsyncStream<Void>.Continuation) async {
    var ended: UInt64?
    while !Task.isCancelled {
      let epoch = database.sessionEpoch()
      if epoch == ended {
        await TestPulse.shared.next(after: TestPulse.shared.current)
        continue
      }
      let reader = database.subscribeValues(epoch: epoch, request: .changes)
      var cause: EndCause?
      let claim: UInt64
      switch await reader.claimReader() {
      case let .claimed(id): claim = id
      case .ended(cause: .closed):
        announce.finish()
        return
      case let .ended(cause):
        preconditionFailure("the change pump could not claim the reader of its own stream: \(cause)")
      }
      while let value = await reader.pull(reader: claim) {
        if case let .ended(reason) = value { cause = reason }
        announce.finish()
        TestPulse.shared.pulse()
      }
      announce.finish()
      ended = epoch
      if case .closed? = cause { return }
    }
  }
}

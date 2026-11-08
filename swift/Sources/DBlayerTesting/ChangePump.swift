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
      var cause: ReadEnd?
      guard let claim = try? await reader.claimReader() else {
        ended = epoch
        continue
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

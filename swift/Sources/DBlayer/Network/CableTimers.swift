import Foundation
import os

public struct CableRecycle: Sendable, Equatable {
  public let cause: String
  public let uptime: TimeInterval
  public let channels: Int
  public let drops: Int
}

public struct CableTimers: Sendable {
  public var now: @Sendable () -> Date = { Date() }
  public var sleep: @Sendable (TimeInterval) async -> Void = { try? await Task.sleep(for: .seconds($0)) }
  public var jitter: @Sendable (Double) -> Double = { Double.random(in: 0 ... $0) }
  public var recycled: @Sendable (CableRecycle) -> Void = { recycle in
    Logger(subsystem: "com.yupi", category: "cable").info(
      "recycle cause=\(recycle.cause) uptime=\(recycle.uptime) channels=\(recycle.channels) drops=\(recycle.drops)"
    )
  }

  public static let live = CableTimers()
}

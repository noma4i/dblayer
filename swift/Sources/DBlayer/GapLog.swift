import Foundation
import os

// TEMPORARY: Remove after the list return measurement.
final class GapLog: GapLogger, @unchecked Sendable {
  func record(message: String) {
    Self.record("rust", message)
  }

  static func record(_ category: String, _ message: String) {
    Logger(subsystem: "com.yupi.gap", category: category).notice(
      "GAP t=\(ProcessInfo.processInfo.systemUptime, privacy: .public) \(message, privacy: .public)"
    )
  }
}

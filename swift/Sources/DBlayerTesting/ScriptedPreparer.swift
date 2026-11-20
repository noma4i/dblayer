import DBlayer
import Foundation

/// A file producer for tests, without codecs.
public final class ScriptedPreparer: Preparer, @unchecked Sendable {
  private let outcome: PrepareOutcome
  private let facts: ProbeOutcome

  public init(outcome: PrepareOutcome = .done(name: nil, mime: nil), facts: ProbeOutcome = ScriptedPreparer.defaultFacts) {
    self.outcome = outcome
    self.facts = facts
  }

  public static let defaultFacts = ProbeOutcome.file(width: 1280, height: 720, mime: nil)

  public func probe(path: String, descriptor: Data?, mime: String?) async -> ProbeOutcome {
    facts
  }

  public func run(job: PrepareJob, progress: PrepareProgress) async -> PrepareOutcome {
    progress.report(fraction: 0.5)
    guard case .done = outcome else { return outcome }
    do {
      try FileManager.default.copyItem(atPath: job.source, toPath: job.destination)
    } catch {
      return .failed(message: "the stand cannot copy the source: \(error.localizedDescription)")
    }
    progress.report(fraction: 1)
    return outcome
  }

  public func cancel(attemptId: UInt64) {}

  public func importDescriptor(saved: SavedPreparation) -> PrepareImportOutcome {
    .failed(message: "no saved preparation was configured")
  }
}

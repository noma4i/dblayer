import Foundation
#if os(iOS)
import BackgroundTasks
import UIKit
#endif

final class Serial<Value>: @unchecked Sendable {
  private let queue = DispatchQueue(label: "dblayer.prepare.state")
  private var value: Value

  init(_ value: Value) { self.value = value }

  func update<Result>(_ body: (inout Value) -> Result) -> Result { queue.sync { body(&value) } }
}

protocol RoundHandle: AnyObject, Sendable {
  func progress(_ fraction: Double)
  func finish(success: Bool)
}

protocol RoundScheduler: Sendable {
  func submit(_ job: RoundJob, launch: @escaping @Sendable (RoundHandle) -> Void, expire: @escaping @Sendable () -> Void) throws
}

protocol RoundAnswers: AnyObject, Sendable {
  func roundStarted(requestId: UInt64, roundId: UInt32, epoch: UInt64)
  func roundRefused(requestId: UInt64, epoch: UInt64, kind: RoundRefusal, detail: String)
  func roundEnded(roundId: UInt32, epoch: UInt64)
}

extension Database: RoundAnswers {}

final class UploadRounds: RoundJobsSink, @unchecked Sendable {
  private let scheduler: RoundScheduler
  private weak var database: (any RoundAnswers)?
  private let handles = Serial([UInt32: RoundHandle]())

  init(database: any RoundAnswers, scheduler: RoundScheduler) {
    self.database = database
    self.scheduler = scheduler
  }

  func scheduleJob(job: RoundJob) {
    do {
      try scheduler.submit(job, launch: { [weak self] handle in
        guard let self, let database = self.database else { handle.finish(success: false); return }
        self.handles.update { $0[job.roundId] = handle }
        database.roundStarted(requestId: job.requestId, roundId: job.roundId, epoch: job.epoch)
      }, expire: { [weak self] in
        self?.handles.update { $0.removeValue(forKey: job.roundId) }?.finish(success: false)
        self?.database?.roundEnded(roundId: job.roundId, epoch: job.epoch)
      })
    } catch {
      database?.roundRefused(requestId: job.requestId, epoch: job.epoch, kind: .schedulerFailed, detail: error.localizedDescription)
    }
  }

  func endJob(roundId: UInt32) {
    handles.update { $0.removeValue(forKey: roundId) }?.finish(success: true)
  }

  func roundProgress(roundId: UInt32, fraction: Double) {
    handles.update { $0[roundId] }?.progress(fraction)
  }
}

#if os(iOS)
private struct UndeclaredTask: LocalizedError {
  let identifier: String
  var errorDescription: String? { "\(identifier) is missing from BGTaskSchedulerPermittedIdentifiers" }
}

final class SystemRoundScheduler: RoundScheduler, @unchecked Sendable {
  private struct Pending {
    let launch: @Sendable (RoundHandle) -> Void
    let expire: @Sendable () -> Void
  }

  private static let pending = Serial<Pending?>(nil)

  private static let registered: Bool = {
    guard #available(iOS 26.0, *) else { return false }
    return BGTaskScheduler.shared.register(forTaskWithIdentifier: identifier, using: nil) { task in
      guard let task = task as? BGContinuedProcessingTask else { return }
      started(task)
    }
  }()

  private let title: String

  static var identifier: String { (Bundle.main.bundleIdentifier ?? "") + ".dblayer.uploads" }

  static var declared: Bool {
    let permitted = Bundle.main.object(forInfoDictionaryKey: "BGTaskSchedulerPermittedIdentifiers") as? [String] ?? []
    return permitted.contains { $0 == identifier || ($0.hasSuffix("*") && identifier.hasPrefix($0.dropLast())) }
  }

  init(title: String) { self.title = title }

  func submit(_ job: RoundJob, launch: @escaping @Sendable (RoundHandle) -> Void, expire: @escaping @Sendable () -> Void) throws {
    guard Self.declared else { throw UndeclaredTask(identifier: Self.identifier) }
    if #available(iOS 26.0, *) {
      _ = Self.registered
      Self.pending.update { $0 = Pending(launch: launch, expire: expire) }
      let request = BGContinuedProcessingTaskRequest(identifier: Self.identifier, title: title, subtitle: "")
      request.strategy = .queue
      do {
        try BGTaskScheduler.shared.submit(request)
      } catch {
        Self.pending.update { $0 = nil }
        throw error
      }
    } else {
      DispatchQueue.main.async {
        let hold = Hold()
        hold.begin(expire: expire)
        launch(hold)
      }
    }
  }

  @available(iOS 26.0, *)
  private static func started(_ task: BGContinuedProcessingTask) {
    guard let taken = pending.update({ let held = $0; $0 = nil; return held }) else { task.setTaskCompleted(success: false); return }
    task.progress.totalUnitCount = 100
    task.expirationHandler = taken.expire
    taken.launch(ContinuedTask(task))
  }
}

@available(iOS 26.0, *)
private final class ContinuedTask: RoundHandle, @unchecked Sendable {
  private let task: BGContinuedProcessingTask

  init(_ task: BGContinuedProcessingTask) { self.task = task }

  func progress(_ fraction: Double) { task.progress.completedUnitCount = Int64(fraction * 100) }

  func finish(success: Bool) { task.setTaskCompleted(success: success) }
}

private final class Hold: RoundHandle, @unchecked Sendable {
  private let held = Serial(UIBackgroundTaskIdentifier.invalid)

  func begin(expire: @escaping @Sendable () -> Void) {
    held.update { $0 = UIApplication.shared.beginBackgroundTask(withName: "dblayer.uploads", expirationHandler: expire) }
  }

  func progress(_ fraction: Double) {}

  func finish(success: Bool) {
    let ended = held.update { let value = $0; $0 = .invalid; return value }
    if ended != .invalid { DispatchQueue.main.async { UIApplication.shared.endBackgroundTask(ended) } }
  }
}
#endif

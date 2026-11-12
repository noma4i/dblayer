import DBlayer
import Foundation

/// A preparer without codecs for the stand: the probe answers the kind the settings name, `run` reports half and all of the work and copies the source to the result.
public final class ScriptedPreparer: Preparer, @unchecked Sendable {
  private let outcome: PrepareOutcome
  private let video: ProbeOutcome

  public init(outcome: PrepareOutcome = .done(name: nil, mime: nil), video: ProbeOutcome = ScriptedPreparer.defaultVideo) {
    self.outcome = outcome
    self.video = video
  }

  /// A 1280x720 MP4 of H.264 and AAC at 30 frames per second without private metadata keys.
  public static let defaultVideo = ProbeOutcome.video(
    source: VideoSource(width: 1280, height: 720, rotationDegrees: 0, frameRate: 30, bitrate: 1_000_000, mp4H264Aac: true),
    metadataKeys: []
  )

  public func probe(path: String, kind: PrepareKind) async -> ProbeOutcome {
    switch kind {
    case .video: return video
    case .image: return .image(width: 1080, height: 1920, exifOrientation: 1, format: .jpeg)
    }
  }

  public func run(job: PrepareJob, progress: PrepareProgress) async -> PrepareOutcome {
    progress.report(fraction: 0.5)
    guard case .done = outcome else { return outcome }
    do {
      try? FileManager.default.removeItem(atPath: job.destination)
      try FileManager.default.copyItem(atPath: job.source, toPath: job.destination)
    } catch {
      return .failed(message: "the stand cannot copy the source: \(error.localizedDescription)")
    }
    progress.report(fraction: 1)
    return outcome
  }

  public func cancel(attemptId: UInt64) {}
}

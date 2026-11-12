@preconcurrency import AVFoundation
import Foundation
import ImageIO
import UniformTypeIdentifiers

public final class SystemPreparer: Preparer, @unchecked Sendable {
  private let running = Serial([UInt64: Running]())

  public init() {}

  public func probe(path: String, kind: PrepareKind) async -> ProbeOutcome {
    let file = URL(fileURLWithPath: path)
    if kind == .image {
      return Self.probeImage(file) ?? .failed(message: "\(path) is not an image")
    }
    do {
      return try await Self.probeVideo(file)
    } catch {
      return .failed(message: "\(path) is not a video: \(error.localizedDescription)")
    }
  }

  private static func probeImage(_ file: URL) -> ProbeOutcome? {
    if let source = CGImageSourceCreateWithURL(file as CFURL, nil),
       let properties = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any],
       let width = properties[kCGImagePropertyPixelWidth] as? Int,
       let height = properties[kCGImagePropertyPixelHeight] as? Int {
      return .image(
        width: UInt32(clamping: width), height: UInt32(clamping: height),
        exifOrientation: (properties[kCGImagePropertyOrientation] as? UInt32) ?? 1,
        format: (CGImageSourceGetType(source) as String?) == UTType.png.identifier ? .png : .jpeg
      )
    }
    return nil
  }

  public func run(job: PrepareJob, progress: PrepareProgress) async -> PrepareOutcome {
    await write(job) { progress.report(fraction: $0) }
  }

  func write(_ job: PrepareJob, report: @escaping @Sendable (Double) -> Void) async -> PrepareOutcome {
    let entry = Running()
    running.update { $0[job.attemptId] = entry }
    defer { running.update { $0[job.attemptId] = nil } }
    let work = Task.detached { () -> PrepareOutcome in
      do {
        switch job.plan {
        case let .video(plan, _, audioBitrate, audioChannels, audioSampleRate, mirror):
          let audio = AudioTarget(bitrate: audioBitrate, channels: audioChannels, sampleRate: audioSampleRate)
          return try await VideoWrite.run(job, plan: plan, audio: audio, mirror: mirror, entry: entry, report: report)
        case let .image(plan, quality, format):
          try ImageWrite.run(job, plan: plan, quality: quality, format: format)
          report(1)
          return .done(name: nil, mime: format == .png ? "image/png" : "image/jpeg")
        }
      } catch {
        try? FileManager.default.removeItem(atPath: job.destination)
        return .failed(message: error is CancellationError ? "the preparation is cancelled" : error.localizedDescription)
      }
    }
    entry.hold(work)
    return await work.value
  }

  public func cancel(attemptId: UInt64) {
    running.update { $0[attemptId] }?.cancel()
  }

  private static func probeVideo(_ file: URL) async throws -> ProbeOutcome {
    let asset = MediaContainer.asset(file)
    guard let track = try await asset.loadTracks(withMediaType: .video).first else {
      return .failed(message: "\(file.path) has no video track")
    }
    let (size, transform, rate, dataRate, formats) = try await track.load(
      .naturalSize, .preferredTransform, .nominalFrameRate, .estimatedDataRate, .formatDescriptions
    )
    let audioTrack = try await asset.loadTracks(withMediaType: .audio).first
    let audioFormats = try await audioTrack?.load(.formatDescriptions) ?? []
    let fits = MediaContainer.isMp4(file)
      && formats.allSatisfy { CMFormatDescriptionGetMediaSubType($0) == kCMVideoCodecType_H264 }
      && audioFormats.allSatisfy { CMFormatDescriptionGetMediaSubType($0) == kAudioFormatMPEG4AAC }
    let source = VideoSource(
      width: UInt32(abs(size.width)), height: UInt32(abs(size.height)),
      rotationDegrees: Int32((atan2(transform.b, transform.a) * 180 / .pi).rounded()),
      frameRate: rate, bitrate: dataRate > 0 ? UInt32(dataRate) : nil, mp4H264Aac: fits
    )
    return .video(source: source, metadataKeys: try await metadataKeys(asset, tracks: [track] + (audioTrack.map { [$0] } ?? [])))
  }

  private static func metadataKeys(_ asset: AVURLAsset, tracks: [AVAssetTrack]) async throws -> [String] {
    var items: [AVMetadataItem] = []
    for format in try await asset.load(.availableMetadataFormats) { items += try await asset.loadMetadata(for: format) }
    for track in tracks { items += try await track.load(.metadata) }
    return items.compactMap { ($0.identifier?.rawValue ?? ($0.key as? String)).map(decodedMetadataKey) }
  }
}

private func decodedMetadataKey(_ identifier: String) -> String {
  var bytes: [UInt8] = []
  var rest = Array(identifier.utf8)[...]
  while let byte = rest.popFirst() {
    let escaped = byte == UInt8(ascii: "%") ? rest.prefix(2).compactMap { Character(UnicodeScalar($0)).hexDigitValue } : []
    if escaped.count == 2 {
      bytes.append(UInt8(escaped[0] * 16 + escaped[1]))
      rest = rest.dropFirst(2)
    } else {
      bytes.append(byte)
    }
  }
  return String(bytes: bytes, encoding: .utf8) ?? String(String.UnicodeScalarView(bytes.map(UnicodeScalar.init)))
}

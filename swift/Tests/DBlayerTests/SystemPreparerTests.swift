@preconcurrency import AVFoundation
import CoreGraphics
import Foundation
import ImageIO
import Testing
import UniformTypeIdentifiers
@testable import DBlayer

private struct FixtureClip {
  var width: Int
  var height: Int
  var codec = AVVideoCodecType.h264
  var audio = false
  var turned = false
  var metadata = false
  var hdr = false
  var frames = 30
}

private func writeClip(_ clip: FixtureClip, to url: URL) async throws {
  let writer = try AVAssetWriter(outputURL: url, fileType: .mp4)
  if clip.metadata {
    writer.metadata = [(AVMetadataIdentifier.quickTimeMetadataLocationISO6709, "+37.3300-122.0300/"), (.iTunesMetadataSongName, "Private title")].map {
      let item = AVMutableMetadataItem()
      item.identifier = $0.0
      item.value = $0.1 as NSString
      item.dataType = "com.apple.metadata.datatype.UTF-8"
      return item
    }
  }
  var settings: [String: Any] = [AVVideoCodecKey: clip.codec, AVVideoWidthKey: clip.width, AVVideoHeightKey: clip.height]
  if clip.hdr {
    settings[AVVideoColorPropertiesKey] = [
      AVVideoColorPrimariesKey: AVVideoColorPrimaries_ITU_R_2020,
      AVVideoTransferFunctionKey: AVVideoTransferFunction_ITU_R_2100_HLG,
      AVVideoYCbCrMatrixKey: AVVideoYCbCrMatrix_ITU_R_2020
    ]
    settings[AVVideoCompressionPropertiesKey] = [AVVideoProfileLevelKey: "HEVC_Main10_AutoLevel"]
  }
  let video = AVAssetWriterInput(mediaType: .video, outputSettings: settings)
  if clip.turned { video.transform = CGAffineTransform(rotationAngle: .pi / 2) }
  writer.add(video)
  let format = clip.hdr ? kCVPixelFormatType_420YpCbCr10BiPlanarVideoRange : kCVPixelFormatType_32BGRA
  let adaptor = AVAssetWriterInputPixelBufferAdaptor(assetWriterInput: video, sourcePixelBufferAttributes: nil)
  var audio: AVAssetWriterInput?
  var audioFormat: CMAudioFormatDescription?
  if clip.audio {
    var description = AudioStreamBasicDescription(
      mSampleRate: 44100, mFormatID: kAudioFormatLinearPCM, mFormatFlags: kAudioFormatFlagIsSignedInteger | kAudioFormatFlagIsPacked,
      mBytesPerPacket: 2, mFramesPerPacket: 1, mBytesPerFrame: 2, mChannelsPerFrame: 1, mBitsPerChannel: 16, mReserved: 0
    )
    CMAudioFormatDescriptionCreate(allocator: nil, asbd: &description, layoutSize: 0, layout: nil, magicCookieSize: 0, magicCookie: nil, extensions: nil, formatDescriptionOut: &audioFormat)
    let input = AVAssetWriterInput(
      mediaType: .audio, outputSettings: [AVFormatIDKey: kAudioFormatMPEG4AAC, AVNumberOfChannelsKey: 1, AVSampleRateKey: 44100, AVEncoderBitRateKey: 64000]
    )
    writer.add(input)
    audio = input
  }
  try #require(writer.startWriting())
  writer.startSession(atSourceTime: .zero)
  nonisolated(unsafe) let adaptorInFeed = adaptor
  await feed(video, count: clip.frames) { index in
    var buffer: CVPixelBuffer?
    CVPixelBufferCreate(nil, clip.width, clip.height, format, nil, &buffer)
    guard let buffer else { return }
    CVPixelBufferLockBaseAddress(buffer, [])
    if !clip.hdr, let base = CVPixelBufferGetBaseAddress(buffer) {
      let stride = CVPixelBufferGetBytesPerRow(buffer)
      for row in 0..<clip.height {
        let line = base.advanced(by: row * stride).assumingMemoryBound(to: UInt8.self)
        for column in 0..<clip.width {
          let level: UInt8 = column < clip.width / 2 ? 255 : 0
          line[column * 4] = level
          line[column * 4 + 1] = level
          line[column * 4 + 2] = level
          line[column * 4 + 3] = 255
        }
      }
    }
    CVPixelBufferUnlockBaseAddress(buffer, [])
    adaptorInFeed.append(buffer, withPresentationTime: CMTime(value: CMTimeValue(index), timescale: 30))
  }
  if let audio, let audioFormat {
    let samples = 44100
    var block: CMBlockBuffer?
    CMBlockBufferCreateWithMemoryBlock(
      allocator: nil, memoryBlock: nil, blockLength: samples * 2, blockAllocator: nil, customBlockSource: nil,
      offsetToData: 0, dataLength: samples * 2, flags: kCMBlockBufferAssureMemoryNowFlag, blockBufferOut: &block
    )
    if let block {
      CMBlockBufferFillDataBytes(with: 0, blockBuffer: block, offsetIntoDestination: 0, dataLength: samples * 2)
      var sample: CMSampleBuffer?
      CMAudioSampleBufferCreateReadyWithPacketDescriptions(
        allocator: nil, dataBuffer: block, formatDescription: audioFormat, sampleCount: samples,
        presentationTimeStamp: .zero, packetDescriptions: nil, sampleBufferOut: &sample
      )
      nonisolated(unsafe) let held = sample
      await feed(audio, count: 1) { _ in if let held { audio.append(held) } }
    }
  }
  await writer.finishWriting()
  try #require(writer.status == .completed, "the fixture clip cannot be written: \(String(describing: writer.error))")
}

private func feed(_ input: AVAssetWriterInput, count: Int, append: @escaping (Int) -> Void) async {
  nonisolated(unsafe) let append = append
  nonisolated(unsafe) let input = input
  await withCheckedContinuation { (done: CheckedContinuation<Void, Never>) in
    nonisolated(unsafe) var next = 0
    nonisolated(unsafe) var finished = false
    input.requestMediaDataWhenReady(on: DispatchQueue(label: "fixture.feed")) {
      guard !finished else { return }
      for index in next..<count {
        guard input.isReadyForMoreMediaData else {
          next = index
          return
        }
        append(index)
      }
      next = count
      input.markAsFinished()
      finished = true
      done.resume()
    }
  }
}

private func writePicture(_ url: URL, type: UTType, width: Int, height: Int, orientation: UInt32, alpha: Bool, tagged: Bool) throws {
  let context = try #require(CGContext(
    data: nil, width: width, height: height, bitsPerComponent: 8, bytesPerRow: 0, space: CGColorSpaceCreateDeviceRGB(),
    bitmapInfo: (alpha ? CGImageAlphaInfo.premultipliedLast : CGImageAlphaInfo.noneSkipLast).rawValue
  ))
  context.setFillColor(CGColor(red: 1, green: 0, blue: 0, alpha: 1))
  context.fill(CGRect(x: 0, y: 0, width: width / 2, height: height))
  context.setFillColor(CGColor(red: 0, green: 0, blue: 1, alpha: alpha ? 0.5 : 1))
  context.fill(CGRect(x: width / 2, y: 0, width: width - width / 2, height: height))
  let image = try #require(context.makeImage())
  var properties: [CFString: Any] = [kCGImagePropertyOrientation: orientation]
  if tagged {
    properties[kCGImagePropertyGPSDictionary] = [
      kCGImagePropertyGPSLatitude: 37.33, kCGImagePropertyGPSLatitudeRef: "N", kCGImagePropertyGPSLongitude: 122.03, kCGImagePropertyGPSLongitudeRef: "W"
    ]
    properties[kCGImagePropertyTIFFDictionary] = [kCGImagePropertyTIFFMake: "Acme", kCGImagePropertyTIFFModel: "Cam 1"]
  }
  let destination = try #require(CGImageDestinationCreateWithURL(url as CFURL, type.identifier as CFString, 1, nil), "\(type.identifier) cannot be written on this runtime")
  CGImageDestinationAddImage(destination, image, properties as CFDictionary)
  try #require(CGImageDestinationFinalize(destination), "\(type.identifier) cannot be encoded on this runtime")
}

private func reencode(_ width: UInt32, _ height: UInt32, _ mirror: Bool, _ audio: Bool) -> PreparePlan {
  .video(
    plan: VideoPlan(reencode: true, width: width, height: height, bitrate: 400_000, frameRate: 30, rotationDegrees: 0),
    stripMetadata: [], audioBitrate: 64_000, audioChannels: 1, audioSampleRate: 44100, mirror: mirror
  )
}

private func photo(_ width: UInt32, _ height: UInt32, turn: PhotoTurn, format: ImageFormat) -> PreparePlan {
  let sideways = turn.rotationDegrees % 180 == 90
  let upright = PixelSize(width: sideways ? height : width, height: sideways ? width : height)
  return .image(
    plan: PhotoPlan(
      turn: turn, upright: upright, crop: PixelRect(x: 0, y: 0, width: upright.width, height: upright.height), mirror: false, output: upright,
      needsDraw: false, sample: 1, decode: PixelSize(width: width, height: height), thumbnailSide: max(width, height), cost: 0
    ),
    quality: 0.8, format: format
  )
}

private struct VideoFacts {
  var width: UInt32
  var height: UInt32
  var codec: FourCharCode
  var frames: Int
  var audio: Bool
  var primaries: String?
  var keys: [String]
}

private func videoFacts(_ url: URL) async throws -> VideoFacts {
  guard case let .video(source, keys) = await SystemPreparer().probe(path: url.path, kind: .video) else { throw PrepareFailure(message: "the result is no video") }
  let asset = MediaContainer.asset(url)
  let track = try #require(try await asset.loadTracks(withMediaType: .video).first)
  let formats = try await track.load(.formatDescriptions)
  let reader = try AVAssetReader(asset: asset)
  let output = AVAssetReaderTrackOutput(track: track, outputSettings: nil)
  reader.add(output)
  reader.startReading()
  let frames = sequence(state: output) { $0.copyNextSampleBuffer() }.reduce(0) { $0 + CMSampleBufferGetNumSamples($1) }
  let extensions = formats.first.flatMap { CMFormatDescriptionGetExtensions($0) as? [String: Any] }
  return VideoFacts(
    width: source.width, height: source.height, codec: CMFormatDescriptionGetMediaSubType(try #require(formats.first)), frames: frames,
    audio: !(try await asset.loadTracks(withMediaType: .audio)).isEmpty,
    primaries: extensions?[kCMFormatDescriptionExtension_ColorPrimaries as String] as? String, keys: keys
  )
}

private func luminance(_ url: URL, left: Bool) async throws -> Double {
  let generator = AVAssetImageGenerator(asset: MediaContainer.asset(url))
  let image = try generator.copyCGImage(at: .zero, actualTime: nil)
  return try level(image, x: left ? 2 : image.width - 3, y: image.height / 2)
}

private func level(_ image: CGImage, x: Int, y: Int) throws -> Double {
  var data = [UInt8](repeating: 0, count: image.width * image.height * 4)
  let context = try #require(CGContext(
    data: &data, width: image.width, height: image.height, bitsPerComponent: 8, bytesPerRow: image.width * 4,
    space: CGColorSpaceCreateDeviceRGB(), bitmapInfo: CGImageAlphaInfo.noneSkipLast.rawValue
  ))
  context.draw(image, in: CGRect(x: 0, y: 0, width: image.width, height: image.height))
  let at = (y * image.width + x) * 4
  return (Double(data[at]) + Double(data[at + 1]) + Double(data[at + 2])) / 765
}

private struct Row {
  let name: String
  var mayFail = false
  let make: (URL) async throws -> Void
  let plan: ([String]) -> PreparePlan
  let check: (URL) async throws -> String
}

private func videoRow(
  _ name: String, _ clip: FixtureClip, mayFail: Bool = false, plan: @escaping ([String]) -> PreparePlan,
  check: @escaping (URL, VideoFacts) async throws -> String
) -> Row {
  Row(name: name, mayFail: mayFail, make: { try await writeClip(clip, to: $0) }, plan: plan, check: { try await check($0, videoFacts($0)) })
}

private struct PictureFacts {
  var width: Int
  var height: Int
  var type: String?
  var tagged: Bool
}

private func pictureFacts(_ url: URL) throws -> PictureFacts {
  let source = try #require(CGImageSourceCreateWithURL(url as CFURL, nil))
  let properties = try #require(CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any])
  let tiff = properties[kCGImagePropertyTIFFDictionary] as? [CFString: Any]
  let exif = properties[kCGImagePropertyExifDictionary] as? [CFString: Any]
  let tagged = properties[kCGImagePropertyGPSDictionary] != nil || tiff?[kCGImagePropertyTIFFMake] != nil || tiff?[kCGImagePropertyTIFFModel] != nil
    || exif?[kCGImagePropertyExifLensModel] != nil || exif?[kCGImagePropertyExifBodySerialNumber] != nil
  return PictureFacts(
    width: properties[kCGImagePropertyPixelWidth] as? Int ?? 0, height: properties[kCGImagePropertyPixelHeight] as? Int ?? 0,
    type: CGImageSourceGetType(source) as String?, tagged: tagged
  )
}

private func pictureRow(
  _ name: String, type: UTType, width: Int, height: Int, orientation: UInt32, alpha: Bool = false, turn: PhotoTurn, format: ImageFormat, mayFail: Bool = false
) -> Row {
  let sideways = turn.rotationDegrees % 180 == 90
  return Row(
    name: name, mayFail: mayFail,
    make: { try writePicture($0, type: type, width: width, height: height, orientation: orientation, alpha: alpha, tagged: true) },
    plan: { _ in photo(UInt32(width), UInt32(height), turn: turn, format: format) },
    check: {
      let facts = try pictureFacts($0)
      #expect(facts.width == (sideways ? height : width) && facts.height == (sideways ? width : height), "\(name) size")
      #expect(facts.type == (format == .png ? UTType.png : UTType.jpeg).identifier, "\(name) format")
      #expect(!facts.tagged, "\(name) keeps no GPS or device data")
      return "\(facts.width)x\(facts.height) \(facts.type ?? "?") no GPS no device data"
    }
  )
}

private func matrixRows() -> [Row] {
  var rows: [Row] = []
  rows.append(videoRow(
    "h264+aac within limits", FixtureClip(width: 64, height: 32, audio: true, metadata: true),
    plan: { keys in
      #expect(!keys.isEmpty, "the source carries private metadata")
      return .video(plan: VideoPlan(reencode: false, width: 64, height: 32, bitrate: 0, frameRate: 30, rotationDegrees: 0), stripMetadata: keys, audioBitrate: 64_000, audioChannels: 1, audioSampleRate: 44100, mirror: false)
    },
    check: { _, facts in
      #expect(facts.width == 64 && facts.height == 32 && facts.codec == kCMVideoCodecType_H264 && facts.frames == 30 && facts.audio)
      #expect(facts.keys.isEmpty, "private metadata is gone: \(facts.keys)")
      return "remux \(facts.width)x\(facts.height) h264 frames=\(facts.frames) audio=\(facts.audio) keys=\(facts.keys)"
    }
  ))
  rows.append(videoRow("h264 above limits", FixtureClip(width: 640, height: 360, audio: true), plan: { _ in reencode(320, 180, false, true) }) { _, facts in
    #expect(facts.width == 320 && facts.height == 180 && facts.codec == kCMVideoCodecType_H264 && facts.frames == 30 && facts.audio)
    return "\(facts.width)x\(facts.height) h264 frames=\(facts.frames) audio=\(facts.audio)"
  })
  rows.append(videoRow("hevc", FixtureClip(width: 128, height: 64, codec: .hevc, audio: true), plan: { _ in reencode(64, 32, false, true) }) { _, facts in
    #expect(facts.width == 64 && facts.height == 32 && facts.codec == kCMVideoCodecType_H264 && facts.frames == 30 && facts.audio)
    return "\(facts.width)x\(facts.height) h264 frames=\(facts.frames) audio=\(facts.audio)"
  })
  rows.append(videoRow("vertical turned 90", FixtureClip(width: 64, height: 32, audio: true, turned: true), plan: { _ in reencode(32, 64, false, true) }) { _, facts in
    #expect(facts.width == 32 && facts.height == 64 && facts.codec == kCMVideoCodecType_H264 && facts.frames == 30)
    return "\(facts.width)x\(facts.height) h264 frames=\(facts.frames)"
  })
  rows.append(videoRow("no audio track", FixtureClip(width: 64, height: 32), plan: { _ in reencode(64, 32, false, false) }) { _, facts in
    #expect(facts.width == 64 && facts.height == 32 && facts.frames == 30 && !facts.audio)
    return "\(facts.width)x\(facts.height) frames=\(facts.frames) audio=\(facts.audio)"
  })
  rows.append(videoRow("mirror", FixtureClip(width: 64, height: 32), plan: { _ in reencode(64, 32, true, false) }) { url, facts in
    let left = try await luminance(url, left: true)
    let right = try await luminance(url, left: false)
    #expect(left < 0.3 && right > 0.7, "the left side is dark and the right side is bright after the mirror")
    return "\(facts.width)x\(facts.height) left=\(left) right=\(right)"
  })
  rows.append(videoRow("hdr hlg hevc", FixtureClip(width: 64, height: 32, codec: .hevc, hdr: true), mayFail: true, plan: { _ in reencode(64, 32, false, false) }) { _, facts in
    #expect(facts.primaries == "ITU_R_709_2", "the result is SDR BT.709, got \(facts.primaries ?? "nil")")
    return "\(facts.width)x\(facts.height) primaries=\(facts.primaries ?? "nil")"
  })
  rows.append(pictureRow("jpeg", type: .jpeg, width: 40, height: 20, orientation: 1, turn: PhotoTurn(rotationDegrees: 0, flipHorizontal: false), format: .jpeg))
  rows.append(pictureRow("heic", type: UTType("public.heic")!, width: 40, height: 20, orientation: 1, turn: PhotoTurn(rotationDegrees: 0, flipHorizontal: false), format: .jpeg, mayFail: true))
  rows.append(pictureRow("png with alpha", type: .png, width: 40, height: 20, orientation: 1, alpha: true, turn: PhotoTurn(rotationDegrees: 0, flipHorizontal: false), format: .png))
  rows.append(pictureRow("jpeg exif orientation 6", type: .jpeg, width: 40, height: 20, orientation: 6, turn: PhotoTurn(rotationDegrees: 90, flipHorizontal: false), format: .jpeg))
  return rows
}

@Suite(.serialized)
struct SystemPreparerTests {
  @Test(.timeLimit(.minutes(5)))
  func theSourceMatrixRunsOnTheRuntimeOfTheDevice() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let preparer = SystemPreparer()
    for (index, row) in matrixRows().enumerated() {
      let source = directory.appending(path: "source-\(index)")
      let destination = directory.appending(path: "result-\(index)")
      do {
        try await row.make(source)
      } catch {
        #expect(row.mayFail, "\(row.name): the fixture cannot be written: \(error)")
        print("MATRIX | \(row.name) | Failed(fixture) | \(error)")
        continue
      }
      var keys: [String] = []
      let kind: PrepareKind = if case .image = row.plan([]) { .image } else { .video }
      switch await preparer.probe(path: source.path, kind: kind) {
      case let .video(_, metadataKeys): keys = metadataKeys
      case .image: break
      case let .failed(message):
        #expect(Bool(false), "\(row.name): probe failed: \(message)")
        continue
      }
      let outcome = await preparer.write(PrepareJob(attemptId: UInt64(index), source: source.path, destination: destination.path, plan: row.plan(keys))) { _ in }
      switch outcome {
      case .done:
        print("MATRIX | \(row.name) | Done | \(try await row.check(destination))")
      case let .failed(message), let .cannotDecode(message):
        #expect(row.mayFail, "\(row.name): \(message)")
        #expect(!FileManager.default.fileExists(atPath: destination.path), "\(row.name) leaves no partial file")
        print("MATRIX | \(row.name) | Failed | \(message)")
      }
    }
  }

  @Test(.timeLimit(.minutes(5)))
  func cancelStopsTheRunAndRemovesThePartialFile() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let source = directory.appending(path: "long.mp4")
    try await writeClip(FixtureClip(width: 640, height: 360, frames: 240), to: source)
    let destination = directory.appending(path: "result.mp4")
    let preparer = SystemPreparer()
    let first = AsyncStream<Double>.makeStream()
    let job = PrepareJob(attemptId: 7, source: source.path, destination: destination.path, plan: reencode(640, 360, false, false))
    let work = Task { await preparer.write(job) { first.continuation.yield($0) } }
    _ = await Cursor(first.stream).next("the first progress value of the run")
    preparer.cancel(attemptId: 7)
    let outcome = await work.value
    guard case .failed = outcome else {
      #expect(Bool(false), "the cancelled run reports a failure, got \(outcome)")
      return
    }
    #expect(!FileManager.default.fileExists(atPath: destination.path))
  }
}

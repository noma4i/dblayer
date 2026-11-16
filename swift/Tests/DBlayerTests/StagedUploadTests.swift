import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let stagedOperations = #"[{"name":"CreateBlob","document":"mutation CreateBlob($filename: String!, $contentType: String!, $byteSize: Int!, $checksum: String!) { createBlob(filename: $filename, contentType: $contentType, byteSize: $byteSize, checksum: $checksum) { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#

private func enqueueStagedSteps(_ transport: MockTransport) {
  transport.enqueue("InitMultipart", reply: #"{"data":{"initMultipart":{"multipartUpload":{"key":"k","uploadId":"u"}}}}"#)
  transport.enqueue("PresignParts", reply: #"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://s3/p1"}]}}}"#)
  transport.enqueuePut("https://s3/p1", etag: "e1")
}

private func stagedFile() throws -> URL {
  let file = FileManager.default.temporaryDirectory.appending(path: "staged-\(UUID().uuidString)")
  try Data("abcdef".utf8).write(to: file)
  return file
}

private func untilHeld(_ status: Cursor<UploadStatusStream>) async -> Bool {
  await status.first("the held status") { item in
    if case let .progress(_, info) = item { return info.progress >= 0.99 }
    return false
  } != nil
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aStagedUploadShowsItsStatusBeforeTheCommitAndTheCommitCreatesTheRow() async throws {
    let transport = MockTransport()
    enqueueStagedSteps(transport)
    transport.enqueue("CompleteMultipart", reply: #"{"data":{"completeMultipart":{"signedId":"sid","blobId":"b1"}}}"#)
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    _ = try await openSigned(config: Config(path: nil), modelsJson: modelsJSON, transport: transport, operations: stagedOperations)
    let file = try stagedFile()
    defer { try? FileManager.default.removeItem(at: file) }
    let scope = try DB.account()
    let staged = try await scope.stage(Moment.self, action: "upload_clip", UploadRequest(path: file.path))
    #expect(staged.action == "upload_clip")
    #expect(await untilHeld(Cursor(scope.uploadStatus(staged))))
    #expect(try await Moment.find(staged.id).current() == nil)
    let receipt = try await staged.commit(values: ["text": .string("hello")])
    #expect(receipt.optimisticValue?.id == staged.id)
    #expect(receipt.optimisticValue?.text == "hello")
    #expect(try await receipt.confirmed().value?.id == "m77")
    await DB.closeForCleanup()
  }
}

private final class SilentJobs: RoundJobsSink, @unchecked Sendable {
  func scheduleJob(job: RoundJob) {}
  func endJob(roundId: UInt32) {}
  func roundProgress(roundId: UInt32, fraction: Double) {}
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aSecondRoundJobsSinkIsRefused() async throws {
    _ = try await openSigned(config: Config(path: nil), modelsJson: modelsJSON, transport: MockTransport())
    let sink = SilentJobs()
    _ = try? DB.database.setRoundJobs(sink: sink)
    expectDbErrorNow("Invalid") { try DB.database.setRoundJobs(sink: sink) }
    await DB.closeForCleanup()
  }
}

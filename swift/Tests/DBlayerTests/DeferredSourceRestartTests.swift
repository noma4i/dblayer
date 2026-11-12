import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(2)))
  func aDeferredSourceReadsMissingFileAfterARestart() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    let path = directory.appending(path: "restart.sqlite").path
    defer { Task { @MainActor in await DB.closeForCleanup(); try? FileManager.default.removeItem(at: directory) } }
    let transport = MockTransport()
    let operations = #"[{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt } }"}]"#
    try await openSignedClocked(modelsJson: modelsJSON, transport: transport, operations: operations, path: path)
    _ = try await DB.account().upload(Moment.self, id: "absent", action: "upload_clip", .pending())
    try await openSignedClocked(modelsJson: modelsJSON, transport: transport, operations: operations, path: path)
    let scope = try DB.account()
    let stream = scope.uploadStatus(StagedUpload<Moment>(id: "absent", action: "upload_clip", database: scope.database, epoch: scope.epoch))
    let failed = await Cursor(stream).first("the deferred source reads failed") { item in
      if case let .progress(progress) = item { return progress.state == "failed" && progress.error?.contains("missing_file") == true }
      return false
    }
    #expect(failed != nil)
  }
}
